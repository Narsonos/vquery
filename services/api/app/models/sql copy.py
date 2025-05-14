from app.models.common import Alias

from typing import List, Literal, Tuple
from sqlmodel import SQLModel, Field, select
from pydantic import field_validator, PrivateAttr
from enum import Enum

import app.utils.context as ctx
import logging
logger = logging.getLogger('applogger')



###############
# TO DO BOARD #
###############
# 1. Add validity checks for str so no arbitrarity of strings is limited




#ALIAS CACHE
async def get_or_fetch_alias(alias_id: int) -> Alias:
    user = ctx.current_user.get()
    dbsession = ctx.current_dbsession.get()
    redis = ctx.current_redis.get()
    
    key = f"alias:{alias_id}"
    alias_json = await redis.get(key)
    if alias_json:
        alias = Alias.model_validate_json(alias_json)
        logger.info(f'Found alias in cache: {alias}')
        return alias
    alias = await Alias.by_id(alias_id=alias_id, owner_id=user.id, dbsession=dbsession)
    await redis.set(key, alias.model_dump_json(), ex=3600)  

    logger.info(f'Pulled alias from db: {alias}')
    return alias

class AggregatesEnum(str, Enum):
    AVG = "AVG"
    SUM = "SUM"
    MAX = "MAX"
    MIN = "MIN"
    COUNT = "COUNT"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper = value.upper()
            for member in cls:
                if member.value == upper:
                    return member
        return None


class OperandFuncEnum(str, Enum):
    YEAR = "AVG"
    MONTH = "SUM"
    DAY = "MAX"
    DATE = "MIN"
    FLOOR = "COUNT"
    ROUND =  "ROUND"
    LOWER = "LOWER"
    UPPER = "UPPER"
    LENGTH= "LENGTH"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper = value.upper()
            for member in cls:
                if member.value == upper:
                    return member
        return None

class DirectionEnum(str, Enum):
    ASC = "ASC"
    DESC = "DESC"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper = value.upper()
            for member in cls:
                if member.value == upper:
                    return member
        return None

class JoinEnum(str, Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper = value.upper()
            for member in cls:
                if member.value == upper:
                    return member
        return None
    
class LogicalEnum(str, Enum):
    AND = "AND"
    OR = "OR"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper = value.upper()
            for member in cls:
                if member.value == upper:
                    return member
        return None




Operators = Literal['=', '>', '<', '>=', '<=', '<>']


class ColumnRepresentation(SQLModel):
    name: str
    dtype: str

class SimpleSchema(SQLModel):
    name: str
    columns: List[ColumnRepresentation]


class AllCols(str, Enum):
    AllCols = '*'
    
    def sql():
        return '*'
    




class EmptyClause(SQLModel):

    def sql(self):
        return ""

class LimitClause(SQLModel):
    limit: int
    offset: int = 0

    def sql(self):
        return f"LIMIT {self.limit} OFFSET {self.offset}" 


#Further we split modelling of sql query onto 2 parts - client/server. That is to allow send only alias_ids instead of alias => validate them easily on the server side.
#With this approach sent JSONs are simpler and make more sense
###########################
#       CLIENT SIDE       #
###########################
class Expr(SQLModel):
    pass

class UnaryOperation(Expr):
    operand: Expr
    operation: Operators

class BinaryOperation(Expr):
    left: Expr
    operation: Operators
    right: Expr

class FunctionCall(Expr):
    func: OperandFuncEnum | AggregatesEnum
    args: List[Expr]

class AliasedExpr(Expr):
    expr: Expr
    alias: str


class ColumnOperandInput(SQLModel):
    col_alias_id: int
    function: OperandFuncEnum | None = Field(default=None)

    _counterpart = PrivateAttr(default_factory= lambda: ColumnOperand) 

class AggregateInput(SQLModel):
    function: AggregatesEnum
    arg: ColumnOperandInput

    _counterpart = PrivateAttr(default_factory= lambda: Aggregate) 


class HavingConditionInput(SQLModel):
    left: AggregateInput | ColumnOperandInput
    operator: Operators 
    right: str | int | float

    _counterpart = PrivateAttr(default_factory= lambda: HavingCondition) 


class WhereConditionInput(SQLModel):
    left: ColumnOperandInput
    operator: Operators 
    right: ColumnOperandInput | str | int | float

    _counterpart = PrivateAttr(default_factory= lambda: WhereCondition) 
    
class JoinConditionInput(SQLModel):
    left: ColumnOperandInput
    operator: Operators
    right: ColumnOperandInput  # Только столбец

    _counterpart = PrivateAttr(default_factory= lambda: JoinCondition) 


class JoinLogicalInput(SQLModel):
    operator: LogicalEnum
    operands: List['JoinExpressionInput']

    _counterpart = PrivateAttr(default_factory= lambda: JoinLogical) 

JoinExpressionInput = JoinConditionInput | JoinLogicalInput 
JoinLogicalInput.model_rebuild()

class WhereLogicalInput(SQLModel):
    operator: LogicalEnum
    operands: List['WhereExpressionInput']
    
    _counterpart = PrivateAttr(default_factory= lambda: WhereLogical) 

class WhereNotInput(SQLModel):
    expression: 'WhereExpressionInput'
    _counterpart = PrivateAttr(default_factory= lambda: WhereNot) 

WhereExpressionInput = WhereConditionInput | WhereLogicalInput | WhereNotInput
WhereLogicalInput.model_rebuild()
WhereNotInput.model_rebuild()

class HavingLogicalInput(SQLModel):
    operator: LogicalEnum
    operands: List['HavingExpressionInput']
    _counterpart = PrivateAttr(default_factory= lambda: HavingLogical) 


class HavingNotInput(SQLModel):
    expression: 'HavingExpressionInput'
    _counterpart = PrivateAttr(default_factory= lambda: HavingNot) 


HavingExpressionInput = HavingConditionInput | HavingLogicalInput | HavingNotInput
HavingLogicalInput.model_rebuild()
HavingNotInput.model_rebuild()


class JoinClauseInput(SQLModel):
    table_alias_id: int = Field(gt=0)
    on_condition: JoinExpressionInput
    type: JoinEnum = 'INNER'
    _counterpart = PrivateAttr(default_factory= lambda: JoinClause) 


class WhereClauseInput(SQLModel):
    expression: WhereExpressionInput
    _counterpart = PrivateAttr(default_factory= lambda: WhereClause) 

class HavingClauseInput(SQLModel):
    expression: HavingExpressionInput
    _counterpart = PrivateAttr(default_factory= lambda: HavingClause) 


class OrderByItemInput(SQLModel):
    operand: AggregateInput | ColumnOperandInput
    direction: DirectionEnum
    _counterpart = PrivateAttr(default_factory= lambda: OrderByItem) 


class OrderByClauseInput(SQLModel):
    items: List[OrderByItemInput] = Field(min_items=1)
    _counterpart = PrivateAttr(default_factory= lambda: OrderByClause) 

class GroupByClauseInput(SQLModel):
    items: List[ColumnOperandInput] = Field(min_items=1)
    _counterpart = PrivateAttr(default_factory= lambda: GroupByClause) 

class SelectClauseInput(SQLModel):
    columns: List[ColumnOperandInput] | List[AllCols] = Field(default_factory = lambda:AllCols['*'])  
    table_alias_id: int = Field(gt=0) 
    join_clause: JoinClauseInput | None = Field(default=None)
    _counterpart = PrivateAttr(default_factory= lambda: SelectClause) 

class SelectQueryInput(SQLModel):
    select: SelectClauseInput
    where: WhereClauseInput | None = Field(default=None)
    groupby: GroupByClauseInput | None = Field(default=None)
    having: HavingClauseInput | None = Field(default=None)
    orderby: OrderByClauseInput | None = Field(default=None)
    limit: LimitClause | None = Field(default=None)
    _counterpart = PrivateAttr(default_factory= lambda: SelectQuery) 



###########################
#       SERVER SIDE       #
###########################


class ColumnOperand(SQLModel):
    column: Alias
    function: OperandFuncEnum | None = Field(default=None)  

    @field_validator('column')
    def is_table(cls, v: Alias):
        if v.is_table:
            raise ValueError(f'Alias {v.alias} for target {v.target} is not a valid column -> It is a table.')

    def sql(self):
        return f'{self.function}({self.column.sql()})' if self.function is not None else f'{self.column.sql()}'
    
    @classmethod #This method converts ColumnOperandInput -> ColumnOperand
    async def from_input(cls, inp: ColumnOperandInput):
        column = await get_or_fetch_alias(alias_id=inp.col_alias_id)
        return ColumnOperand.model_construct(column=column, function=inp.function)

class Aggregate(SQLModel):

    function: AggregatesEnum
    arg: ColumnOperand

    def sql(self):
        return f'{self.function}({self.arg.sql()})'

    class Config:
        arbitrary_types_allowed=True
    
    @classmethod #This method converts AggregateInput -> AggregateOperand
    async def from_input(cls, inp: AggregateInput):
        return Aggregate.model_construct(
            function=inp.function,
            arg = await inp.arg._counterpart.from_input(inp=inp.arg)
        )


class HavingCondition(SQLModel):
    left: Aggregate | ColumnOperand
    operator: Operators 
    right: str | int | float 
    
    def sql(self):
        return f'{self.left.sql()} {self.operator} {self.right}'
    
    @classmethod
    async def from_input(cls, inp: HavingConditionInput):
        return HavingCondition.model_construct(
            left = await inp.left._counterpart.from_input(inp=inp.left),
            operator = inp.operator,
            right = inp.right
        )

class WhereCondition(SQLModel):
    left: ColumnOperand
    operator: Operators 
    right: ColumnOperand | str | int | float
    
    def sql(self):
        l,r = self.left, self.right

        if isinstance(l, ColumnOperand):
            l = l.sql()
        if isinstance(r, ColumnOperand):
            r = r.sql()

        return f'{l} {self.operator} {r}'
    

    @classmethod
    async def from_input(cls, inp: WhereConditionInput):
        return WhereCondition.model_construct(
            left = await inp.left._counterpart.from_input(inp=inp.left),
            operator = inp.operator,
            right = (await inp.right._counterpart.from_input(inp=inp.right)) if hasattr(inp.right, "_counterpart") else inp.right #basically ColumnOperandInput -> ColumnOperand if self is ColumnOperandInput
        )

class JoinCondition(SQLModel):
    left: ColumnOperand
    operator: Operators
    right: ColumnOperand  

    def sql(self):
        return f'{self.left.sql()} {self.operator} {self.right.sql()}'

    @classmethod
    async def from_input(cls, inp: JoinConditionInput):
        return JoinCondition.model_construct(
            left = await inp.left._counterpart.from_input(inp=inp.left),
            operator = inp.operator,
            right = await inp._counterpart.from_input(inp=inp.right)
        )


class JoinLogical(SQLModel):
    operator: LogicalEnum
    operands: List['JoinExpression']

    def sql(self):
        return f"({' {} '.format(self.operator).join([x.sql() for x in self.operands])})"

    @classmethod
    async def from_input(cls, inp: JoinLogicalInput):
        return JoinLogical.model_construct(
            operator = inp.operator,
            operands = [await x._counterpart.from_input(inp=x) for x in inp.operands] #i.e. List["JoinExpressionInput"] -> List["JoinExpression"]
        )


JoinExpression = JoinCondition | JoinLogical 
JoinLogical.model_rebuild()

class WhereLogical(SQLModel):
    operator: LogicalEnum
    operands: List['WhereExpression']

    def sql(self):
        return f"({' {} '.format(self.operator).join([x.sql() for x in self.operands])})"

    @classmethod
    async def from_input(cls, inp: WhereLogicalInput):
        return WhereLogical.model_construct(
            operator = inp.operator,
            operands = [await x._counterpart.from_input(inp=x) for x in inp.operands]
        )

class WhereNot(SQLModel):
    expression: 'WhereExpression'

    def sql(self):
        return f"(NOT {self.expression.sql()})"
    
    @classmethod
    async def from_input(cls, inp: WhereNotInput):
        return WhereNot.model_construct(
            expression = await inp.expression._counterpart.from_input(inp=inp.expression)
        )

WhereExpression = WhereCondition | WhereLogical | WhereNot
WhereLogical.model_rebuild()
WhereNot.model_rebuild()

class HavingLogical(SQLModel):
    operator: LogicalEnum
    operands: List['HavingExpression']

    def sql(self):
        return f"({' {} '.format(self.operator).join([x.sql() for x in self.operands])})"
    

    @classmethod
    async def from_input(cls, inp: HavingLogicalInput):
        return HavingLogical.model_construct(
            operator = inp.operator,
            operands = [await x._counterpart.from_input(inp=x) for x in inp.operands]
        )


class HavingNot(SQLModel):
    expression: 'HavingExpression'

    def sql(self):
        return f"(NOT {self.expression.sql()})"
    
    @classmethod
    async def from_input(cls, inp: HavingNotInput):
        return HavingNot.model_construct(
            expression = await inp.expression._counterpart.from_input(inp=inp.expression)
        )

HavingExpression = HavingCondition | HavingLogical | HavingNot
HavingLogical.model_rebuild()
HavingNot.model_rebuild()






class JoinClause(SQLModel):
    table: Alias
    on_condition: JoinExpression
    type: JoinEnum = 'INNER'

    @field_validator('table')
    def is_table(cls, v: Alias):
        if not v.is_table:
            raise ValueError(f'Alias {v.alias} for target {v.target} is not a valid table name.')

    def sql(self):
        return f"{self.type} JOIN {self.table.sql()} ON {self.on_condition.sql()}"

    @classmethod
    async def from_input(cls, inp: JoinClauseInput):
        
        return JoinClause.model_construct(
            table = await get_or_fetch_alias(alias_id=inp.table_alias_id),
            on_condition = await inp.on_condition._counterpart.from_input(inp=inp.on_condition),
            expression = inp.type
        )


class WhereClause(SQLModel):
    expression: WhereExpression
    def sql(self):
        return f'WHERE {self.expression.sql()}'

    @classmethod
    async def from_input(cls, inp: WhereClauseInput):
        return WhereClause.model_construct(
            expression = await inp.expression._counterpart.from_input(inp=inp.expression)
        )



class HavingClause(SQLModel):
    expression: HavingExpression

    def sql(self):
        return f'HAVING {self.expression.sql()}'

    @classmethod
    async def from_input(cls, inp: HavingClauseInput):
        return HavingClause.model_construct(
            expression = await inp.expression._counterpart.from_input(inp=inp.expression)
        )





class OrderByItem(SQLModel):
    operand: Aggregate | ColumnOperand
    direction: DirectionEnum

    def sql(self):
        return f'{self.operand.sql()} {self.direction.upper()}'
    
    @classmethod
    async def from_input(cls, inp: OrderByItemInput):
        return OrderByItem.model_construct(
            operand = await inp.operand._counterpart.from_input(inp=inp.operand),
            direction= inp.direction
        )


class OrderByClause(SQLModel):
    items: List[OrderByItem] = Field(min_items=1)

    def sql(self):
        return f"ORDER BY {','.join([item.sql() for item in self.items])}"

    @classmethod
    async def from_input(cls, inp: OrderByClauseInput):
        return OrderByClause.model_construct(
            items = [await x._counterpart.from_input(inp=x) for x in inp.items]
        )


class GroupByClause(SQLModel):
    items: List[ColumnOperand] = Field(min_items=1)

    def sql(self):
        return f"GROUP BY {','.join([item.sql() for item in self.items])}"

    @classmethod
    async def from_input(cls, inp: GroupByClauseInput):
        return GroupByClause.model_construct(
            items = [await x._counterpart.from_input(inp=x) for x in inp.items]
        )


class SelectClause(SQLModel):
    columns: List[ColumnOperand] | List[AllCols] = Field(default_factory = lambda:AllCols['*']) 
    from_: Alias
    join_clause: JoinClause | EmptyClause = Field(default_factory = lambda:EmptyClause())

    @field_validator('from_')
    def is_table(cls, v: Alias):
        if not v.is_table:
            raise ValueError(f'Alias {v.alias} for target {v.target} is not a valid table name.')

    def sql(self):
        return f"SELECT {', '.join([x.sql() for x in self.columns])} FROM {self.from_.sql()} {self.join_clause.sql()}"
    

    @classmethod
    async def from_input(cls, inp: SelectClauseInput):
        from_ =  await get_or_fetch_alias(alias_id=inp.table_alias_id)
        columns = [(await x._counterpart.from_input(inp=x)) if isinstance(x, ColumnOperandInput) else AllCols('*') for x in inp.columns]
        join = (await inp.join_clause._counterpart.from_input(inp=inp.join_clause)) if inp.join_clause is not None else EmptyClause()
        logger.info(f'from_ : {from_}')
        obj = SelectClause.model_construct(
            columns = columns, 
            from_ = from_,
            join_clause = join
        )

        logger.info(f'HOW SELECT CLAUSE SEES ITSELF {obj}')
        return obj 

class SelectQuery(SQLModel):
    select: SelectClause
    where: WhereClause | EmptyClause = Field(default_factory = lambda:EmptyClause())
    groupby: GroupByClause | EmptyClause = Field(default_factory = lambda:EmptyClause())
    having: HavingClause | EmptyClause = Field(default_factory = lambda:EmptyClause())
    orderby: OrderByClause | EmptyClause = Field(default_factory = lambda:EmptyClause())
    limit: LimitClause | EmptyClause = Field(default_factory = lambda:EmptyClause())

    def sql(self):
        return f"{self.select.sql()} {self.where.sql()} {self.groupby.sql()} {self.having.sql()} {self.orderby.sql()} {self.limit.sql()}".strip() + ';'


    @classmethod 
    async def from_input(cls, inp: SelectQueryInput) -> "SelectQuery":

        select = await inp.select._counterpart.from_input(inp=inp.select)
        where = await inp.where._counterpart.from_input(inp=inp.where) if inp.where is not None else EmptyClause()
        groupby = await inp.groupby._counterpart.from_input(inp=inp.groupby) if inp.groupby is not None else EmptyClause()
        having = await inp.having._counterpart.from_input(inp=inp.having) if inp.having is not None else EmptyClause()
        orderby = await inp.orderby._counterpart.from_input(inp=inp.orderby) if inp.orderby is not None else EmptyClause()
        limit = inp.limit if inp.limit is not None else EmptyClause()

        logger.info(f'{select.from_}')
        return SelectQuery.model_construct(
            select = select,
            where = where,
            groupby = groupby,
            having = having,
            orderby =  orderby,
            limit = limit,
        )



