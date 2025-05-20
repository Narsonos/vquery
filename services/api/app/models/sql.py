from app.models.common import Alias, Query
from app.utils import exceptions as exc, context as ctx

from typing import List, Literal, Tuple, Union, Annotated, Optional
from sqlmodel import SQLModel, Field, select
from pydantic import field_validator, PrivateAttr, model_validator
from enum import Enum
from sqlalchemy.ext.asyncio import AsyncSession

import logging
logger = logging.getLogger('applogger')



##############################
#           ENUMS            #
##############################


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
        raise exc.ValueNotAllowed({'used': value, 'allowed': [v.value for v in cls]})


class OperandFuncEnum(str, Enum):
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    DATE = "DATE"
    FLOOR = "FLOOR"
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
        raise exc.ValueNotAllowed({'msg':'The value you used is not allowed!','used': value, 'allowed': [v.value for v in cls]})

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
        raise exc.ValueNotAllowed({'msg':'The value you used is not allowed!','used': value, 'allowed': [v.value for v in cls]})
    
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
        raise exc.ValueNotAllowed({'msg':'The value you used is not allowed!','used': value, 'allowed': [v.value for v in cls]})
    
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
        raise exc.ValueNotAllowed({'msg':'The value you used is not allowed!','used': value, 'allowed': [v.value for v in cls]})

class DateUnitEnum(Enum):
    DAY = "DAY"
    MONTH = "MONTH"
    YEAR = "YEAR"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper = value.upper()
            for member in cls:
                if member.value == upper:
                    return member
        raise exc.ValueNotAllowed({'msg':'The value you used is not allowed!','used': value, 'allowed': [v.value for v in cls]})
    
class AllCols(str, Enum):
    AllCols = '*'
    
    def sql(self):
        return '*'
    
    @property
    def _counterpart(self):
        return self.__class__
    
    @classmethod
    async def from_input(cls, inp: 'AllCols'):
        return inp
    
    

CompOperators = Literal['=', '>', '<', '>=', '<=', '<>', 'REGEXP']
Operators = Literal["+", "-", "/", "*", '%', 'DIV']

class ColumnRepresentation(SQLModel):
    name: str
    dtype: str

class SimpleSchema(SQLModel):
    name: str
    columns: List[ColumnRepresentation]


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
    _counterpart = PrivateAttr(default_factory= lambda: _Expr)
    type: str
    

class BinaryOperation(Expr):
    _counterpart = PrivateAttr(default_factory= lambda: _BinaryOperation) 
    type: Literal["op"]
    left: "Expression"
    operation: Operators
    right: "Expression"
    

class FunctionCall(Expr):
    _counterpart = PrivateAttr(default_factory= lambda: _FunctionCall) 
    type: Literal["func"]
    func: OperandFuncEnum | AggregatesEnum
    args: List["Expression"] | List[AllCols]
    

class IntervalExpr(Expr):
    _counterpart = PrivateAttr(default_factory= lambda: _IntervalExpr)
    type: Literal["interval"]
    value: int
    unit: DateUnitEnum
    

class CaseItem(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: _CaseItem)
    case: 'BooleanExpression'
    then: 'Expression'
    

class CaseExpr(Expr):
    _counterpart = PrivateAttr(default_factory= lambda: _CaseExpr)
    type: Literal['case']
    cases: List[CaseItem] = Field(min_length=1)
    default: 'Expression'




#these two represent atomic database objects
#They do not need counterparts - because they are always stored as parts of aliases.
class TableOperand(Expr):
    _counterpart = PrivateAttr(default_factory= lambda: TableOperand) 
    type: Literal["table"]
    table: str
    is_aggregate: bool = False
   

    @classmethod
    async def from_input(cls, inp: 'TableOperand'):
        return TableOperand.model_construct(type=inp.type, table=inp.table)
    
    def sql(self):
        return f'`{self.table}`'

class ColumnOperand(Expr):
    _counterpart = PrivateAttr(default_factory= lambda: ColumnOperand) 
    type: Literal["col"]
    table: str
    column: str
    is_aggregate: bool = False
    

    @classmethod
    async def from_input(cls, inp: 'ColumnOperand'):
        return ColumnOperand.model_construct(type=inp.type, table=inp.table, column=inp.column)

    def sql(self):
        return f'`{self.table}`.`{self.column}`'

class BooleanExpr(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: _BooleanExpr)
    type: str
    






class AliasedExpr(Expr, BooleanExpr):
    _counterpart = PrivateAttr(default_factory= lambda: _AliasedExpr) 
    type: Literal["aliased"]
    alias_id: Optional[int] = None
    alias_name: Optional[str] = None
    

    @model_validator(mode='after')
    def check_exclusive_fields(self) -> 'AliasedExpr':
        if bool(self.alias_id) == bool(self.alias_name):
            raise ValueError("Exactly one of 'alias_id' or 'alias_name' must be provided")
        return self


class NotExpr(BooleanExpr):
    _counterpart = PrivateAttr(default_factory= lambda: _NotExpr)
    type: Literal["not"]
    operand: "BooleanExpression"
    


class BooleanBinaryExpr(BooleanExpr):
    _counterpart = PrivateAttr(default_factory= lambda: _BooleanBinaryExpr)
    type: Literal["and_or"]
    left: "BooleanExpression"
    bool_op: LogicalEnum  # Только AND / OR
    right: "BooleanExpression"
    


class ComparisonExpr(BooleanExpr):
    _counterpart = PrivateAttr(default_factory= lambda: _ComparisonExpr)
    type: Literal["compare"]
    left: "Expression"
    operator: CompOperators  # <, >, =, <=, ...
    right: "Expression"


class BetweenExpr(BooleanExpr):
    _counterpart = PrivateAttr(default_factory= lambda: _BetweenExpr)
    type: Literal["between"]
    expr: "Expression"
    upper: "Expression"
    lower: "Expression"
    negate: bool = Field(default=False)

class IsNull(BooleanExpr):
    _counterpart = PrivateAttr(default_factory= lambda: _IsNull)
    type: Literal["isnull"]
    operand: "Expression"
    

class InExpr(BooleanExpr):
    _counterpart = PrivateAttr(default_factory=lambda: _InExpr)
    type: Literal['in']
    left: "Expression"
    right: 'SelectSubquery'

class ExistsExpr(BooleanExpr):
    _counterpart = PrivateAttr(default_factory=lambda: _ExistsExpr)
    type: Literal['exists']
    subquery: 'SelectSubquery'



class AliasCreate(SQLModel):
    alias: str = Field(description="Alias string that can be used to represent a field/table in the onwer's context", )
    target: "Expression" = Field(description="An field/table represented by an alias")

    @field_validator('alias')
    @classmethod
    def validate_alias(cls, v):
        if ':' in v:
            raise ValueError('Alias name cannot contain colon (:)')
        return v


def extract_cols_from_expression_tree(expr: "Expression") -> List[ColumnOperand]:
    result = []
    if isinstance(expr, ColumnOperand):
        result.append(expr)
    elif isinstance(expr, BinaryOperation):
        result.extend(extract_cols_from_expression_tree(expr.left))
        result.extend(extract_cols_from_expression_tree(expr.right))
    elif isinstance(expr, FunctionCall):
        for arg in expr.args:
            result.extend(extract_cols_from_expression_tree(arg))
    elif isinstance(expr, TableOperand):
        raise ValueError('Wrong type of operand! Table operand can NOT be used in any expression, i.e. (table + 5, or func(table))')
    return result







class JoinClauseInput(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: JoinClause) 
    table: 'AliasedExpr | SelectSubquery'
    on_condition: "BooleanExpression"
    type: JoinEnum = 'INNER'
    


class WhereClauseInput(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: WhereClause) 
    expression: "BooleanExpression"
    

class HavingClauseInput(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: HavingClause) 
    expression: "BooleanExpression"
    


class OrderByItemInput(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: OrderByItem) 
    operand: "Expression"
    direction: DirectionEnum
    


class OrderByClauseInput(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: OrderByClause) 
    items: List[OrderByItemInput] = Field(min_items=1)
    

class GroupByClauseInput(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: GroupByClause) 
    items: List["Expression"] = Field(min_items=1)
    



class SelectClauseInput(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: SelectClause) 
    columns: List['AliasedExpr | SelectSubquery | ColumnOperand'] | List[AllCols] = Field(default_factory = lambda:AllCols['*'])  
    from_: 'AliasedExpr | SelectSubquery'
    join_clause: JoinClauseInput | None = Field(default=None)
    

class SelectQueryInput(SQLModel):
    _counterpart = PrivateAttr(default_factory= lambda: SelectQuery) 
    select: SelectClauseInput
    where: WhereClauseInput | None = Field(default=None)
    groupby: GroupByClauseInput | None = Field(default=None)
    having: HavingClauseInput | None = Field(default=None)
    orderby: OrderByClauseInput | None = Field(default=None)
    limit: LimitClause | None = Field(default=None)
    
class SelectSubquery(Expr):
    _counterpart = PrivateAttr(default_factory=lambda: _SelectSubquery)
    type:Literal["subquery"]
    query_id: int | None = None
    query_name: str | None = None
    

    @model_validator(mode='after')
    def check_exclusive_fields(self):
        if bool(self.query_id) == bool(self.query_name):
            raise ValueError("Exactly one of 'query_id' or 'query_name' must be provided")
        return self


#######################################
#       SERVER SIDE EXPRESSSIONS      #
#######################################


class _Expr(SQLModel):
    def sql(self) -> str:
        raise NotImplemented
    
    @property
    def is_aggregate(self) -> bool:
        raise NotImplemented

    @classmethod
    async def from_input(cls, inp) -> '_Expr':
        raise NotImplemented


#Works for both external Expr and and internal _Expr
class LiteralObject(Expr, _Expr):
    _counterpart = PrivateAttr(default_factory= lambda: LiteralObject) #returns itself just for polymorphiс expressions
    type: Literal["literal"]
    value: str | int | float | bool
    
     
    @property
    def is_aggregate(self):
        return False
    
    @field_validator('value')
    @classmethod
    def clean_string(cls, v: str|int|float|bool):
        if isinstance(v, str):
            v = v.replace("'", "''")
        return v


    def sql(self):
        if isinstance(self.value, str):
            return f"'{self.value}'"
        
        elif isinstance(self.value, (int, float)):
            return f"{self.value}"
        
        elif isinstance(self.value, bool):
            return f'{int(self.value)}'
    
    @classmethod 
    async def from_input(cls, inp):
        return inp


class _BinaryOperation(_Expr):
    left: _Expr
    operation: Operators
    right: _Expr

    @model_validator(mode='after')
    def ensure_subqueries_are_scalar(self):
        for v in [self.left, self.right]:
            if isinstance(v, _SelectSubquery):
                v.scalarize() 


    def sql(self):
        return  f'({self.left.sql()} {self.operation} {self.right.sql()})'

    @property
    def is_aggregate(self): 
        return self.left.is_aggregate or self.right.is_aggregate

    @classmethod 
    async def from_input(cls, inp: BinaryOperation):
        return _BinaryOperation.model_construct(
            left = await inp.left._counterpart.from_input(inp.left),
            operation = inp.operation,
            right = await inp.right._counterpart.from_input(inp.right)
            )


class _FunctionCall(_Expr):
    func: OperandFuncEnum | AggregatesEnum
    args: List[_Expr] | List[AllCols]

    def sql(self):
        return f"{self.func.value}({','.join([farg.sql() for farg in self.args])})" # => f(z,x,c,...) or f()

    @model_validator(mode='after')
    def ensure_subqueries_are_scalar(self):
        for v in self.args:
            if isinstance(v, _SelectSubquery):
                v.scalarize() 

    @property
    def is_aggregate(self):
        logger.info(f'IF FUNC {self.func.value} IS AGG: {self.func in AggregatesEnum}')
        return self.func in AggregatesEnum or any([x.is_aggregate for x in self.args]) #if this func is aggregate or any of it's arguments is aggregate

    @classmethod
    async def from_input(cls, inp: FunctionCall):
        return _FunctionCall.model_construct(
            func = inp.func,
            args = [await x._counterpart.from_input(x) for x in inp.args]
        )

class _IntervalExpr(Expr):
    type: Literal["interval"]
    value: int
    unit: DateUnitEnum

    def sql(self):
        return f"INTERVAL {self.value} {self.unit.value}"

    @property
    def is_aggregate(self):
        return False

    @classmethod
    async def from_input(cls, inp: IntervalExpr):
        return _IntervalExpr.model_construct(
            value = inp.value,
            unit = inp.unit
        )





class _BooleanExpr(SQLModel):
    def sql(self):
        raise NotImplemented
    
    @property
    def is_aggregate(self) -> bool:
        raise NotImplemented

    @classmethod
    async def from_input(cls, inp) -> '_BooleanExpr':
        raise NotImplemented
    

class _AliasedExpr(_Expr, _BooleanExpr):
    alias: Alias

    def sql_aliased(self):
        return f'`{self.alias.alias}`' #table.column or column

    def sql_as(self):
        return f'{self.alias.target} AS `{self.alias.alias}`'
    
    def sql(self):
        return f'{self.alias.target}'
    
    @property
    def is_aggregate(self):
        return self.alias.is_aggregate
    
    @classmethod
    async def from_input(cls, inp:AliasedExpr):
        user = ctx.current_user.get()
        dbsession = ctx.current_dbsession.get()
        redis = ctx.current_redis.get()

        alias = await Alias.fetch(alias_id=inp.alias_id, alias_name=inp.alias_name, user=user, dbsession=dbsession, redis=redis)
        
        return _AliasedExpr.model_construct(
            alias=alias
        )
    

class _NotExpr(_BooleanExpr):
    operand: _BooleanExpr

    def sql(self):
        return f'NOT ({self.operand.sql()})'

    @property
    def is_aggregate(self) -> bool:
        return self.operand.is_aggregate
    
    @classmethod
    async def from_input(cls, inp: NotExpr):
        return _NotExpr.model_construct(
            operand = await inp.operand._counterpart.from_input(inp.operand)
        )



class _BooleanBinaryExpr(_BooleanExpr):
    left: _BooleanExpr
    bool_op: LogicalEnum
    right: _BooleanExpr
    
    def sql(self):
        return f'({self.left.sql()} {self.bool_op.value} {self.right.sql()})'

    @property
    def is_aggregate(self) -> bool:
        return self.left.is_aggregate or self.right.is_aggregate
    
    @classmethod
    async def from_input(cls, inp: BooleanBinaryExpr):
        return _BooleanBinaryExpr.model_construct(
            left = await inp.left._counterpart.from_input(inp.left),
            bool_op = inp.bool_op,
            right = await inp.right._counterpart.from_input(inp.right)
        )



class _ComparisonExpr(_BooleanExpr):
    left: _Expr
    operator: Operators
    right: _Expr

    @model_validator(mode='after')
    def ensure_subqueries_are_scalar(self):
        for v in [self.left, self.right]:
            if isinstance(v, _SelectSubquery):
                v.scalarize() 

    def sql(self):
        return f'({self.left.sql()} {self.operator} {self.right.sql()})'

    @property
    def is_aggregate(self) -> bool:
        return self.left.is_aggregate or self.right.is_aggregate
    
    @classmethod
    async def from_input(cls, inp: ComparisonExpr):
        return _ComparisonExpr.model_construct(
            left = await inp.left._counterpart.from_input(inp.left),
            operator = inp.operator,
            right = await inp.right._counterpart.from_input(inp.right)
        )



class _BetweenExpr(_BooleanExpr):
    expr: _Expr
    upper: _Expr
    lower: _Expr
    negate: bool = Field(default=False)

    @model_validator(mode='after')
    def ensure_subqueries_are_scalar(self):
        for v in [self.expr, self.upper, self.lower]:
            if isinstance(v, _SelectSubquery):
                v.scalarize() 



    def sql(self):
        return f'{self.expr.sql()} {"NOT" if self.negate else ""} BETWEEN {self.lower.sql()} AND {self.upper.sql()}'
    
    @property
    def is_aggregate(self) -> bool:
        return self.expr.is_aggregate or self.upper.is_aggregate or self.lower.is_aggregate
    
    @classmethod
    async def from_input(cls, inp: BetweenExpr):
        return _BetweenExpr.model_construct(
            expr = await inp.expr._counterpart.from_input(inp.expr),
            upper = await inp.upper._counterpart.from_input(inp.upper),
            lower = await inp.lower._counterpart.from_input(inp.lower),
            negate = inp.negate
        )

class _IsNull(_BooleanExpr):
    operand: _Expr

    @field_validator('operand', mode='after')
    @classmethod
    def ensure_subqueries_are_scalar(cls, v):
        if isinstance(v, _SelectSubquery):
            v.scalarize() 

    def sql(self):
        return f'({self.operand.sql()} IS NULL)'

    @property
    def is_aggregate(self) -> bool:
        return self.operand.is_aggregate
    
    @classmethod
    async def from_input(cls, inp:IsNull):
        return _IsNull.model_construct(operand = await inp.operand._counterpart.from_input(inp.operand))


class _InExpr(_BooleanExpr):
    left: _Expr
    right: '_SelectSubquery'

    @field_validator('right', mode='after')
    @classmethod
    def ensure_the_subquery_has_a_single_column(cls, sub:'_SelectSubquery'):
        if sub.query.col_count != 1:
            raise ValueError('The subquery has col_count not equal to 1, which is vital for SQL IN Expression.')

    def sql(self):
        return f'({self.left.sql()} IN {self.right.sql()})'

    @property
    def is_aggregate(self) -> bool:
        return self.left.is_aggregate or self.right.is_aggregate
    
    @classmethod
    async def from_input(cls, inp:InExpr):
        return _InExpr.model_construct(
            left = await inp.left._counterpart.from_input(inp.left),
            right = await inp.right._counterpart.from_input(inp.right)
            )            

class _ExistsExpr(_BooleanExpr):
    subquery: '_SelectSubquery'

    def sql(self):
        return f'EXISTS {self.subquery.sql()}'

    @property
    def is_aggregate(self) -> bool:
        return False
    
    @classmethod
    async def from_input(cls, inp:ExistsExpr):
        return _ExistsExpr.model_construct(
            subquery = await inp.subquery._counterpart.from_input(inp.subquery),
        )



class _CaseItem(_Expr):
    case: _BooleanExpr
    then: _Expr


    @field_validator('then', mode='after')
    @classmethod
    def ensure_subqueries_are_scalar(cls, v):
        if isinstance(v, _SelectSubquery):
            v.scalarize() 

    def sql(self):
        return f"WHEN {self.case.sql()} THEN {self.then.sql()}"

    @property
    def is_aggregate(self):
        return self.case.is_aggregate or self.then.is_aggregate

    @classmethod
    async def from_input(cls, inp: CaseItem):
        return _CaseItem.model_construct(
            case = await inp.case._counterpart.from_input(inp.case),
            then = await inp.then._counterpart.from_input(inp.then)
        )

class _CaseExpr(_Expr):
    cases: List[_CaseItem] = Field(min_length=1)
    default: _Expr

    def sql(self):
        cases_sql = " ".join(case.sql() for case in self.cases)
        return f"CASE {cases_sql} ELSE {self.default.sql()} END"

    @property
    def is_aggregate(self):
        return all([case.is_aggregate for case in self.cases]) or self.default.is_aggregate

    @classmethod
    async def from_input(cls, inp: CaseExpr):
        return _CaseExpr.model_construct(
            cases = [await case._counterpart.from_input(case) for case in inp.cases],
            default = await inp.default._counterpart.from_input(inp.default)
        )




#################################
#       SERVER SIDE CLAUSES     #
#################################



class JoinClause(SQLModel):
    table: '_AliasedExpr | _SelectSubquery'
    on_condition: _BooleanExpr
    type: JoinEnum = 'INNER'

    @field_validator('table')
    def is_table(cls, v: '_AliasedExpr | _SelectSubquery'):
        if isinstance(v, _AliasedExpr):
            if not v.alias.is_table:
                raise ValueError(f'Join Clause got wrong alias object as table. Its sql: {v.sql()}')

    def sql(self):
        return f"{self.type.value} JOIN {self.table.sql_as() if isinstance(self.table, _SelectSubquery) else self.table.sql()} ON {self.on_condition.sql()}"

    @classmethod
    async def from_input(cls, inp: JoinClauseInput):
        on_condition = await inp.on_condition._counterpart.from_input(inp=inp.on_condition)
        if on_condition.is_aggregate:
            raise exc._ImproperAggregateUse('JOIN clause does not allow aggregate usage! "on-condition" part seems to be aggregate.')

        return JoinClause.model_construct(
            table = await inp.table._counterpart.from_input(inp.table),
            on_condition = on_condition,
            type = inp.type
        )


class WhereClause(SQLModel):
    expression: _BooleanExpr
    def sql(self):
        return f'WHERE {self.expression.sql()}'

    @classmethod
    async def from_input(cls, inp: WhereClauseInput):
        expression = await inp.expression._counterpart.from_input(inp=inp.expression)
        if expression.is_aggregate:
            raise exc._ImproperAggregateUse('WHERE clause does not allow aggregate usage! Passed expression seems to be aggregate.')
        return WhereClause.model_construct(
            expression = expression
        )



class HavingClause(SQLModel):
    expression: _BooleanExpr

    def sql(self):
        return f'HAVING {self.expression.sql()}'

    @classmethod
    async def from_input(cls, inp: HavingClauseInput):
        return HavingClause.model_construct(
            expression = await inp.expression._counterpart.from_input(inp=inp.expression)
        )





class OrderByItem(SQLModel):
    operand: _Expr
    direction: DirectionEnum

    def sql(self):
        return f'{self.operand.sql()} {self.direction.upper()}'

    @field_validator('operand', mode='after')
    @classmethod
    def ensure_subqueries_are_scalar(cls, v):
        if isinstance(v, _SelectSubquery):
            v.scalarize() 


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
    items: List[_Expr] = Field(min_items=1)

    def sql(self):
        return f"GROUP BY {','.join([item.sql() for item in self.items])}"

    @field_validator('items', mode='after')
    @classmethod
    def ensure_subqueries_are_scalar(cls, items):
        for item in items:
            if isinstance(items, _SelectSubquery):
                items.scalarize() #makes col._sql scalar or throws exception

    @classmethod
    async def from_input(cls, inp: GroupByClauseInput):
        items = [await x._counterpart.from_input(inp=x) for x in inp.items]

        if any(item.is_aggregate for item in items):
            raise exc._ImproperAggregateUse('GROUP BY clause does not allow aggregate usage! One of passed items seems to be aggregate.')

        return GroupByClause.model_construct(
            items = items
        )




class SelectClause(SQLModel):
    columns: List['_AliasedExpr | _SelectSubquery | ColumnOperand'] | List[AllCols] = Field(default_factory = lambda:AllCols['*']) 
    from_: '_AliasedExpr |_SelectSubquery'
    join_clause: JoinClause | EmptyClause = Field(default_factory = lambda:EmptyClause())

    @field_validator('from_')
    @classmethod
    def is_table(cls, v: _AliasedExpr):
        if not v.alias.is_table:
            raise ValueError(f'Alias {v.alias} for target {v.sql()} is not a valid table name.')

    @field_validator('columns', mode='after')
    @classmethod
    def ensure_subqueries_are_scalar(cls, cols: List['_AliasedExpr | _SelectSubquery | ColumnOperand'] | List[AllCols]):
        for col in cols:
            if isinstance(col, _SelectSubquery):
                col.scalarize() #makes col._sql scalar or throws exception


    def sql(self):
        cols_sql = ', '.join([x.sql_as() if isinstance(x,(_AliasedExpr,_SelectSubquery)) else x.sql() for x in self.columns])
        from_sql = self.from_.sql_as() if isinstance(self.from_,_SelectSubquery) else self.from_.sql()
        join_sql = self.join_clause.sql()
        return f"SELECT {cols_sql} FROM {from_sql} {join_sql}"
    

    @classmethod
    async def from_input(cls, inp: SelectClauseInput):
        from_ =  await inp.from_._counterpart.from_input(inp=inp.from_)
        columns = [(await x._counterpart.from_input(inp=x)) if isinstance(x, (AliasedExpr, ColumnOperand, SelectSubquery)) else AllCols('*') for x in inp.columns]
        join = (await inp.join_clause._counterpart.from_input(inp=inp.join_clause)) if inp.join_clause is not None else EmptyClause()

        obj = SelectClause.model_construct(
            columns = columns, 
            from_ = from_,
            join_clause = join
        )

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

    @property
    def get_col_count(self):
        return -1 if self.select.columns == ['*'] else len(self.select.columns)



class _SelectSubquery(_Expr):
    query: Query
    _sql: str

    def sql(self):
        return f'({self._sql})'  #removes ";" from the end of subquery  

    def sql_as(self):
        return f'({self._sql}) AS `{self.query.name}`'
    
    def scalarize(self):
        self._sql = self.query.scalarize()

    @property
    def is_aggregate(self): #We consider subquery WITHIN expressions as a SCALAR value, thus it's not aggregate. Behaves as such
        return False

    @classmethod 
    async def from_input(cls, inp: SelectSubquery) -> "_SelectSubquery":
        user = ctx.current_user.get()
        dbsession = ctx.current_dbsession.get()
        redis = ctx.current_redis.get()

        query: Query = await Query.fetch(query_id=inp.query_id, query_name=inp.query_name, user=user, dbsession=dbsession, redis=redis)

        return _SelectSubquery.model_construct(
            query = query,
            _sql = query.query_sql.rstrip(";")
        )


Expression = Annotated[Union[
    FunctionCall,
    BinaryOperation,
    LiteralObject,
    AliasedExpr,
    ColumnOperand,
    TableOperand,
    IntervalExpr,
    CaseExpr,
    SelectSubquery
    ], Field(discriminator='type')]

BooleanExpression = Annotated[Union[
    NotExpr,
    BooleanBinaryExpr,
    ComparisonExpr,
    AliasedExpr,
    BetweenExpr,
    IsNull,
    InExpr
    ], Field(discriminator='type')]





