#Fastapi
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse


#Project files
from app.utils.common import convert_input_query
from app.config import Config
from app.db import RedisDependency, DBDependency, get_users, get_all_table_names, get_sql_schema
import app.utils.security as security
import app.utils.exceptions as exc
import app.models.common as models
import app.models.sql as sqllib


#SQLAlchemy/SQLModel
from sqlmodel import select, insert, delete
import sqlalchemy.exc as sqlexc
from sqlalchemy import text

#Pydantic/Typing
from typing import Annotated, List, get_args

import traceback




router = APIRouter(
    prefix="/actions",
    tags = ["actions"],
    responses={404: {"description": "Requested resource is not found"}}
    )

import logging
logger = logging.getLogger('applogger')


@router.get('/execute')
async def execute(user: security.CurrentUserDependency, dbsession:DBDependency, query_id: int):

    query = await dbsession.scalar(select(models.Query).where(models.Query.owner_id==user.id, models.Query.query_id==query_id))
    if not query:
        raise exc.QueryNotExists
    

    try:
        result = (await dbsession.execute(text(query.query_sql))).all()
        return [row._asdict() for row in result]
    except exc.SQLException as e:
        logger.debug(f'Exception: {e} of class {e.__class__.__name__}\nTraceback:{"".join(traceback.format_tb(e.__traceback__))}')
        raise e.get_http_exception()
    except Exception as e:
        logger.debug(f'Exception: {e} of class {e.__class__.__name__}\nTraceback:{"".join(traceback.format_tb(e.__traceback__))}')
        raise exc.InvalidQuery from e


@router.get('/get_queries')
async def get_queries(user: security.CurrentUserDependency, dbsession:DBDependency, query_id: int|None = None) -> List[models.Query]:
    """If query_id was passed -> return specific query, else -> returns all of them"""
    q = select(models.Query).where(models.Query.owner_id == user.id) 
    q = q.where(models.Query.query_id == query_id) if query_id is not None else q
    return await dbsession.scalars(q)

@router.get('/del_query')
async def del_query(user: security.CurrentUserDependency, dbsession: DBDependency, query_id: int):
    await dbsession.execute(delete(models.Query).where(models.Query.owner_id == user.id, models.Query.query_id==query_id))
    await dbsession.commit()
    return JSONResponse(content={"msg":f"Query was deleted successfully (if existed)"})


@router.post('/new_query')
async def new_query(
    user: security.CurrentUserDependency,
    dbsession:DBDependency,
    name: str,
    query: sqllib.SelectQuery = Depends(convert_input_query)
    ):
    
    try:
        await dbsession.execute(text(query.sql()))
    except exc.SQLException as e:
        logger.debug(f'Exception: {e} of class {e.__class__.__name__}\nTraceback:{"".join(traceback.format_tb(e.__traceback__))}')
        raise e.get_http_exception()
    except Exception as e:
        logger.debug(f'Exception: {e} of class {e.__class__.__name__}\nTraceback:{"".join(traceback.format_tb(e.__traceback__))}')
        raise exc.InvalidQuery from e

    try:
        q = models.Query(owner_id=user.id, name=name, query_sql=query.sql())
        dbsession.add(q)
        await dbsession.commit()
        await dbsession.refresh(q)
        return JSONResponse(content={'msg':'Query added successfully', 'obj': q.model_dump()})
    except sqlexc.IntegrityError:
        raise exc.QueryAlreadyExists


@router.get('/schemas')
async def get_schemas(user: security.CurrentUserDependency, dbsession:DBDependency, table_name: str | None = None, names_only: bool = False) -> List[str] | sqllib.SimpleSchema | List[sqllib.SimpleSchema]:
    """
    Returns all allowed tables schemas. 
    - If passed table_name -> Returns a schema of that table
    - names_only - if True returns a list of name strings
    Use to fetch allowed options for Alias creation
    """
    
    allowed_tables: List[str] = await get_all_table_names(dbsession, exclude_system=True)
    
    if table_name:
        if table_name in allowed_tables:
            if not names_only:
                schema = await get_sql_schema(tname=table_name, session=dbsession, as_string=False)
                return sqllib.SimpleSchema(name=table_name, columns=[sqllib.ColumnRepresentation(name=name, dtype=dt) for name,dt in schema])
            return [table_name]
        else:
            raise exc.TableNotExists
    else:
        if names_only:
            return allowed_tables
        schemas = []
        for tname in allowed_tables:
            schema = await get_sql_schema(tname=tname, session=dbsession, as_string=False)
            cols = [sqllib.ColumnRepresentation(name=name, dtype=dt) for name,dt in schema]
            schema = sqllib.SimpleSchema(name=tname, columns=[sqllib.ColumnRepresentation(name=name, dtype=dt) for name,dt in schema])
            schemas.append(schema)
        return schemas

@router.get('/tables')
async def get_tables(user: security.CurrentUserDependency, dbsession:DBDependency, table_name: str) -> List[dict]:
    allowed_tables: List[str] = await get_all_table_names(dbsession,exclude_system=True)
    if table_name not in allowed_tables:
        raise exc.TableNotExists(f'Table {table_name} does not exist!')
    return [row._asdict() for row in (await dbsession.execute(text(f'SELECT * FROM {table_name};'))).all()]    



@router.get('/get_aliases')
async def get_aliases(user: security.CurrentUserDependency, dbsession: DBDependency, alias_id: int|None = None) -> List[models.Alias]:
    q = select(models.Alias).where(models.Alias.owner_id == user.id) 
    q = q.where(models.Alias.alias_id == alias_id) if alias_id is not None else q
    return await dbsession.scalars(q)

@router.get('/del_alias')
async def del_alias(user: security.CurrentUserDependency, dbsession: DBDependency, alias_id: int):
    await dbsession.execute(delete(models.Alias).where(models.Alias.owner_id == user.id, models.Alias.alias_id==alias_id))
    await dbsession.commit()
    return JSONResponse(content={"msg":f"Alias was deleted successfully (if existed)"})

@router.post('/new_alias')
async def new_alias(user: security.CurrentUserDependency, dbsession:DBDependency, new_alias: sqllib.AliasCreate):
    
    is_aggreagte = (await new_alias.target._counterpart.from_input(inp=new_alias.target)).is_aggregate

    allowed_tables: List[str] = await get_all_table_names(dbsession, exclude_system=True)
    if isinstance(new_alias.target, sqllib.TableOperand):
        is_table = True
        if new_alias.target.table not in allowed_tables:
            raise exc.AliasTargetNotExists
        
    else:
        is_table = False
        #List of ColumnOperand which are "table.column" type of objects
        try:
            col_refs = sqllib.extract_cols_from_expression_tree(new_alias.target)
        except ValueError:
            raise exc.TableInsteadOfColumn
        #ensure that all col refs are col_refs
        for col in col_refs:
            if col.table not in allowed_tables:
                raise exc.AliasTargetNotExists
            real_schema = await get_sql_schema(tname=col.table, session=dbsession, as_string=False, only_names=True)
            if col.column not in real_schema:
                raise exc.AliasTargetNotExists
    try:

        aliased_obj_sql_string = (await new_alias.target._counterpart.from_input(inp=new_alias.target)).sql()
        #То есть в Alias объект будет зашит его sql string, уже неважно какая там модель, он становится атомарным в каком-то смысле
        new_alias = models.Alias(
            alias = new_alias.alias,
            target = aliased_obj_sql_string,
            is_table=is_table,
            is_aggregate=is_aggreagte,
            owner_id=user.id
        )
        dbsession.add(new_alias)
        await dbsession.commit()
        await dbsession.refresh(new_alias)
        return JSONResponse(content={'msg':f'Alias for target was added successfully', 'obj':new_alias.model_dump()})
    except sqlexc.IntegrityError:
        raise exc.AliasAlreadyExists

        

@router.get('/help')
async def language(user: security.CurrentUserDependency):
    return {
        'about':'This is a JSON-ified portion of SQL language for Select queries specifically.',
        'operators_and_functions': {
            'arithmetic_operators': {
                'desc': 'Set of basic operators for arithmetic expressions',
                'content': list(get_args(sqllib.Operators)),
                'used_in': ['op'] 
            },
            'comparison_operators': {
                'desc': 'This set includes common comparison operators and a REGEXP operator',
                'content': list(get_args(sqllib.CompOperators)),
                'used_in': ['compare']
            },
            'functions': {
                'desc': 'A subset of non-aggregate SQL Functions.',
                'content': [x for x in sqllib.OperandFuncEnum],
                'used_in': ['op', 'func']
            },
            'aggregates': {
                'desc': 'A subset of aggregate SQL Functions.',
                'content': [x for x in sqllib.AggregatesEnum],
                'used_in': ['op', 'func']
            },
            'logical_binary': {
                'desc': 'AND, OR - binary logical operators, thus used only within a binary logical expression',
                'content': [x for x in sqllib.LogicalEnum],
                'used_in': ['and_or']
            },
            'logical_unary':{
                'desc': 'NOT operator - as a unary logical expression. Used automatically within a NOT expression',
                'content': ['NOT'],
                'used_in': ['not']
            }
        },
        'Expr_types': {
            'func': {
                'desc': 'A function call (aggregate and not). A sub-type of Expression',
                'args': {
                    'func':'A name of the function, like "AVG" or "MAX"',
                    'args': 'A list of arguments of any Expression subtype.'
                }   
            },
            'op': {
                'desc': 'A binary operation. A sub-type of Expression',
                'args': {
                    'left': 'Left operand of Expression subtype',
                    'operation': 'An arithmetic operator',
                    'right': 'Right operand of Expression subtype'
                }  
            },
            'table': {
                'desc': 'Represents a table. This type is used only for alias creation, in select clause aliases should be used instead (raw tables will not be accepted). A sub-type of Expression',
                'args': {
                    'table': 'A name of a table object in your database.'   
                }   
            },
            'col': {
                'desc': 'Represents a column in a table. This type can be used in select query BUT only in where/orderby/having/etc, it is not accepted in SELECT clause itself - aliases must be used instead. A sub-type of Expression',
                'args': {
                    'table': 'A name of a table object in a database.',
                    'column': 'A name of a column in the given table.'
                }   
            },
            'aliased': {
                'desc': 'Represents an Aliased expression - the core component of this model. A sub-type of Expression',
                'args': {
                    'alias_id': 'A name of an existing alias.'
                }  
            },
            'literal': {
                'desc': 'A literal expression that is basically an arbitrary int/str/float/bool value. A sub-type of Expression.',
                'args': {
                    'value': 'A value of a literal expression.'
                } 
            }
        },
        'booleanExpr_types': {
            'not': {
                'desc': 'Unary boolean expression NOT. A sub-type of BooleanExpr',
                'args': {
                    'operand': 'The one and only operand of NOT operator of a BooleanExpr sub-type.'
                } 
            },
            'and_or': {
                'desc': 'Unary boolean expression NOT. A sub-type of BooleanExpr',
                'args': {
                    'left': 'Left operand of BooleanExpr subtype',
                    'bool_op': 'A logical AND/OR operator',
                    'right': 'Right operand of BooleanExpr subtype'
                }
            },
            'compare': {
                'desc': 'A comparison operation - a bridge between regular Expressions and BooleanExpr types. A sub-type of BooleanExpr',
                'args': {
                    'left': 'Left operand of (!) Expression subtype',
                    'operation': 'A comparison operator',
                    'right': 'Right operand of (!) Expression subtype'
                }
            },

        },
        'clauses': {
            'select': {
                'columns': 'A list of column aliases (type "aliased") or a list ["*"] which represents all cols',
                'from_': 'A table alias',
                'join_clause': {
                    'table': 'A table alias',
                    'on_condition': 'A condition of sub-type BooleanExpr',
                    'type': [x for x in sqllib.JoinEnum]
                }
            },
            'where': {
                'expression': 'An expression of type BooleanExpr sub-type',
            },
            'having': {
                'expression': 'An expression of type BooleanExpr sub-type',
            },
            'orderby': {
                'items': [
                    {
                        'operand': 'An expression of type Expression',
                        'direction': [x for x in sqllib.DirectionEnum]
                    }
                ]
            },
            'groupby': {
                'items': 'A list of expressions of Expression sub-type'
            }
        }
        
    }