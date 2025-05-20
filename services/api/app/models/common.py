from sqlmodel import Field, SQLModel, Relationship, String, UniqueConstraint, select
import re
from typing import Optional, Literal, List
from sqlmodel import Column, Text, JSON
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils import exceptions as exc


import logging
logger = logging.getLogger('applogger')

class User(SQLModel, table=True):
    __tablename__ = '__users__'
    id: int | None = Field(default=None, primary_key=True, description='Integer user identifier')
    username: str = Field(unique=True, min_length=3, max_length=32, description='A unique username used for logging in')
    password_hash : str = Field(description='A hashed password')
    description: str | None = Field(default=None, max_length=128, description='Some arbitrary metadata')
    role: Literal["user","admin"] = Field(default="user", sa_type=String(10), description='Role identifier')
    status: Literal["active","inactive"] = Field(default='active', sa_type=String(10), description='Turn on/off a user')
    queries: List["Query"] = Relationship(back_populates="owner", sa_relationship_kwargs={"lazy":"selectin"})
    aliases: List["Alias"] = Relationship(back_populates="owner", sa_relationship_kwargs={"lazy":"selectin"})

class UserCreationModel(SQLModel):
    username: str = Field(min_length=3, max_length=32, description='A unique username used for logging in')
    password: str = Field(min_length=8, max_length=32, description='User password')
    description: str | None = Field(default=None, max_length=128, description='Some arbitrary metadata')
    
class UserLoginModel(SQLModel):
    username: str = Field(min_length=3, max_length=32)
    password: str = Field(min_length=8, max_length=32)




class Token(SQLModel):
    token: str
    token_type: str

class TokenResponse(SQLModel):
    access_token: str
    refresh_token: str
    token_type: str






class Query(SQLModel, table=True):
    __tablename__ = '__queries__'
    query_id: int | None = Field(default=None, primary_key=True, description="An id of a query")
    owner_id: int = Field(foreign_key="__users__.id")
    name: str = Field(description="A name of the query to display")
    query_sql: str = Field(sa_column=Column(Text),description="A SelectQuery target stored as a SQL string")
    col_names: list[str] = Field(sa_column=Column(JSON))
    col_count: int
    owner: Optional["User"] = Relationship(back_populates='queries', sa_relationship_kwargs={"lazy":"selectin"})
    __table_args__ = (UniqueConstraint('name','owner_id', name="unique_name_per_user"),)

    class Config:
        validate_assignment=True

    @classmethod
    async def by_id(cls, query_id: int, owner_id: int, dbsession: AsyncSession):
        query = await dbsession.scalar(select(Query).where(Query.query_id == query_id and Query.owner_id == owner_id))
        logger.info(f'database returned that {query} as an query for id={query_id} ownerid={owner_id}')
        return query
    
    @classmethod
    async def by_name(cls, name: str, owner_id: int, dbsession: AsyncSession):
        query = await dbsession.scalar(select(Query).where(Query.name == name and Query.owner_id == owner_id))
        logger.info(f'database returned that {query} as an query for name={query} ownerid={owner_id}')
        return query
    
    @property
    def is_columnwise_scalar(self):
        return self.col_count == 1
    
    def scalarize(self) -> str:
        sql = self.query_sql.rstrip(";")
        if not self.is_columnwise_scalar:
            raise exc._ImpossiblToScalarize(f"Query returns multiple columns: col_count={self.col_count}")

        limit_match = re.search(r'LIMIT\s+(\d+)(\s+OFFSET\s+\d+)?$', sql, flags=re.IGNORECASE)
        if limit_match:
            limit_value = int(limit_match.group(1))
            if limit_value != 1:
                raise exc._ImpossiblToScalarize(f"Query contains clause LIMIT {limit_value} != 1")
            return sql  # already scalar
        else:
            return f"{sql} LIMIT 1"

    @classmethod
    async def fetch(cls:'Query', query_id: int | None, query_name: str | None, user, dbsession, redis) -> 'Query':
        if query_id:
            pointer_key = f"query:id:{query_id}"
            db_fetch_coro = Query.by_id(query_id=query_id, owner_id=user.id, dbsession=dbsession)
            error_str = f'query_id={query_id}'
        elif query_name:
            pointer_key = f"query:name:{user.id}:{query_name}"
            db_fetch_coro = Query.by_name(name=query_name, owner_id=user.id, dbsession=dbsession)
            error_str = f'query_name={query_name}'
        else:
            raise exc.IncorrectQueryFetchRequest

        #get pointer to actual query JSON
        redis_key = await redis.get(pointer_key)
        if redis_key:
            query_json = await redis.get(redis_key)
            if query_json:
                query = Query.model_validate_json(query_json)
                if query.owner_id == user.id:
                    logger.info(f'Found query in cache: {query}')
                    return query
                logger.warning('Query found in cache, but this user has no access to it')
                raise exc._QueryNotExists(f'No such query among your queries: {error_str}')

        #not in cache - fetch from DB
        query = await db_fetch_coro
        if query:
            #real key for storing query JSON
            real_key = f"query:{query.query_id}:{query.owner_id}:{query.name}"
            query_json = query.model_dump_json()
            await redis.set(real_key, query_json, ex=3600)

            #set both ID and name-based pointers
            await redis.set(f"query:id:{query.query_id}", real_key, ex=3600)
            await redis.set(f"query:name:{query.owner_id}:{query.name}", real_key, ex=3600)

            logger.info(f'Pulled query from DB and cached: {query}')
        else:
            raise exc._QueryNotExists(f'No such query among your queries: {error_str}')
        return query





class Alias(SQLModel, table=True):
    __tablename__ = '__aliases__'
    alias_id: int | None = Field(default=None, primary_key=True, description="An id of an alias")
    owner_id: int = Field(foreign_key="__users__.id")
    alias: str = Field(description="Alias string that can be used to represent a field/table in the onwer's context")
    target: str = Field(description="An field/table represented by an alias")
    is_table: bool = Field(default=False, description="If this target is a field name")
    is_aggregate: bool = Field(default=False, description="Shows if stored value is aggregate")
    owner: Optional["User"] = Relationship(back_populates='aliases', sa_relationship_kwargs={"lazy":"selectin"})
    __table_args__ = (UniqueConstraint('alias','owner_id', name="unique_name_per_user"),)

    class Config:
        validate_assignment=True

    def sql(self):
        return self.target #Таргет - это уже валидное SQL выражение, просто строка
    
    @classmethod
    async def by_id(cls, alias_id: int, owner_id: int, dbsession: AsyncSession):
        alias = await dbsession.scalar(select(Alias).where(Alias.alias_id == alias_id and Alias.owner_id == owner_id))
        logger.info(f'database returned that {alias} as an alias for id={alias_id} ownerid={owner_id}')
        return alias
    
    @classmethod
    async def by_name(cls, name: str, owner_id: int, dbsession: AsyncSession):
        alias = await dbsession.scalar(select(Alias).where(Alias.alias == name and Alias.owner_id == owner_id))
        logger.info(f'database returned that {alias} as an alias for name={alias} ownerid={owner_id}')
        return alias

    @classmethod
    async def fetch(cls, alias_id: int | None, alias_name: str | None, user, dbsession, redis) -> 'Alias':
        if alias_id:
            pointer_key = f"alias:id:{alias_id}"
            db_fetch_coro = cls.by_id(alias_id=alias_id, owner_id=user.id, dbsession=dbsession)
            error_str = f'alias_id={alias_id}'
        elif alias_name:
            pointer_key = f"alias:name:{user.id}:{alias_name}"
            db_fetch_coro = cls.by_name(name=alias_name, owner_id=user.id, dbsession=dbsession)
            error_str = f'alias_name={alias_name}'
        else:
            raise exc.IncorrectAliasFetchRequest

        #get pointer to actual alias JSON
        redis_key = await redis.get(pointer_key)
        if redis_key:
            alias_json = await redis.get(redis_key)
            if alias_json:
                alias = cls.model_validate_json(alias_json)
                if alias.owner_id == user.id:
                    logger.info(f'Found alias in cache: {alias}')
                    return alias
                logger.warning('Alias found in cache, but this user has no access to it')
                raise exc._AliasNotExists(f'No such alias among your aliases: {error_str}')

        #not in cache - fetch from DB
        alias = await db_fetch_coro
        if alias:
            #real key for storing alias JSON
            real_key = f"alias:{alias.alias_id}:{alias.owner_id}:{alias.alias}"
            alias_json = alias.model_dump_json()
            await redis.set(real_key, alias_json, ex=3600)

            #set both ID and name-based pointers
            await redis.set(f"alias:id:{alias.alias_id}", real_key, ex=3600)
            await redis.set(f"alias:name:{alias.owner_id}:{alias.alias}", real_key, ex=3600)

            logger.info(f'Pulled alias from DB and cached: {alias}')
        else:
            raise exc._AliasNotExists(f'No such alias among your aliases: {error_str}')
        return alias


