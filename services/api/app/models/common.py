from sqlmodel import Field, SQLModel, Relationship, String, UniqueConstraint, select

from typing import Optional, Literal, List
from enum import Enum
from sqlmodel import Column, Text
from sqlalchemy.ext.asyncio import AsyncSession

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
    
    
    owner: Optional["User"] = Relationship(back_populates='queries', sa_relationship_kwargs={"lazy":"selectin"})
    __table_args__ = (UniqueConstraint('name','owner_id', name="unique_name_per_user"),)

    class Config:
        validate_assignment=True

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
    



