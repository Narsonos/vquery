from app.config import Config
import app.models.sql as sqllib
import app.models.common as models
import app.utils.security as security

from datetime import datetime,date
from fastapi import Depends
from app.db import DBDependency, RedisDependency
from typing import Any
import logging
from sqlalchemy import select
from sqlmodel import SQLModel
import app.utils.context as ctx

logger = logging.getLogger('applogger')




####################
# Common utilities #
####################

def json_serializer(obj):
    #add conversions for non-serializable stuff here
    if isinstance(obj,(datetime,date)):
        return obj.isoformat()
    raise TypeError(f"{type(obj)} is not serializable!")

async def convert_input_query(
    query: sqllib.SelectQueryInput,
    user: security.CurrentUserDependency,
    dbsession: DBDependency,
    redis: RedisDependency
) -> sqllib.SelectQuery:
    """
    From all alias_ids -> to real Alias Objects if they do exist
    """
    model_ctx = {
    }
    context = await ctx.set_request_context(user=user, dbsession=dbsession, redis=redis, model_context=model_ctx)
    rs: sqllib.SelectQuery = await sqllib.SelectQuery.from_input(inp=query)
    
    return rs

