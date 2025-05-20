import app.models.sql as sqllib
import app.utils.security as security

from datetime import datetime,date
from app.db import DBDependency, RedisDependency
import logging
import app.utils.context as ctx
import re

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


def parse_db_error(exc: Exception) -> tuple[str, str]:
    msg = str(exc)
    lower_msg = msg.lower()

    # Unknown column
    if "unknown column" in lower_msg:
        m = re.search(r"unknown column '([^']+)' in '([^']+)'", msg, re.IGNORECASE)
        if m:
            col, context = m.group(1), m.group(2)
            user_msg = f"Ошибка: Неизвестный столбец '{col}' в контексте '{context}'."
            return user_msg, msg

    # Syntax error
    if "syntax error" in lower_msg:
        m = re.search(r"near '([^']+)'", msg, re.IGNORECASE)
        near = m.group(1) if m else "<не удалось извлечь из текста ошибки>"
        user_msg = f"Ошибка синтаксиса SQL около '{near}'."
        return user_msg, msg

    # Unknown table
    if "unknown table" in lower_msg:
        m = re.search(r"unknown table '([^']+)'", msg, re.IGNORECASE)
        table = m.group(1) if m else "неизвестная таблица"
        user_msg = f"Ошибка: Неизвестная таблица '{table}'."
        return user_msg, msg

    # Duplicate column name
    if "duplicate column name" in lower_msg:
        m = re.search(r"duplicate column name '([^']+)'", msg, re.IGNORECASE)
        col = m.group(1) if m else "неизвестная колонка"
        user_msg = f"Ошибка: Повторяющееся имя колонки '{col}'."
        return user_msg, msg

    # Subquery returns more than one row
    if "subquery returns more than 1 row" in lower_msg or "more than one row returned" in lower_msg:
        user_msg = "Ошибка: Подзапрос вернул более одной строки, ожидается одно значение."
        return user_msg, msg

    # Default fallback
    user_msg = "Ошибка выполнения запроса MySQL."
    return user_msg, msg