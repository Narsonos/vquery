from contextvars import ContextVar
from db import RedisDependency, DBDependency
import app.utils.security as security
import logging

logger = logging.getLogger('applogger')

current_user = ContextVar("current_user")
current_dbsession = ContextVar("current_dbsession")
current_redis = ContextVar("current_redis")
current_model_context = ContextVar('model_context')


async def set_request_context(
    user=security.CurrentUserDependency,
    dbsession=DBDependency,
    redis=RedisDependency,
    model_context = {}
):
    logger.info('Context set!')
    current_user.set(user)
    current_dbsession.set(dbsession)
    current_redis.set(redis)
    current_model_context.set(model_context)
    