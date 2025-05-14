from app.db import redis_client as redis

from functools import wraps
import inspect
import logging
import asyncio
import uuid
import time
import httpx

logger = logging.getLogger('applogger')


class RedisQueueError(Exception):
    """Base class for Redis Queue exception family"""
    pass 

class RedisQueueTimeoutError(RedisQueueError):
    """Raised when waiting for a free slot exceeds the timeout."""
    pass

class RedisQueueResourceIsAlreadyUsed(RedisQueueError):
    """Raised when User is already using this resource"""
    pass



def is_async(func):
    """Helper function to determine if the function is async"""

    if inspect.iscoroutinefunction(func):
        return True
    if inspect.isfunction(func):
        return False
    if isinstance(func, staticmethod):  
        func = func.__func__  
        return inspect.iscoroutinefunction(func)  
    return False

async def wait_for_slot(resource_queue_key: str, used_slots_key:str, lock_key:str, task: str, limit: int, timeout: float, check_interval: float = 1.0):
    """FIFO queue waiting - waits until a free slot for given resource has appeared.
    Arguments:
    - resource_queue_key:str - key to the queue for given resource
    - used_slots_key:str - key to the counter of used slots for given resource
    - lock_key:str - key to a redis lock
    - task:str - a UUID/ID of this task (to track our position in the queue)
    - limit:int number of slots
    - timeout:float - timeout in seconds, for how long we are going to wait at max before throwing an exception
    - check_interval:float - interval between checks (if a free slot appeared) in seconds
    """
    
    #another approach: using redis lists to maintian FIFO    
    async with redis.lock(lock_key, 30):
        await redis.rpush(resource_queue_key, task)

    try:
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time <= timeout:
            async with redis.lock(lock_key, 30):
                used_slots = int(await redis.get(used_slots_key) or 0)

            if used_slots < limit: #if free slot appeared
                async with redis.lock(lock_key, 30):
                    first = await redis.lindex(resource_queue_key, 0) #who's first in the queue?
                if first == task: #if that's us - exit queue
                    return
            await asyncio.sleep(check_interval) #else - wait

        #if timed out     
        async with redis.lock(lock_key, 30):
            await redis.lrem(resource_queue_key, 0, task)
        raise RedisQueueTimeoutError(f'Redis Queue timeout waiting for {resource_queue_key} for {(asyncio.get_event_loop().time() - start_time):.2f}s. Limit = {limit}')
    
    #if cancelled
    except asyncio.CancelledError:
        raise 
    finally:
        async with redis.lock(lock_key, 30):
            await redis.lrem(resource_queue_key, 0, task) 
        
def use_queue_for_resource(resource: str, limit: int, exec_timeout: int, timeout:float = 10.0, check_interval: float = 1.0):
    """Decorator factory that creates resource sepcific queue managers
    Arguments:
    - resource: str - arbitrary name of the resource (queue name)
    - limit: int - arbitrary N of slots the resource has
    - exec_timeout: int - time in seconds for which a slot can be occupied
    - timeout: float - time in seconds for which a task is waiting for a free slot
    - check_interval: float - time in seconds between checks for a free slot
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args,**kwargs):
            resource_queue_key = f'queue:{resource}:waiting'
            used_slots_key = f'queue:{resource}:active'
            lock_key = f'queue:{resource}:queue_lock'
            task = str(uuid.uuid4()) 

            async with redis.lock(lock_key, 30):
                qlen = await redis.llen(resource_queue_key)
                used = int(await redis.get(used_slots_key) or 0)

            logger.debug(f'[QUEUE] Resource {resource} is accessed. Waiting: {qlen}; Active: {used}. Func: {func.__name__}')
            
            await wait_for_slot(resource_queue_key, used_slots_key, lock_key, task, limit, timeout, check_interval) #This will raise Exception, so no need to check for limits.

            async with redis.lock(lock_key, 30):
                await redis.incr(used_slots_key) #Active users don't need a list, a counter must be enough
            
            try: 
                if is_async(func):
                    return await asyncio.wait_for(func(*args, **kwargs), timeout=exec_timeout)
                else:
                    return func(*args, **kwargs)
            except asyncio.CancelledError:
                logger.warning(f'[QUEUE] Task cancelled! Cleaning up slot {task[:8]}...{task[:-8]}.')
                raise  
            finally:
                async with redis.lock(lock_key, 30):
                    await redis.decr(used_slots_key) #Active users don't need a list, a counter must be enough
                    qlen = await redis.llen(resource_queue_key)
                    used = int(await redis.get(used_slots_key) or 0)
                logger.debug(f'[QUEUE] Resource with key {used_slots_key} is released. Waiting: {qlen}; Active: {used}. Func: {func.__name__}')

        return wrapper
    return decorator                
    
def lock_for_same_user(resource:str, user_id_kwarg_name:str):
    """Locks a resource with an arbitrary name <resource>. A user is identified by a value contained in a kwarg with a passed name.
    Example: A target function is def vote_in_a_poll(user_id:int, option:int) -> then user_id_kwarg is "user_id", i.e. it's value is going to be used to distinguish users.
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            user_id = kwargs.get(user_id_kwarg_name)
            user_specific_key = f'queue:{resource}:{user_id}'
            locked = await redis.get(user_specific_key)
            if not locked:
                logger.debug(f'[QUEUE] User {user_id} used {resource}')
                await redis.set(user_specific_key, 1)

                try:
                    if is_async(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args,**kwargs)
                finally:
                    logger.debug(f'[QUEUE] User {user_id} freed {resource}')
                    await redis.delete(user_specific_key)
            else:
                logger.debug(f'[QUEUE] User {user_id} tried to use {resource} twice!')
                raise RedisQueueResourceIsAlreadyUsed(f'Resource {resource} cannot be accessed twice by {user_id}')
        return wrapper
    return decorator

def rate_limit(resource:str, min_time_between_requests:float, ttl: float = 600):
    """Decorator factory: limits request rate resource-specifically"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            resource_rate_key = f'queue:{resource}:last_request'
            lock_key = f'queue:{resource}:rate_lock'
            

            async with redis.lock(lock_key, timeout=ttl):
                current_time = time.time()
                last_request_time = await redis.get(resource_rate_key)
                if last_request_time:
                    delta = current_time - float(last_request_time)
                    if delta < min_time_between_requests:
                        logger.debug(f'[DEBUG] Waiting for {min_time_between_requests - delta}')
                        await asyncio.sleep(min_time_between_requests - delta)
                await redis.set(resource_rate_key, time.time(), ex=ttl)
            
            if is_async(func):
                if last_request_time:
                    logger.debug(f'[DEBUG] Last at {last_request_time}, executed at {time.time()}, delta = {time.time() - float(last_request_time)}')
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        return wrapper
    return decorator

class RateLimitedTransport(httpx.AsyncBaseTransport):
    """Custom HTTPx Async transport that uses rate_limit"""

    def __init__(self, base: httpx.AsyncBaseTransport = None, resource:str = "default", min_time_between_requests: float = 1 , retries: int = 0):
        self.base_transport = base or httpx.AsyncHTTPTransport(retries=retries)
        self.resource = resource
        self.min_time_between_requests = min_time_between_requests

    
    async def handle_async_request(self, request):
        @rate_limit(self.resource, min_time_between_requests=self.min_time_between_requests)
        async def _handle(self, request):
            return await self.base_transport.handle_async_request(request)
        return await _handle(self, request)