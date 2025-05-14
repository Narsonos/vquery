#Fastapi/Asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import asyncio
from contextlib import asynccontextmanager

#Project files
from app.config import Config
from app.routers import user, actions
from app.db import sessManagerObject, wait_for_db, init_db, redis_client
from app.utils.security import create_admin_on_startup_if_not_exists


#Misc
import datetime
import tzlocal # type: ignore
import os

#Logging
import logging
import logging.handlers
import loguru # type: ignore



###################
#     Logging     #
###################

logger = logging.getLogger('applogger')
logger.setLevel(logging.DEBUG)


stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(message)s'))
stream_handler.setLevel(logging.INFO)

debug_handler = logging.handlers.RotatingFileHandler(
    filename=os.path.join('app','logs','debug.log'),
    maxBytes=10*1024*1024,
    backupCount=3,
    encoding='utf-8'
    )
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))

logger.addHandler(stream_handler)
logger.addHandler(debug_handler)





###################
#       App       #
###################

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f'[APP: Startup] Startup began...')

    #Database
    await wait_for_db()    
    await init_db()
    await create_admin_on_startup_if_not_exists()
    #Redis
    app.state.redis = redis_client

    logger.info(f'[APP: Startup] Startup finished!')
    yield
    await app.state.redis.close() #exit redis
    if sessManagerObject._engine is not None:
        await sessManagerObject.close() #exit db
    
    


GIT_COMMIT = os.getenv("GIT_COMMIT")
MODE = os.getenv("MODE")

app = FastAPI(
    title = f'{Config.APP_NAME} commit {GIT_COMMIT if GIT_COMMIT else "None"}',
    keep_blank_values_in_query_string=True,
    swagger_ui_parameters={
        "defaultModelsExpandDepth": -1,
        "docExpansion": None,
        "displayRequestDuration":True
    },
    lifespan=lifespan, 
    root_path=f"/{Config.APP_NAME}"
)

app.include_router(user.router)
app.include_router(actions.router)




########################
#  Shutdowns & Health  #
########################


# система завершения работы
active_requests = 0
shutdown_event = asyncio.Event()

@app.get("/")
@app.get("/health",include_in_schema=False)
async def read_root():
    """Indicates if the server is alive"""
    
    if shutdown_event.is_set():
        return JSONResponse(status_code=500,content={})
    else:
        return

def handle_shutdown_signal():
    asyncio.ensure_future(initiate_shutdown())

async def initiate_shutdown():
    global shutdown_event
    shutdown_event.clear()
    shutdown_event.set()

    async def _wait_for_requests():
        while active_requests > 0:
            await asyncio.sleep(0.1)

    async def wait_for_requests_to_finish():
        try:
            await asyncio.wait_for(_wait_for_requests(), timeout=10*60)
        except asyncio.TimeoutError:
            print("Выключаемся")
        finally:
            os._exit(0)
    
    await wait_for_requests_to_finish()

import signal
signal.signal(signal.SIGINT, lambda sig, frame: handle_shutdown_signal())
signal.signal(signal.SIGTERM, lambda sig, frame: handle_shutdown_signal())
# система завершения работы

@app.middleware("http")
async def add_logging_middleware(request: Request, call_next):
    try:
        global active_requests
        active_requests += 1

        response = await call_next(request)

        return response
        
    except Exception as e:
        loguru.logger.exception(e)
        return JSONResponse(
            status_code=500,
            content={'successful':False,'detail':'Необработанная ошибка'}
        )
    finally:
        active_requests -= 1



@app.get("/check")
def check(request: Request):
    """Shows how the request is seen by the server + some time info"""

    tz = tzlocal.get_localzone()
    server_dt = datetime.datetime.now(tz)

    response = {
        "headers": dict(request.headers),
        "base_url": str(request.base_url),
        "hostname": str(request.client.host),
        "real_ip": request.headers.get('X-Real-IP',"X-Real-IP is missing"),
        "forwarded_for": request.headers.get('X-Forwarded-For', "X-Forwarded-For is missing"),
        "server_date": {
            "TZ":tz.key,
            "tnname": server_dt.strftime("%Z"),
            "h_tztime": server_dt.utcoffset().total_seconds() / 3600,
            "m_tztime": server_dt.utcoffset().total_seconds() / 60,
            "datetime": server_dt.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "m_timestamp": int(server_dt.timestamp() * 1000),
            "s_timestamp": int(server_dt.timestamp())
        }

    }
    return JSONResponse(content=response)
