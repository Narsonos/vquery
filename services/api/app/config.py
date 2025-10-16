import os
import ssl

#For PostgreSQL
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


class Config():
    #Basic app settings
    APP_NAME = 'vquery' #Is gonna match the app root 
    SECRET = os.getenv("SECRET") #Main secret for all protected requests
    UVICORN_PORT = 8000
    UVICORN_HOST = '0.0.0.0'
    BASE_URL_LOCAL = f"http://{UVICORN_HOST}:{UVICORN_PORT}" #Utilized by telegram proxy mechanisms

    #Security settings
    DEFAULT_ADMIN_USERNAME = 'admin'
    DEFAULT_ADMIN_PASSWORD = os.getenv("DEFAULT_ADMIN_PASSWORD")
    JWT_SECRET = os.getenv("JWT_SECRET")
    REFRESH_SECRET = os.getenv("REFRESH_SECRET")
    ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 240
    REFRESH_TOKEN_EXPIRE_DAYS = 7
    LOCK_TIME = 60 #TTL for locks. I.e. in 60s the lock is considered as deadlock => gets auto-unlocked.

    #Redis
    REDIS_PASS = os.getenv("REDIS_PASS")

    #MySQL Template
    DB_USER = os.getenv("MYSQL_USER")
    DB_PASS = os.getenv("MYSQL_PASSWORD")
    DB_NAME = os.getenv("MYSQL_DATABASE")
    DB_HOST = 'db'
    DB_PORT = 3306
    DB_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    #PostgreSQL Template
    #DB_USER = os.getenv("POSTGRES_USER")
    #DB_PASS = os.getenv("POSTGRES_PASSWORD")
    #DB_NAME = os.getenv("POSTGRES_DB")
    #DB_HOST = 'db'
    #DB_PORT = 5432
    #DB_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    DB_KWARGS = {
        'echo': False,
    }

    #Better not to change. If you are willing to change this, you'll have to change names of models in models.system and all the imports of these classes
    #This parameter controls only checks, the table creation depends on ORM model names, thus remains unchanged... 
    SYSTEM_TABLE_PREFIX = "__"

    #Short format for table names -> up to 50 bytes per name and rest per index 
    USE_SHORTER_NAMES = True


