import uvicorn
import logging
from app.config import Config
    
class ExcludeAioHttpFilter(logging.Filter):
    def filter(self, record):
        return not record.getMessage().startswith("Unclosed connection")

asyncio_access_logger = logging.getLogger("asyncio")
asyncio_access_logger.addFilter(ExcludeAioHttpFilter())

if __name__ == '__main__':
    uvicorn.run(
        app = "main:app",
        host = Config.UVICORN_HOST,
        port = Config.UVICORN_PORT,
        log_level = 'info',
        use_colors = True,
        proxy_headers = True,
        http = 'httptools',
        lifespan = 'on',
        forwarded_allow_ips="*"
    )