from asyncio import create_task

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import urllib3

from .cache import background_cache_cleanup
from .config import config
from .elastic import verify_datashader_indices
from .drawing import initialize_custom_color_maps
from .logger import logger
from .routers import cache, data, index, indices, legend, tms

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(UserWarning)

logger.info("Loaded configuration %s", config)
logger.setLevel(config.log_level)

if config.verify_indices:
    verify_datashader_indices(config.elastic_hosts)

initialize_custom_color_maps()

app = FastAPI()
app.include_router(cache.router)
app.include_router(data.router)
app.include_router(index.router)
app.include_router(indices.router)
app.include_router(legend.router)
app.include_router(tms.router)




origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def app_startup():
    create_task(background_cache_cleanup())
