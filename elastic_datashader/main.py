from fastapi import FastAPI

import logging

from .config import config
from .elastic import verify_datashader_indices
from .drawing import initialize_custom_color_maps
from .routers import index, tms

logger = logging.getLogger(__name__)
logger.info("Loaded configuration %s", config)
logging.getLogger("elasticsearch").setLevel(logging.WARN)
logging.getLogger("numba").setLevel(logging.WARN)

verify_datashader_indices(config.elastic_hosts)
initialize_custom_color_maps()

app = FastAPI()
app.include_router(tms.router)
app.include_router(index.router)
