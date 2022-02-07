from fastapi import FastAPI

from .config import config
from .elastic import verify_datashader_indices
from .drawing import initialize_custom_color_maps
from .logger import logger
from .routers import cache, data, index, indices, legend, parameters, tms

logger.info("Loaded configuration %s", config)
verify_datashader_indices(config.elastic_hosts)
initialize_custom_color_maps()

app = FastAPI()
app.include_router(cache.router)
app.include_router(data.router)
app.include_router(index.router)
app.include_router(indices.router)
app.include_router(legend.router)
app.include_router(parameters.router)
app.include_router(tms.router)
