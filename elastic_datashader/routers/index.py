from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from ..cache import du, build_layer_info
from ..config import config

router = APIRouter()
templates = Jinja2Templates(directory="elastic_datashader/templates")

@router.get("/")
@router.get("/index")
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,  # required when using templates
            "title": "Elastic Datashader Server",
            "cache_size": du(config.cache_path),
            "layer_info": build_layer_info(config.cache_path),
        }
    )
