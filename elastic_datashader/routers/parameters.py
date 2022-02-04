from json import loads
from pprint import pformat

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..cache import du, build_layer_info
from ..config import config

router = APIRouter()
templates = Jinja2Templates(directory="elastic_datashader/templates")

@router.get("/parameters")
async def parameters(request: Request):
    name = request.query_params.get("name")
    hash_ = request.query_params.get("hash")

    context = {
        "request": request,
        "title": "Elastic Datashader Server",
        "name": name,
        "hash": hash_,
        "params": {},
    }

    if name is not None and hash_ is not None:
        params_json = config.cache_path / name / hash_ / "params.json"

        if params_json.exists():
            params = loads(params_json.read_text())
            context["params"] = params
            context["generated_params"] = pformat(params.get("generated_params", {}))
            return templates.TemplateResponse("parameters.html", context, status_code=200)

        raise HTTPException(status_code=404, detail=f"{name}/{hash_}/params.json does not exist")

    raise HTTPException(status_code=400, detail="Missing required name and hash query parameters")
