from shutil import rmtree

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from ..cache import age_off_cache
from ..config import config
from ..logger import logger

router = APIRouter()

@router.get("/clear_cache")
async def clear_cache(request: Request):
    name = request.query_params.get("name")
    hash_ = request.query_params.get("hash")

    # If no name is provided, we're done
    if name is None:
        raise HTTPException(detail=f"Unknown request: {name} / {hash_}", status_code=400)

    # delete a specific cache
    tile_cache_path = config.cache_path / name

    if hash_ is not None:
        tile_cache_path = tile_cache_path / hash_

    # Check if it exists
    if tile_cache_path.exists():
        rmtree(tile_cache_path)
        logger.info("Clearing hash/layer: %s", tile_cache_path)

    return RedirectResponse(request.headers.get('HTTP_REFERER'))

@router.get("/age_cache")
async def age_cache(request: Request):
    # Either the index name or age must be set.
    # We do not allow blanket deletes.
    age = request.query_params.get("age")
    name = request.query_params.get("name")
    hash_ = request.query_params.get("hash")

    # if no age is provided, we're done
    if age is None:
        raise HTTPException(detail=f"Unknown request: {name} / {hash_}", status_code=400)

    age_limit = int(age)
    age_off_cache(config.cache_path, age_limit)
    return RedirectResponse(request.headers.get('HTTP_REFERER'))
