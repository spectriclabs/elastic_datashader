from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from ..cache import age_off_cache, clear_hash_cache
from ..config import config

router = APIRouter()

@router.get("/clear_cache")
async def clear_cache(name: str, param_hash: Optional[str], request: Request):
    clear_hash_cache(config.cache_path, name, param_hash)
    return RedirectResponse(request.headers.get('Referer', '/'))

@router.get("/age_cache")
async def age_cache(name: str, age: int, request: Request):
    age_off_cache(config.cache_path, name, age)
    return RedirectResponse(request.headers.get('Referer', '/'))
