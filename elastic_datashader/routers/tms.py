from datetime import datetime
from os import getpid
from socket import gethostname
from typing import Optional

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document
from elasticsearch.exceptions import NotFoundError
from fastapi import APIRouter, HTTPException, Request, Response

from ..cache import (
    check_cache_dir,
    get_cache,
    set_cache,
    tile_id,
    tile_name,
)
from ..config import config
from ..drawing import gen_error
from ..elastic import get_es_headers
from ..logger import logger
from ..parameters import extract_parameters, merge_generated_parameters
from ..tilegen import (
    TILE_HEIGHT_PX,
    TILE_WIDTH_PX,
    generate_nonaggregated_tile,
    generate_tile,
)

router = APIRouter(
    prefix="/tms",
    tags=["tms"],
    responses={404: {"description": "Not found"}},
)

def error_tile_response(ex: Exception) -> Response:
    img = gen_error(TILE_HEIGHT_PX, TILE_WIDTH_PX)

    return Response(
        img,
        status_code=200,
        headers={
            "Cache-Control": "max-age=60",
            "Content-Type": "image/png",
            "Access-Control-Allow-Origin": "*",
            "Error": str(ex),
        }
    )

def check_proxy_key(tms_proxy_key: Optional[str]) -> None:
    '''
    Validate request is from proxy if proxy mode is enabled
    '''
    tms_key = config.tms_key

    if tms_key is not None and tms_key != tms_proxy_key:
        logger.warning(
            "TMS must be accessed via reverse proxy: keys %s != %s",
            tms_key,
            tms_proxy_key,
        )
        raise HTTPException(status_code=403, detail="TMS must be accessed via reverse proxy")

def create_datashader_tiles_entry(es, **kwargs) -> None:
    '''
    Create an entry in .datashader_tiles
    '''
    doc_info = {
         **kwargs,
        'host': gethostname(),
        'pid': getpid(),
        'timestamp': datetime.now(),
    }

    doc = Document(**doc_info)
    doc.save(using=es, index=".datashader_tiles")

def make_image_response(img: bytes, user: str, parameter_hash: str, cache_max_seconds: int) -> Response:
    return Response(
        img,
        status_code=200,
        headers={
            "Cache-Control": f"max-age={cache_max_seconds}",
            "Content-Type": "image/png",
            "Access-Control-Allow-Origin": "*",
            "Datashader-Parameter-Hash": parameter_hash,
            "Datashader-RunAs-User": user,
         }
    )

def cached_response(es, idx, x, y, z, params, parameter_hash) -> Optional[Response]:
    img = get_cache(config.cache_path, tile_name(idx, x, y, z, parameter_hash))

    if img is not None:
        logger.info("Hit cache (%s), returning", parameter_hash)

        try:
            es.update(  # pylint: disable=E1123
                ".datashader_tiles",
                tile_id(idx, x, y, z, parameter_hash),
                body={"script" : {"source": "ctx._source.cache_hits++"}},
                retry_on_conflict=5,
            )
        except NotFoundError:
            logger.warning("Unable to find cached tile entry in .datashader_tiles")

        return make_image_response(img, params.get("user") or "", parameter_hash, 60)

    return None

def generate_tile_response(es, idx, x, y, z, params, parameter_hash, request: Request) -> Response:
    headers = get_es_headers(request_headers=request.headers, user=params["user"])
    logger.debug("Loaded input headers %s", request.headers)
    logger.debug("Loaded elasticsearch headers %s", headers)

    # Get or generate extended parameters
    params = merge_generated_parameters(request.headers, params, idx, parameter_hash)

    base_tile_info = {
        'hash': parameter_hash,
        'idx': idx,
        'x': x,
        'y': y,
        'z': z,
        'url': str(request.url),
        'params': params,
    }

    render_time_start = datetime.now()

    try:
        if params["render_mode"] in ("ellipses", "tracks"):
            img, metrics = generate_nonaggregated_tile(idx, x, y, z, request.headers, params)
        else:
            img, metrics = generate_tile(idx, x, y, z, request.headers, params)

    except Exception as ex:  # pylint: disable=W0703
        logger.exception("Exception Generating Tile for request %s", request)
        error_info = {**base_tile_info, 'error': repr(ex)}
        create_datashader_tiles_entry(es, **error_info)

        # generate an error tile/don't cache cache it
        return error_tile_response(ex)

    elapsed_time = (datetime.now() - render_time_start).total_seconds()
    new_tile_info = {
        **base_tile_info,
        '_id': tile_id(idx, x, y, z, parameter_hash),
        'render_time': elapsed_time,
        'metrics': metrics,
        'cache_hits': 0,
    }

    create_datashader_tiles_entry(es, **new_tile_info)

    # Store image as well
    check_cache_dir(config.cache_path, idx)
    set_cache(config.cache_path, tile_name(idx, x, y, z, parameter_hash), img)
    return make_image_response(img, params.get("user") or "", parameter_hash, 60)

@router.get("/{idx}/{z}/{x}/{y}.png")
async def get_tms(idx: str, x: int, y: int, z: int, request: Request):
    check_proxy_key(request.headers.get('tms-proxy-key'))

    es = Elasticsearch(
        config.elastic_hosts.split(","),
        verify_certs=False,
        timeout=120,
    )

    # Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request.headers, request.query_params)
    except Exception as ex:  # pylint: disable=W0703
        logger.exception("Error while extracting parameters")
        params = {"user": request.headers.get("es-security-runas-user", None)}
        error_info = {
            'idx': idx,
            'x': x,
            'y': y,
            'z': z,
            'url': str(request.url),
            'params': params,
            'error': repr(ex)
        }

        create_datashader_tiles_entry(es, **error_info)
        return error_tile_response(ex)

    cache_enabled = request.query_params.get("force") is None

    # Try to use a cached response if the cache is enabled
    if cache_enabled and (response := cached_response(es, idx, x, y, z, params, parameter_hash)) is not None:
        return response

    # Cache miss, or cache disabled, so generate a tile
    return generate_tile_response(es, idx, x, y, z, params, parameter_hash, request)
