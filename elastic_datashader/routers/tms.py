from datetime import datetime, timezone
from os import getpid
from socket import gethostname
from typing import Optional

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Document
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, Response

from ..cache import (
    cache_entry_exists,
    cache_placeholder_exists,
    check_cache_dir,
    claim_cache_placeholder,
    get_cache,
    set_cache,
    release_cache_placeholder,
    rendering_tile_name,
    tile_id,
    tile_name,
)
from ..config import config
from ..drawing import generate_x_tile
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
    img = generate_x_tile(TILE_HEIGHT_PX, TILE_WIDTH_PX)

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

def temporary_tile_response() -> Response:
    img = generate_x_tile(TILE_HEIGHT_PX, TILE_WIDTH_PX, color=(128, 128, 128, 128))

    return Response(
        img,
        status_code=418,  # "I'm a teapot. This isn't the response you're looking for. Try again."
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "max-age=5",
            "Content-Type": "image/png",
            "Retry-After": "10",
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
        'timestamp': datetime.now(timezone.utc),
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
    # First check to see if the tile is still being rendered.
    if cache_placeholder_exists(config.cache_path, rendering_tile_name(idx, x, y, z, parameter_hash)):
        logger.debug(
            "Could not get tile from cache because it is still rendering: %s",
            rendering_tile_name(idx, x, y, z, parameter_hash)
        )
        return None

    # Try to get the image from the cache.
    img = get_cache(config.cache_path, tile_name(idx, x, y, z, parameter_hash))

    if img is not None:
        logger.info("Found tile in cache: %s", tile_name(idx, x, y, z, parameter_hash))

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

    logger.debug("Did not find image in cache: %s", tile_name(idx, x, y, z, parameter_hash))
    return None

def generate_tile_to_cache(idx: str, x: int, y: int, z: int, params, parameter_hash: str, request: Request) -> None:
    check_cache_dir(config.cache_path, idx)

    # Before any heavy lifting, double-check that the cache entry doesn't already exist.
    if cache_entry_exists(config.cache_path, tile_name(idx, x, y, z, parameter_hash)):
        logger.debug(
            "Not generating tile because it already exists in the cache: %s",
            tile_name(idx, x, y, z, parameter_hash)
        )
        return

    # Try to set a placeholder, which claims the rendering task.
    # If the placeholder already exists then another process already claimed the task.
    if not claim_cache_placeholder(config.cache_path, rendering_tile_name(idx, x, y, z, parameter_hash)):
        logger.debug(
            "Not generating tile because the cache placeholder could not be claimed: %s",
            rendering_tile_name(idx, x, y, z, parameter_hash)
        )
        return

    # Prepare rendering params.
    # If we fail, then make sure to remove the cache placeholder and unclaim the task.
    # Then bail and let another request have a shot at it.
    try:
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

    except Exception as ex:  # pylint: disable=W0703
        logger.error(
            "Failed to prepare tile rendering parameters for %s: %s",
            tile_name(idx, x, y, z, parameter_hash),
            str(ex)
        )
        logger.debug("Releasing cache placeholder %s", rendering_tile_name(idx, x, y, z, parameter_hash))
        release_cache_placeholder(config.cache_path, rendering_tile_name(idx, x, y, z, parameter_hash))
        raise

    # Render the tile image.
    # If we fail, then make sure to remove the cache placeholder and unclaim the task.
    # Then bail and let another request have a shot at it.
    try:
        render_time_start = datetime.now(timezone.utc)

        if params["render_mode"] in ("ellipses", "tracks"):
            img, metrics = generate_nonaggregated_tile(idx, x, y, z, request.headers, params)
        else:
            img, metrics = generate_tile(idx, x, y, z, request.headers, params)

    except Exception as ex:  # pylint: disable=W0703
        logger.error(
            "Failed to generate tile %s: %s",
            tile_name(idx, x, y, z, parameter_hash),
            str(ex)
        )
        error_info = {**base_tile_info, 'error': repr(ex)}
        create_datashader_tiles_entry(
            Elasticsearch(config.elastic_hosts.split(","), verify_certs=False, timeout=120),
            **error_info
        )
        logger.debug("Releasing cache placeholder %s", rendering_tile_name(idx, x, y, z, parameter_hash))
        release_cache_placeholder(config.cache_path, rendering_tile_name(idx, x, y, z, parameter_hash))
        raise

    # Add tile info to ElasticSearch.
    # If we fail, then make sure to remove the cache placeholder and unclaim the task.
    # Then bail and let another request have a shot at it.
    try:
        elapsed_time = (datetime.now(timezone.utc) - render_time_start).total_seconds()
        new_tile_info = {
            **base_tile_info,
            '_id': tile_id(idx, x, y, z, parameter_hash),
            'render_time': elapsed_time,
            'metrics': metrics,
            'cache_hits': 0,
        }

        create_datashader_tiles_entry(
            Elasticsearch(config.elastic_hosts.split(","), verify_certs=False, timeout=120),
            **new_tile_info,
        )

    except Exception as ex:  # pylint: disable=W0703
        logger.error(
            "Failed to add info to ES for tile %s: %s",
            tile_name(idx, x, y, z, parameter_hash),
            str(ex)
        )
        logger.debug("Releasing cache placeholder %s", rendering_tile_name(idx, x, y, z, parameter_hash))
        release_cache_placeholder(config.cache_path, rendering_tile_name(idx, x, y, z, parameter_hash))
        raise

    # Finally, write the rendered tile to the cache.
    # Regardless of the outcome, make sure to remove the cache placeholder and unclaim the task.
    try:
        set_cache(config.cache_path, tile_name(idx, x, y, z, parameter_hash), img)
    except Exception as ex:  # pylint: disable=W0703
        logger.error(
            "Failed to cache tile %s: %s",
            tile_name(idx, x, y, z, parameter_hash),
            str(ex)
        )
    finally:
        logger.debug("Releasing cache placeholder %s", rendering_tile_name(idx, x, y, z, parameter_hash))
        release_cache_placeholder(config.cache_path, rendering_tile_name(idx, x, y, z, parameter_hash))

@router.get("/{idx}/{z}/{x}/{y}.png")
async def get_tms(idx: str, x: int, y: int, z: int, request: Request, background_tasks: BackgroundTasks):
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

    # Try to use a cached response
    if (response := cached_response(es, idx, x, y, z, params, parameter_hash)) is not None:
        return response

    # Cache miss.
    # Generate the tile into the cache in the background.
    # In the meantime, return a temporary tile with a short browser-cache timeout and retry time,
    # so when the tile gets re-requested by the browser, it will hopefully be waiting in the cache.
    background_tasks.add_task(generate_tile_to_cache, idx, x, y, z, params, parameter_hash, request)
    return temporary_tile_response()
