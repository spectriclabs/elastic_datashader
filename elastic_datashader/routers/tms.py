from logging import getLogger

from fastapi import APIRouter, Header, HTTPException

from ..config import config

logger = getLogger(__name__)

router = APIRouter(
    prefix="/tms",
    tags=["tms"],
    responses={404: {"description": "Not found"}},
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
        'host': socket.gethostname(),
        'pid': os.getpid(),
        'timestamp': datetime.now(),
    }

    doc = Document(**doc_info)
    doc.save(using=es, index=".datashader_tiles")

def make_image_response(img, params, parameter_hash, cache_max_seconds) -> Response:
    resp = Response(img, status=200)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Datashader-Parameter-Hash"] = parameter_hash
    resp.headers["Datashader-RunAs-User"] = params.get("user", "")
    resp.cache_control.max_age = cache_max_seconds
    return resp

def cached_response(es, idx, x, y, z, params, parameter_hash) -> Optional[Response]:
    cache_path = Path(current_app.config["CACHE_DIRECTORY"])
    c = get_cache(cache_path, tile_name(idx, x, y, z, parameter_hash))

    if c is not None:
        current_app.logger.info("Hit cache (%s), returning", parameter_hash)
        img = c
        try:
            es.update(  # pylint: disable=E1123
                ".datashader_tiles",
                tile_id(idx, x, y, z, parameter_hash),
                body={"script" : {"source": "ctx._source.cache_hits++"}},
                retry_on_conflict=5,
            )
        except NotFoundError:
            current_app.logger.warn("Unable to find cached tile entry in .datashader_tiles")

        return make_image_response(img, params, parameter_hash, 60)

    return None

def generate_tile_response(es, idx, x, y, z, params, parameter_hash) -> Response:
    headers = get_es_headers(request_headers=request.headers, user=params["user"])
    current_app.logger.debug("Loaded input headers %s", request.headers)
    current_app.logger.debug("Loaded elasticsearch headers %s", headers)

    # Get or generate extended parameters
    params = merge_generated_parameters(params, idx, parameter_hash)

    base_tile_info = {
        'hash': parameter_hash,
        'idx': idx,
        'x': x,
        'y': y,
        'z': z,
        'url': request.url,
        'params': params,
    }

    render_time_start = datetime.now()

    try:
        if params["render_mode"] in ("ellipses", "tracks"):
            img, metrics = generate_nonaggregated_tile(idx, x, y, z, params)
        else:
            img, metrics = generate_tile(idx, x, y, z, params)
    except Exception as ex:  # pylint: disable=W0703
        logging.exception("Exception Generating Tile for request %s", request)
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
    cache_path = Path(current_app.config["CACHE_DIRECTORY"])
    check_cache_dir(cache_path, idx)
    set_cache(cache_path, tile_name(idx, x, y, z, parameter_hash), img)
    return make_image_response(img, params, parameter_hash, 60)

@router.get("/{idx}/{z}/{x}/{int:y}.png")
async def get_tms(
    idx: str,
    x: int,
    y: int,
    z: int,
    request: Request
):
    check_proxy_key(request.headers.get('tms-proxy-key'))

    es = Elasticsearch(
        config.elastic_hosts.split(","),
        verify_certs=False,
        timeout=120,
    )

    # Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request)
    except Exception as ex:  # pylint: disable=W0703
        current_app.logger.exception("Error while extracting parameters")
        params = {"user": request.headers.get("es-security-runas-user", None)}
        error_info = {
            'idx': idx,
            'x': x,
            'y': y,
            'z': z,
            'url': request.url,
            'params': params,
            'error': repr(ex)
        }

        create_datashader_tiles_entry(es, **error_info)
        return error_tile_response(ex)

    use_cache = request.args.get("force") is None

    # Try to use a cached response
    if use_cache and (response := cached_response(es, idx, x, y, z, params, parameter_hash)) is not None:
        return response

    # Cache miss, or cache disabled, so generate a tile
    return generate_tile_response(es, idx, x, y, z, params, parameter_hash)
