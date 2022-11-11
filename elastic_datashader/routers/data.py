from json import dumps

from fastapi import APIRouter, HTTPException, Request, Response

from ..config import config
from ..elastic import get_search_base
from ..logger import logger
from ..parameters import extract_parameters

router = APIRouter(
    prefix="/data",
    tags=["data"],
    responses={404: {"description": "Not found"}},
)

def error_data_response(ex: Exception) -> Response:
    return Response(
        str(ex),
        status_code=200,
        headers={
            "Cache-Control": "max-age=60",
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Error": str(ex),
        }
    )

@router.get("/{idx}/{lat}/{lon}/{radius}")
async def get_data(idx: str, lat: float, lon: float, radius: float, request: Request):
    # Handle lat/lon conversion
    try:
        lat = float(lat)
        lon = float(lon)
        radius = float(radius)
        # Check for paging args
        from_arg = int(request.args.get("from", 0))
        size_arg = int(request.args.get("size", 100))

    except Exception:  # pylint: disable=W0703
        logger.exception("Error while converting lat/lon/radius/from/size")
        return error_data_response("Error while converting lat/lon/radius/from/size")

    # Handle includes list
    includes_list = request.query_params.get("includes", None)

    if includes_list:
        includes_list = includes_list.split(',')

    # Validate request is from proxy if proxy mode is enabled
    tms_key = config.tms_key
    tms_proxy_key = request.headers.get("TMS_PROXY_KEY")

    if tms_key is not None:
        if tms_key != tms_proxy_key:
            logger.warning(
                "TMS must be accessed via reverse proxy: keys %s != %s",
                tms_key,
                tms_proxy_key,
            )
            return HTTPException(status_code=403, detail="TMS must be accessed via reverse proxy")

    # Get hash and parameters
    try:
        _parameter_hash, params = extract_parameters(request.headers, request.query_params)
    except Exception as ex:  # pylint: disable=W0703
        logger.exception("Error while extracting parameters")
        return error_data_response(ex)

    geopoint_field = params["geopoint_field"]

    # Build and execute search
    base_s = get_search_base(config.elastic_hosts, request.headers, params, idx)
    distance_filter_dict = {"distance": f"{radius}m", geopoint_field: {"lat": lat, "lon": lon}}
    base_s = base_s.filter("geo_distance", **distance_filter_dict)
    distance_sort_dict = {geopoint_field: {"lat": lat, "lon": lon}, "order": "asc", "ignore_unmapped": True}
    base_s = base_s.sort({"_geo_distance": distance_sort_dict})
    # Paginate
    base_s = base_s[from_arg: from_arg+size_arg]

    search_resp = base_s.execute()
    hits = []
    hit_count = 0

    for hit in search_resp:
        if includes_list:
            # Only include named fields
            named = {}

            for f in includes_list:
                named[f] = hit.to_dict().get(f, None)

            hits.append(named)

        else:
            hits.append(hit.to_dict())
        hit_count += 1

    # Generate response
    logger.info("Processed %s hits", hit_count)
    return Response(
        dumps({
            "total_hits": search_resp.hits.total.value,
            "from": from_arg,
            "size": size_arg,
            "hits": hits,
        }),
        status_code=200,
        headers={
            "Cache-Control": "max-age=60",
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
    )
