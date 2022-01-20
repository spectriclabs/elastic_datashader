from datetime import datetime
from pathlib import Path
from typing import Optional

import copy
import json
import logging
import shutil
import os
import socket
import mercantile

from flask import Blueprint, current_app, request, redirect, Response

import pynumeral

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document
from elasticsearch.exceptions import NotFoundError

from elastic_datashader.helpers.cache import (
    check_cache_age,
    get_cache,
    check_cache_dir,
    set_cache,
    tile_id,
    tile_name,
)
from elastic_datashader.helpers.drawing import (
    create_color_key,
    gen_error,
)
from elastic_datashader.helpers.elastic import (
    get_search_base,
    get_es_headers,
    to_32bit_float,
    get_tile_categories
)
from elastic_datashader.helpers.parameters import (
    extract_parameters,
    merge_generated_parameters,
)
from elastic_datashader.helpers.tilegen import (
    TILE_HEIGHT_PX,
    TILE_WIDTH_PX,
    generate_nonaggregated_tile,
    generate_tile,
)

api_blueprints = Blueprint("rest_api", __name__)


@api_blueprints.route("/clear_cache", methods=["GET"])
def clear_cache():
    name = request.args.get("name")
    hash_ = request.args.get("hash")
    cache_dir = current_app.config.get("CACHE_DIRECTORY")

    # If no name is provided, we're done
    if name is None:
        return Response(f"Unknown request: {name} / {hash_}", status=500)

    # delete a specific cache
    tile_cache_path = Path(cache_dir) / name
    if hash_ is not None:
        tile_cache_path = tile_cache_path / hash_

    # Check if it exists
    if tile_cache_path.exists():
        shutil.rmtree(tile_cache_path)
        current_app.logger.info("Clearing hash/layer: %s", tile_cache_path)

    # Not needed with the hashing approach?
    # current_app.logger.warn("Recreating cache path %s", tile_cache_path)
    # tile_cache_path.mkdir(parents=True, exist_ok=True)

    return redirect(request.referrer)


@api_blueprints.route("/age_cache", methods=["GET"])
def age_cache():
    # Either the index name or age must be set.
    # We do not allow blanket deletes.
    age = request.args.get("age")
    name = request.args.get("name")
    hash_ = request.args.get("hash")

    # if no age is provided, we're done
    if age is None:
        return Response(f"Unknown request: {name} / {hash_}", status=500)

    age_limit = int(age)
    cache_dir = current_app.config["CACHE_DIRECTORY"]
    check_cache_age(cache_dir, age_limit)
    return redirect(request.referrer)


@api_blueprints.route("/<idx>/<field_name>/legend.json", methods=["GET"])
def provide_legend(idx, field_name):  # pylint: disable=W0613
    # Extract out special extent parameter that is independent from hash
    extent = None
    params = request.args.get("params")
    if params and params != "{params}":
        params = json.loads(params)
        extent = params.get("extent")
    
    zoom = params.get("zoom")
    if (zoom is None) and extent:
        zoom = mercantile.bounding_tile(
            max(-180.0, extent["minLon"]),
            max(-90.0, extent["minLat"]),
            max(180.0, extent["maxLon"]),
            max(90.0, extent["maxLat"]),
        ).z - 1
    elif (zoom is None) and not extent:
        return legend_response("[]", "no zoom")

    zoom = int(zoom)

    # Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request)
    except Exception as e:  # pylint: disable=W0703
        current_app.logger.exception("Error while extracting parameters")
        return legend_response("[]", e)

    params = merge_generated_parameters(params, idx, parameter_hash)

    # Assign param value to legacy keyword values
    geopoint_field = params["geopoint_field"]
    category_type = params["category_type"]
    category_histogram = params["category_histogram"]
    category_format = params["category_format"]
    cmap = params["cmap"]
    histogram_interval = params.get("generated_params", {}).get(
        "histogram_interval", None
    )
    field_min = params.get("generated_params", {}).get(
        "field_min", None
    )
    field_max = params.get("generated_params", {}).get(
        "field_max", None
    )

    # If not in category mode, just return nothing
    if params["category_field"] is None:
        return legend_response("[]", parameter_hash=parameter_hash, params=params)

    cmap = params["cmap"]
    category_field = params["category_field"]
    geopoint_field = params["geopoint_field"]

    base_s = get_search_base(current_app.config.get("ELASTIC"), params, idx)
    if extent:
        legend_bbox = {
            "top_left": {
                "lat": min(90.0, extent["maxLat"]),
                "lon": max(-180.0, extent["minLon"]),
            },
            "bottom_right": {
                "lat": max(-90.0, extent["minLat"]),
                "lon": min(180.0, extent["maxLon"]),
            },
        }
        current_app.logger.info("legend_bbox: %s", legend_bbox)
        base_s = base_s.filter("geo_bounding_box", **{geopoint_field: legend_bbox})
    legend_s = copy.copy(base_s)
    legend_s = legend_s.params(size=0)

    legend = {}
    if histogram_interval is not None and category_histogram in (True, None):
        # Put in the histogram search
        legend_s.aggs.bucket(
            "categories",
            "histogram",
            field=category_field,
            interval=histogram_interval,
            min_doc_count=1,
        )

        # Perform the execution
        response = legend_s.execute()
        # If no categories then return blank list
        if not hasattr(response.aggregations, "categories"):
            return legend_response("[]", parameter_hash=parameter_hash, params=params)

        # Generate the legend list
        for category in response.aggregations.categories:
            # Bin the data
            raw = float(category.key)
            # Format with pynumeral if provided
            if category_format:
                label = "%s-%s" % (
                    pynumeral.format(raw, category_format),
                    pynumeral.format(raw + histogram_interval, category_format),
                )
            else:
                label = "%s-%s" % (raw, raw + histogram_interval)
            legend[label] = category.doc_count
    elif category_field:
        tiles_iter = mercantile.tiles(
            max(-180.0, extent["minLon"]),
            max(-90.0, extent["minLat"]),
            min(180.0, extent["maxLon"]),
            min(90.0, extent["maxLat"]),
            zoom
        )

        for tile in tiles_iter:
            #Query the database to get the categories for this tile
            _, tile_legend = get_tile_categories(
                base_s,
                tile.x,
                tile.y,
                tile.z,
                geopoint_field,
                category_field,
                int(current_app.config["MAX_LEGEND_ITEMS_PER_TILE"]),
            )
            
            for k, v in tile_legend.items():
                if category_type == "number":
                    try:
                        k = pynumeral.format(to_32bit_float(k), category_format)
                    except ValueError:
                        k = str(k)                        
                else:
                    k = str(k)
                legend[k] = legend.get(k, 0) + v

    color_key_legend = []
    if not legend:
        return legend_response("[]", parameter_hash=parameter_hash, params=params)
    else:
        # Extract other to put it at the end
        other = legend.pop("Other", None)
        for k, count in sorted(legend.items(), key=lambda x: x[1], reverse=True):
            c = create_color_key([k], cmap=cmap, field_min=field_min, field_max=field_max, histogram_interval=histogram_interval).get(
                str(k), "#000000"
            )
            color_key_legend.append({"key": k, "color": c, "count": count})
        # Add Other to the end
        if other:
            k = "Other"
            count = other
            if not params.get("ellipses"):
                c = create_color_key([k], cmap=cmap, field_min=field_min, field_max=field_max, histogram_interval=histogram_interval).get(
                    str(k), "#000000"
                )
            else:
                # In ellipse mode everything gets it's own color
                # so there is never a color for Other
                c = None
            color_key_legend.append({"key": k, "color": c, "count": count})         

        return legend_response(json.dumps(color_key_legend), parameter_hash=parameter_hash, params=params)

@api_blueprints.route("/data/<idx>/<lat>/<lon>/<radius>", methods=["GET"])
def get_data(idx, lat, lon, radius):
    #Handle lat/lon conversion
    try:
        lat = float(lat)
        lon = float(lon)
        radius = float(radius)
        #Check for paging args
        from_arg = int(request.args.get("from", 0))
        size_arg = int(request.args.get("size", 100))
    except Exception:  # pylint: disable=W0703
        current_app.logger.exception("Error while converting lat/lon/radius/from/size")
        return error_data_response("Error while converting lat/lon/radius/from/size")

    #Handle includes list
    includes_list = request.args.get("includes", None)
    if includes_list:
        includes_list = includes_list.split(',')

    # Validate request is from proxy if proxy mode is enabled
    tms_key = current_app.config.get("TMS_KEY")
    tms_proxy_key = request.headers.get("TMS_PROXY_KEY")
    if tms_key is not None:
        if tms_key != tms_proxy_key:
            current_app.logger.warning(
                "TMS must be accessed via reverse proxy: keys %s != %s",
                tms_key,
                tms_proxy_key,
            )
            return Response("TMS must be accessed via reverse proxy", status=403)

    # Get hash and parameters
    try:
        _parameter_hash, params = extract_parameters(request)
    except Exception:  # pylint: disable=W0703
        current_app.logger.exception("Error while extracting parameters")
        return error_data_response("Error while extracting parameters")

    geopoint_field = params["geopoint_field"]

    #Build and execute search
    base_s = get_search_base(current_app.config.get("ELASTIC"), params, idx)
    distance_filter_dict = {"distance":"%sm"%radius, geopoint_field:{"lat":lat, "lon":lon}}
    base_s = base_s.filter("geo_distance", **distance_filter_dict)
    distance_sort_dict = {geopoint_field:{"lat":lat, "lon":lon}, "order":"asc", "ignore_unmapped":True}
    base_s = base_s.sort({"_geo_distance": distance_sort_dict})
    #Paginate
    base_s = base_s[from_arg:from_arg+size_arg]
    
    search_resp = base_s.execute()
    hits = []
    hit_count = 0
    for hit in search_resp:
        if includes_list:
            #Only include named fields
            named = {}
            for f in includes_list:
                named[f] = hit.to_dict().get(f, None)
            hits.append(named)
        else:
            hits.append(hit.to_dict())
        hit_count += 1

    #Generate response
    current_app.logger.info("Processed %s hits"%hit_count)
    resp = Response(json.dumps({"total_hits":search_resp.hits.total.value,
                     "from":from_arg,
                     "size":size_arg,
                     "hits":hits}), status=200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    return resp

def check_tms_key(headers) -> Optional[Response]:
    '''
    Validate request is from proxy if proxy mode is enabled
    '''
    tms_key = current_app.config.get("TMS_KEY")
    tms_proxy_key = headers.get("TMS_PROXY_KEY")

    if tms_key is not None and tms_key != tms_proxy_key:
        current_app.logger.warning(
            "TMS must be accessed via reverse proxy: keys %s != %s",
            tms_key,
            tms_proxy_key,
        )
        return Response("TMS must be accessed via reverse proxy", status=403)

    return None

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

@api_blueprints.route("/tms/<idx>/<int:z>/<int:x>/<int:y>.png", methods=["GET"])
def get_tms(idx, x: int, y: int, z: int) -> Response:
    if response := check_tms_key(request.headers) is not None:
        return response

    # TMS tile coordinates
    x = int(x)
    y = int(y)
    z = int(z)

    es = Elasticsearch(
        current_app.config.get("ELASTIC").split(","),
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

@api_blueprints.route("/indices", methods=["GET"])
def retrieve_indices():
    es = Elasticsearch(
        current_app.config.get("ELASTIC").split(","),
        verify_certs=False,
        timeout=120)
    indices = [idx for idx in sorted(es.indices.get_alias("*")) if not idx.startswith(".")]
    indices_json = json.dumps({"indices": indices})
    resp = Response(indices_json, status=200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    return resp

@api_blueprints.route("/indices/<index>/field_caps", methods=["GET"])
def retrieve_field_caps(index):
    es = Elasticsearch(
        current_app.config.get("ELASTIC").split(","), verify_certs=False, timeout=120
    )
    field_caps = es.field_caps(  # pylint: disable=E1123
        index,
        fields='*',
        ignore_unavailable=True,
    )

    response_json = json.dumps(field_caps)
    resp = Response(response_json, status=200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    return resp

@api_blueprints.route("/indices/<index>/mapping", methods=["GET"])
def retrieve_index_mapping(index):
    es = Elasticsearch(
        current_app.config.get("ELASTIC").split(","), verify_certs=False, timeout=120
    )
    index_mapping = es.indices.get_mapping(index)
    mapping = [
        {"name": field, "type": props["type"]}
        for field, props in index_mapping[index]["mappings"]["properties"].items()
    ]

    indices_json = json.dumps({"mapping": mapping})
    resp = Response(indices_json, status=200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    return resp


def legend_response(data: str, error: Optional[Exception] = None, **kwargs) -> Response:
    resp = Response(data, status=200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    if kwargs.get("parameter_hash"):
        resp.headers["Datashader-Parameter-Hash"] = kwargs["parameter_hash"]
    if kwargs.get("params"):
        resp.headers["Datashader-RunAs-User"] = kwargs["params"].get("user", "")
    resp.cache_control.max_age = 60
    if error is not None:
        resp.headers["Error"] = str(error)
    return resp


def error_tile_response(ex: Exception) -> Response:
    img = gen_error(TILE_HEIGHT_PX, TILE_WIDTH_PX)
    resp = Response(img, status=200)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Error"] = str(ex)
    resp.cache_control.max_age = 60
    return resp

def error_data_response(
    err: str
) -> Response:
    resp = Response(err, status=200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    return resp
