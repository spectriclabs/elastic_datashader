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
