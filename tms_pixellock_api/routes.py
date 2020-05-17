#!/usr/bin/env python3
import copy
import json
import logging
import shutil
import subprocess
from pathlib import Path
from pprint import pformat

import pynumeral
from flask import Blueprint, current_app, render_template, request, redirect, Response

from tms_pixellock_api.helpers.cache import (
    check_cache_age,
    get_cache,
    check_cache_dir,
    set_cache,
    build_layer_info,
)
from tms_pixellock_api.helpers.drawing import create_color_key, gen_error
from tms_pixellock_api.helpers.elastic import get_search_base, get_es_headers
from tms_pixellock_api.helpers.parameters import (
    extract_parameters,
    merge_generated_parameters,
)
from tms_pixellock_api.helpers.tilegen import generate_nonaggregated_tile, generate_tile

blueprints = Blueprint("rest_api", __name__, template_folder="templates")


##############################
# VIEW ENDPOINTS
##############################


@blueprints.route("/")
@blueprints.route("/index")
def index():
    cache_dir = current_app.config["CACHE_DIRECTORY"]

    # Calc Cache Size
    cache_size = (
        subprocess.check_output(["du", "-sh", cache_dir]).split()[0].decode("utf-8")
    )

    # Build Layer Info
    return render_template(
        "index.html",
        title="Elastic Data Shader Server",
        cache_size=cache_size,
        layer_info=build_layer_info(cache_dir),
    )


@blueprints.route("/parameters", methods=["GET"])
def display_parameters():
    cache_dir = Path(current_app.config["CACHE_DIRECTORY"])
    name = request.args.get("name")
    hash_ = request.args.get("hash")

    template_kwargs = {
        "title": "Elastic Data Shader Server",
        "name": name,
        "hash": hash_,
        "params": {},
    }

    params_json = cache_dir / name / hash_ / "params.json"
    if params_json.exists():
        with params_json.open("r") as f:
            params = json.load(f)
        template_kwargs.update(
            {
                "params": params,
                "generated_params": pformat(params.get("generated_params", {})),
            }
        )

    return render_template("parameters.html", **template_kwargs)


##############################
# API ENDPOINTS
##############################


@blueprints.route("/clear_cache", methods=["GET"])
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


@blueprints.route("/age_cache", methods=["GET"])
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


@blueprints.route("/<idx>/<field_name>/legend.json", methods=["GET"])
def provide_legend(idx, field_name):
    # Extract out special extent parameter that is independent from hash
    extent = None
    params = request.args.get("params")
    if params and params != "{params}":
        params = json.loads(params)
        extent = params.get("extent")

    # Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request)
    except Exception as e:
        current_app.logger.exception("Error while extracting parameters")
        return legend_response("[]", e)

    # If not in category mode, just return nothing
    if params["category_field"] is None:
        return legend_response("[]")

    # Get or generate extended parameters
    cache_dir = Path(current_app.config["CACHE_DIRECTORY"])
    paramsfile = cache_dir / f"{idx}/{parameter_hash}/params.json"
    params = merge_generated_parameters(params, paramsfile, idx)

    # Assign param value to legacy keyword values
    geopoint_field = params["geopoint_field"]
    category_type = params["category_type"]
    category_histogram = params["category_histogram"]
    category_format = params["category_format"]
    cmap = params["cmap"]
    histogram_interval = params.get("generated_params", {}).get(
        "histogram_interval", None
    )

    # Get search object
    base_s = get_search_base(current_app.config["ELASTIC"], params, idx)
    legend_s = copy.copy(base_s)
    legend_s = legend_s.params(size=0)

    # if an extent was provided use it for the filter
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
        legend_s = legend_s.filter("geo_bounding_box", **{geopoint_field: legend_bbox})

    max_legend_categories = 50
    if histogram_interval is not None and category_histogram in (True, None):
        # Put in the histogram search
        legend_s.aggs.bucket(
            "categories",
            "histogram",
            field=field_name,
            interval=histogram_interval,
            min_doc_count=1,
        )
    else:
        # Non-histogram legend
        legend_s.aggs.bucket(
            "categories", "terms", field=field_name, size=max_legend_categories
        )
    # Perform the execution
    response = legend_s.execute()
    # If no categories then return blank list
    if not hasattr(response.aggregations, "categories"):
        return legend_response("[]")

    # Generate the legend list
    color_key_legend = []
    for category in response.aggregations.categories:
        if (
            histogram_interval
            and category_type == "number"
            and category_histogram in (True, None)
        ):
            # Bin the data
            raw = float(category.key)
            # Format with pynumeral if provided
            if category_format:
                k = "%s-%s" % (
                    pynumeral.format(raw, category_format),
                    pynumeral.format(raw + histogram_interval, category_format),
                )
            else:
                k = "%s-%s" % (raw, raw + histogram_interval)
        else:
            k = str(category.key)
        c = create_color_key([str(category.key)], cmap=cmap).get(
            str(category.key), "#000000"
        )
        color_key_legend.append({"key": k, "color": c, "count": category.doc_count})

    other_cnt = getattr(response.aggregations.categories, "sum_other_doc_count", 0)
    if other_cnt > 0:
        c = create_color_key(["Other"], cmap=cmap).get("Other", "#000000")
        color_key_legend.append({"key": "Other", "count": other_cnt})

    return legend_response(json.dumps(color_key_legend))


def legend_response(data, error=None) -> Response:
    resp = Response(data, status=200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    if error is not None:
        resp.headers["Error"] = str(error)
    return resp


@blueprints.route("/tms/<idx>/<int:z>/<int:x>/<int:y>.png", methods=["GET"])
def get_tms(idx, x: int, y: int, z: int):
    tile_height_px = 256
    tile_width_px = 256

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

    # TMS tile coordinates
    x = int(x)
    y = int(y)
    z = int(z)

    # Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request)
    except Exception as e:
        current_app.logger.exception("Error while extracting parameters")
        return error_tile_response(e, tile_height_px, tile_width_px)

    cache_dir = Path(current_app.config["CACHE_DIRECTORY"])
    tile_name = f"{idx}/{parameter_hash}/{z}/{x}/{y}.png"
    force = request.args.get("force")

    # Check if the cached image already exists
    c = get_cache(cache_dir, tile_name)
    if c is not None and force is None:
        current_app.logger.info("Hit cache (%s), returning", parameter_hash)
        # Return Cached Value
        img = c
    else:
        # Generate a tile
        if force is not None:
            current_app.logger.info(
                "Forced cache flush, generating a new tile %s/%s/%s", z, x, y
            )
        else:
            current_app.logger.info(
                "No cache (%s), generating a new tile %s/%s/%s", parameter_hash, z, x, y
            )

        check_cache_dir(cache_dir, idx)

        headers = get_es_headers(request.headers)
        current_app.logger.info("Loaded input headers %s", request.headers)
        current_app.logger.info("Loaded elasticsearch headers %s", headers)

        # Get or generate extended parameters
        paramsfile = cache_dir / idx / f"{parameter_hash}/params.json"
        params = merge_generated_parameters(params, paramsfile, idx)

        # Separate call for ellipse
        try:
            if params["ellipses"]:
                img = generate_nonaggregated_tile(idx, x, y, z, params)
            else:
                img = generate_tile(idx, x, y, z, params)
        except Exception as e:
            logging.exception("Exception Generating Tile for request %s", request)
            # generate an error tile/don't cache cache it
            return error_tile_response(e, tile_height_px, tile_width_px)

        # Store image as well
        set_cache(cache_dir, tile_name, img)

    resp = Response(img, status=200)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    return resp


def error_tile_response(
    e: Exception, tile_height_px: int, tile_width_px: int
) -> Response:
    img = gen_error(tile_height_px, tile_width_px)
    resp = Response(img, status=200)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Error"] = str(e)
    resp.cache_control.max_age = 60
    return resp
