#!/usr/bin/env python3
import collections
import copy
import json
import logging
import os
import shutil
import subprocess
import time
from pprint import pformat

import pynumeral
from flask import Blueprint, current_app, render_template, request, redirect, Response

from tms_pixellock_api.helpers.cache import (
    check_cache_age,
    get_cache,
    check_cache_dir,
    set_cache,
)
from tms_pixellock_api.helpers.tilegen import generate_nonaggregated_tile, generate_tile
from tms_pixellock_api.helpers.drawing import create_color_key, gen_error
from tms_pixellock_api.helpers.timeutil import pretty_time_delta
from tms_pixellock_api.helpers.elastic import get_search_base, get_es_headers
from tms_pixellock_api.helpers.parameters import (
    extract_parameters,
    merge_generated_parameters,
)


blueprints = Blueprint("rest_api", __name__, template_folder="templates")


@blueprints.route("/")
@blueprints.route("/index")
def index():
    # Calc Cache Size
    cache_size = (
        subprocess.check_output(["du", "-sh", current_app.config["CACHE_DIRECTORY"]])
        .split()[0]
        .decode("utf-8")
    )
    # Build Layer Info
    layer_info = {}
    layers = os.listdir(current_app.config["CACHE_DIRECTORY"])
    for l in layers:
        if not os.path.isfile(current_app.config["CACHE_DIRECTORY"] + l):
            hashes = os.listdir(current_app.config["CACHE_DIRECTORY"] + l + "/")
            for h in hashes:
                if os.path.exists(
                    current_app.config["CACHE_DIRECTORY"] + l + "/" + h + "/params.json"
                ):
                    with open(
                        current_app.config["CACHE_DIRECTORY"]
                        + l
                        + "/"
                        + h
                        + "/params.json"
                    ) as f:
                        params = json.loads(f.read())
                    # Check age of hash
                    params["age_timestamp"] = os.path.getmtime(
                        current_app.config["CACHE_DIRECTORY"]
                        + l
                        + "/"
                        + h
                        + "/params.json"
                    )
                    params["age"] = pretty_time_delta(
                        time.time() - params["age_timestamp"]
                    )
                    # Check size of hash
                    try:
                        params["size"] = (
                            subprocess.check_output(
                                [
                                    "du",
                                    "-sh",
                                    current_app.config["CACHE_DIRECTORY"] + l + "/" + h,
                                ]
                            )
                            .split()[0]
                            .decode("utf-8")
                        )
                    except OSError:
                        params["size"] = "Error"
                    if layer_info.get(l) is None:
                        layer_info[l] = collections.OrderedDict()
                    layer_info[l][h] = params

            # Order hashes based off age, newest to oldest
            if layer_info.get(l):
                layer_info[l] = collections.OrderedDict(
                    reversed(
                        sorted(
                            layer_info[l].items(), key=lambda x: x[1]["age_timestamp"]
                        )
                    )
                )
    return render_template(
        "index.html",
        title="Elastic Data Shader Server",
        cache_size=cache_size,
        layer_info=layer_info,
    )


@blueprints.route("/parameters", methods=["GET"])
def display_parameters():
    color_file = os.path.join(
        current_app.config["CACHE_DIRECTORY"]
        + "/%s/%s-colormap.json" % (request.args.get("name"), request.args.get("field"))
    )

    # Build Layer Info
    layers = os.listdir(current_app.config["CACHE_DIRECTORY"])
    for l in layers:
        if l == request.args.get("name"):
            if not os.path.isfile(current_app.config["CACHE_DIRECTORY"] + l):
                hashes = os.listdir(current_app.config["CACHE_DIRECTORY"] + l + "/")
                for h in hashes:
                    if h == request.args.get("hash"):
                        if os.path.exists(
                            current_app.config["CACHE_DIRECTORY"]
                            + l
                            + "/"
                            + h
                            + "/params.json"
                        ):
                            with open(
                                current_app.config["CACHE_DIRECTORY"]
                                + l
                                + "/"
                                + h
                                + "/params.json"
                            ) as f:
                                params = json.loads(f.read())
                                generated_params = pformat(
                                    params.get("generated_params", {})
                                )
                                return render_template(
                                    "parameters.html",
                                    title="Elastic Data Shader Server",
                                    params=params,
                                    generated_params=generated_params,
                                    name=request.args.get("name"),
                                    hash=request.args.get("hash"),
                                )
    return render_template(
        "parameters.html",
        title="Elastic Data Shader Server",
        params={},
        name=request.args.get("name"),
        hash=request.args.get("hash"),
    )


@blueprints.route("/clear_cache", methods=["GET"])
def clear_cache():
    if request.args.get("name") is not None:
        # delete a specific cache
        tile_cache_path = os.path.join(
            current_app.config.get("CACHE_DIRECTORY"), request.args.get("name")
        )
        if request.args.get("hash") is not None:
            tile_cache_path = os.path.join(
                current_app.config.get("CACHE_DIRECTORY"),
                request.args.get("name"),
                request.args.get("hash"),
            )

        # Check if it exists
        if os.path.exists(tile_cache_path):
            shutil.rmtree(tile_cache_path)
            current_app.logger.info("Clearing hash/layer : %s", tile_cache_path)

        # Not needed with the hashing approach?
        # current_app.logger.warn("Recreating cache path %s", tile_cache_path)
        # pathlib.Path(os.path.join(tile_cache_path)).mkdir(parents=True, exist_ok=True)

        return redirect(request.referrer)
    return Response(
        "Unknown request: %s / %s"
        % (request.args.get("name"), request.args.get("hash")),
        status=500,
    )


@blueprints.route("/age_cache", methods=["GET"])
def age_cache():
    # Either the index name or age must be set.  We do not allow blanket deletes
    if request.args.get("age") is not None:
        age_limit = int(request.args.get("age"))
        cache_dir = current_app.config["CACHE_DIRECTORY"]
        check_cache_age(cache_dir, age_limit)
        return redirect(request.referrer)
    return Response(
        "Unknown request: %s / %s"
        % (request.args.get("name"), request.args.get("hash")),
        status=500,
    )


@blueprints.route("/<idx>/<field_name>/legend.json", methods=["GET"])
def provide_legend(idx, field_name):
    # Extract out special extent parameter that is independent from hash
    extent = None
    params = request.args.get("params")
    if params and params != "{params}":
        params = json.loads(request.args.get("params"))
        if params.get("extent"):
            extent = params.get("extent")

    # Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request)
    except Exception as e:
        current_app.logger.exception("Error while extracting parameters")
        resp = Response("[]", status=200)
        resp.headers["Content-Type"] = "application/json"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Error"] = str(e)
        resp.cache_control.max_age = 60
        return resp

    # If not in category mode, just return nothing
    if params["category_field"] is None:
        resp = Response("[]", status=200)
        resp.headers["Content-Type"] = "application/json"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.cache_control.max_age = 60
        return resp

    # Get or generate extended parameters
    paramsfile = os.path.join(
        current_app.config["CACHE_DIRECTORY"], idx, "%s/params.json" % parameter_hash
    )
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
        resp = Response("[]", status=200)
        resp.headers["Content-Type"] = "application/json"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.cache_control.max_age = 60
        return resp

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
        color_key_legend.append(dict(key=k, color=c, count=category.doc_count))

    other_cnt = getattr(response.aggregations.categories, "sum_other_doc_count", 0)
    if other_cnt > 0:
        c = create_color_key(["Other"], cmap=cmap).get("Other", "#000000")
        color_key_legend.append(dict(key="Other", count=other_cnt))

    data = json.dumps(color_key_legend)
    resp = Response(data, status=200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    return resp


@blueprints.route("/tms/<idx>/<int:z>/<int:x>/<int:y>.png", methods=["GET"])
def get_tms(idx, x, y, z):
    tile_height_px = 256
    tile_width_px = 256

    # Validate request is from proxy if proxy mode is enabled
    if current_app.config.get("TMS_KEY") is not None:
        if current_app.config.get("TMS_KEY") != request.headers.get("TMS_PROXY_KEY"):
            current_app.logger.warning(
                "TMS must be accessed via reverse proxy: keys %s != %s",
                current_app.config.get("TMS_KEY"),
                request.headers.get("TMS_PROXY_KEY"),
            )
            resp = Response("TMS must be accessed via reverse proxy", status=403)
            return resp

    # TMS tile coordinates
    x = int(x)
    y = int(y)
    z = int(z)

    # Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request)
    except Exception as e:
        current_app.logger.exception("Error while extracting parameters")
        img = gen_error(tile_height_px, tile_width_px)
        resp = Response(img, status=200)
        resp.headers["Content-Type"] = "image/png"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Error"] = str(e)
        resp.cache_control.max_age = 60
        return resp

    # Check if the cached image already exists
    c = get_cache(
        current_app.config["CACHE_DIRECTORY"],
        "/%s/%s/%s/%s/%s.png" % (idx, parameter_hash, z, x, y),
    )
    if c is not None and request.args.get("force") is None:
        current_app.logger.info("Hit cache (%s), returning" % parameter_hash)
        # Return Cached Value
        img = c
    else:
        # Generate a tile
        if request.args.get("force") is not None:
            current_app.logger.info(
                "Forced cache flush, generating a new tile %s/%s/%s" % (z, x, y)
            )
        else:
            current_app.logger.info(
                "No cache (%s), generating a new tile %s/%s/%s"
                % (parameter_hash, z, x, y)
            )

        check_cache_dir(current_app.config.get("CACHE_DIRECTORY"), idx)

        headers = get_es_headers(request.headers)
        current_app.logger.info("Loaded input headers %s", request.headers)
        current_app.logger.info("Loaded elasticsearch headers %s", headers)

        # Get or generate extended parameters
        paramsfile = os.path.join(
            current_app.config["CACHE_DIRECTORY"],
            idx,
            "%s/params.json" % parameter_hash,
        )
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
            img = gen_error(tile_width_px, tile_height_px)
            resp = Response(img, status=200)
            resp.headers["Content-Type"] = "image/png"
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Error"] = str(e.args)
            resp.cache_control.max_age = 60
            return resp

        # Store image as well
        set_cache(
            current_app.config["CACHE_DIRECTORY"],
            "/%s/%s/%s/%s/%s.png" % (idx, parameter_hash, z, x, y),
            img,
        )

    resp = Response(img, status=200)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.cache_control.max_age = 60
    return resp
