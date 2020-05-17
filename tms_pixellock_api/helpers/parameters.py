#!/usr/bin/env python3
import copy
import fcntl
import hashlib
import json
import math
import pathlib
from datetime import datetime

from flask import current_app, Response

from tms_pixellock_api.helpers.timeutil import quantize_time_range, convert_kibana_time
from tms_pixellock_api.helpers.elastic import get_search_base, build_dsl_filter


def extract_parameters(request):
    """Get the parameters from a request and return hash and dict of parameters

    :param request:
    """

    # Default values
    from_time = None
    to_time = "now"
    params = {
        "user": request.headers.get("es-security-runas-user", None),
        "geopoint_field": None,
        "timestamp_field": "@timestamp",
        "lucene_query": None,
        "dsl_query": None,
        "dsl_filter": None,
        "cmap": None,
        "category_field": None,
        "category_type": None,
        "category_format": None,
        "category_histogram": None,
        "ellipses": False,
        "ellipse_major": "",
        "ellipse_minor": "",
        "ellipse_tilt": "",
        "ellipse_units": "",
        "ellipse_max_cep": 50,
        "spread": None,
        "span_range": None,
        "resolution": "finest",
        "max_bins": int(current_app.config["MAX_BINS"]),
        "max_batch": int(current_app.config["MAX_BATCH"]),
        "max_ellipses_per_tile": int(current_app.config["MAX_ELLIPSES_PER_TILE"]),
    }

    # Extract user from headers

    # Argument Parameters
    arg_params = request.args.get("params")
    if arg_params and arg_params != "{params}":
        arg_params = json.loads(request.args.get("params"))
        if arg_params.get("timeFilters", {}).get("from"):
            from_time = arg_params.get("timeFilters", {}).get("from")
        if arg_params.get("timeFilters", {}).get("to"):
            to_time = arg_params.get("timeFilters", {}).get("to")
        if arg_params.get("filters"):
            params["dsl_filter"] = build_dsl_filter(arg_params.get("filters"))
        if arg_params.get("query") and arg_params.get("query", {}).get(
            "language", None
        ) in ("lucene", "kuery"):
            # accept 'kuery' for backwords compatibility...
            params["lucene_query"] = arg_params.get("query").get("query")
        elif (
            arg_params.get("query")
            and arg_params.get("query", {}).get("language", None) == "dsl"
        ):
            params["dsl_query"] = arg_params.get("query").get("query")
    elif arg_params and arg_params == "{params}":
        # If the parameters haven't been provided yet
        resp = Response("TMS parameters not yet provided", status=204)
        return resp

    # Custom parameters can be provided by the URL
    params["ellipses"] = request.args.get("ellipses", default=params["ellipses"])
    if params["ellipses"] == "false" or params["ellipses"] == "False":
        params["ellipses"] = False
    else:
        # Handle the other fields
        params["ellipse_major"] = request.args.get("ellipse_major", default="")
        params["ellipse_minor"] = request.args.get("ellipse_minor", default="")
        params["ellipse_tilt"] = request.args.get("ellipse_tilt", default="")
        params["ellipse_units"] = request.args.get("ellipse_units", default="")
        if (
            params["ellipse_major"] == ""
            or params["ellipse_major"] == ""
            or params["ellipse_major"] == ""
        ):
            params["ellipses"] = False
    if request.args.get("ellipse_search", default="") == "narrow":
        params["ellipse_max_cep"] = 1.0
    elif request.args.get("ellipse_search", default="") == "normal":
        params["ellipse_max_cep"] = 10.0
    elif request.args.get("ellipse_search", default="") == "wide":
        params["ellipse_max_cep"] = 50.0

    params["category_field"] = request.args.get(
        "category_field", default=params["category_field"]
    )
    params["category_format"] = request.args.get(
        "category_pattern", default=params["category_format"]
    )
    params["category_type"] = request.args.get(
        "category_type", default=params["category_type"]
    )
    params["category_histogram"] = request.args.get(
        "category_histogram", default=params["category_histogram"]
    )
    if params["category_histogram"] in ("true", "True", "TRUE"):
        params["category_histogram"] = True
    elif params["category_histogram"] in ("false", "False", "False"):
        params["category_histogram"] = False
    else:
        params["category_histogram"] = None

    params["spread"] = request.args.get("spread")
    # Handle text-value spread in both legacy and new format
    if params["spread"] in ("coarse", "large"):
        params["spread"] = 10
    elif params["spread"] in ("fine", "medium"):
        params["spread"] = 3
    elif params["spread"] in ("finest", "small"):
        params["spread"] = 1
    elif params["spread"] == "auto":
        params["spread"] = None
    else:
        try:
            params["spread"] = int(params["spread"])
        except (TypeError, ValueError):
            params["spread"] = None
    params["resolution"] = request.args.get("resolution", default=params["resolution"])

    params["cmap"] = request.args.get("cmap", default=params["cmap"])
    if params["cmap"] is None:
        if params["category_field"] is None:
            params["cmap"] = "bmy"
        else:
            params["cmap"] = "glasbey_category10"

    params["span_range"] = request.args.get("span", default="auto")
    params["geopoint_field"] = request.args.get(
        "geopoint_field", default=params["geopoint_field"]
    )
    params["timestamp_field"] = request.args.get(
        "timestamp_field", default=params["timestamp_field"]
    )

    # Handle dumb javascript on the client side
    if params["category_field"] == "null":
        params["category_field"] = None

    # Handle time bounding
    now = datetime.utcnow()
    params["stop_time"] = now
    if to_time:
        try:
            params["stop_time"] = convert_kibana_time(to_time, now)
        except ValueError:
            current_app.logger.exception("invalid to_time parameter")
            raise Exception("invalid to_time parameter")

    params["start_time"] = None
    if from_time:
        try:
            params["start_time"] = convert_kibana_time(from_time, now)
        except ValueError:
            current_app.logger.exception("invalid from_time parameter")
            raise Exception("invalid from_time parameter")

    params["start_time"], params["stop_time"] = quantize_time_range(
        params["start_time"], params["stop_time"]
    )

    if params["geopoint_field"] is None:
        current_app.logger.error("missing geopoint_field")
        raise Exception("missing geopoint_field")

    # Calculate a hash value for the specific parameter set
    parameter_hash = hashlib.md5()
    for k, p in sorted(params.items()):
        if isinstance(p, datetime):
            p = p.isoformat()
        parameter_hash.update(str(p).encode("utf-8"))
    parameter_hash = parameter_hash.hexdigest()

    current_app.logger.debug("Parameters: %s (%s)", params, parameter_hash)
    return parameter_hash, params


def generate_global_params(params, idx):
    geopoint_field = params["geopoint_field"]
    timestamp_field = params["timestamp_field"]
    start_time = params["start_time"]
    stop_time = params["stop_time"]
    category_field = params["category_field"]
    category_type = params["category_type"]
    category_histogram = params["category_histogram"]
    spread = params["spread"]
    span_range = params["span_range"]
    lucene_query = params["lucene_query"]
    dsl_query = params["dsl_query"]
    dsl_filter = params["dsl_filter"]

    histogram_range = 0
    histogram_interval = None
    global_doc_cnt = None
    global_bounds = None

    # Create base search
    base_s = get_search_base(current_app.config.get("ELASTIC"), params, idx)

    # west, south, east, north
    global_bounds = [-180, -90, 180, 90]
    global_doc_cnt = 0

    bounds_s = copy.copy(base_s)
    bounds_s = bounds_s.params(size=0)

    # We only need to do a global query if we are in span 'auto' or
    # using a numeric category

    # if span_range is auto we need to estimate the density
    if span_range is None or span_range == "auto":
        # See how far the data spans and how many points are in it
        bounds_s.aggs.metric("viewport", "geo_bounds", field=geopoint_field).metric(
            "point_count", "value_count", field=geopoint_field
        )
    # If the field is a number, we need to figure out it's min/max globally
    if category_type == "number" and category_histogram in (True, None):
        bounds_s.aggs.metric("field_stats", "stats", field=category_field)

    # Execute and process search
    if len(list(bounds_s.aggs)) > 0:
        bounds_resp = bounds_s.execute()
        assert len(bounds_resp.hits) == 0

        if hasattr(bounds_resp.aggregations, "viewport"):
            if hasattr(bounds_resp.aggregations.viewport, "bounds"):
                global_bounds = [
                    bounds_resp.aggregations.viewport.bounds.top_left.lon,
                    bounds_resp.aggregations.viewport.bounds.bottom_right.lat,
                    bounds_resp.aggregations.viewport.bounds.bottom_right.lon,
                    bounds_resp.aggregations.viewport.bounds.top_left.lat,
                ]
        if hasattr(bounds_resp.aggregations, "point_count"):
            global_doc_cnt = bounds_resp.aggregations.point_count.value

        # In a numeric field, we can fall back to histogram mode if there are too many unique values
        if category_type == "number" and category_histogram in (True, None):
            current_app.logger.info("Generating histogram parameters")
            if hasattr(bounds_resp.aggregations, "field_stats"):
                current_app.logger.info(
                    "field stats %s", bounds_resp.aggregations.field_stats
                )
                # to prevent strain on the cluster, if there are over 1million
                # documents given the current parameters, reduce the number of histogram
                # bins.  Note this is kinda a wag...maybe something smarter can be done
                if global_doc_cnt > 100000:
                    category_cnt = 200
                else:
                    category_cnt = 500
                # determine the range of category values
                if bounds_resp.aggregations.field_stats.count > 0:
                    if bounds_resp.aggregations.field_stats.max is None:
                        histogram_range = 0
                    elif bounds_resp.aggregations.field_stats.min is None:
                        histogram_range = 0
                    else:
                        histogram_range = (
                            bounds_resp.aggregations.field_stats.max
                            - bounds_resp.aggregations.field_stats.min
                        )
                        if histogram_range > 0:
                            # round to the nearest larger power of 10
                            histogram_range = math.pow(
                                10, math.ceil(math.log10(histogram_range))
                            )
                            histogram_interval = histogram_range / category_cnt
                            current_app.logger.info(
                                "histogram interval %s, category_cnt: %s ",
                                histogram_interval,
                                category_cnt,
                            )
                        else:
                            histogram_range = 0
    else:
        current_app.logger.debug("Skipping global query")

    # Return generated params dict
    generated_params = {
        "histogram_interval": histogram_interval,
        "global_doc_cnt": global_doc_cnt,
        "global_bounds": global_bounds,
    }

    return generated_params


def merge_generated_parameters(params, paramsfile, idx):
    """

    :param params:
    :param paramsfile:
    :param idx:
    :return:
    """
    # Lock and open file
    params_path = pathlib.Path(paramsfile)
    params_path.parent.mkdir(parents=True, exist_ok=True)

    lockfile_path = params_path.with_suffix(params_path.suffix + ".lock")

    generated_params = None
    with lockfile_path.open("w") as lockfile:
        fcntl.flock(lockfile, fcntl.LOCK_EX)
        try:
            if params_path.exists():
                current_app.logger.warn(
                    "Found parameters file, using generated params from that"
                )
                # Params file exists so read it in
                with params_path.open("r") as stream:
                    full_params = json.load(stream)
                # update timestamp for cache cleanup purposes
                params_path.touch(exist_ok=True)
                generated_params = full_params.get("generated_params")

            if generated_params is None:
                current_app.logger.warn("Discovering generated params")
                # Params file either does not exists
                # or does not have generated parameters in it
                generated_params = generate_global_params(params, idx)

                # Write extended params to file
                params_cleaned = copy.copy(params)
                params_cleaned["generated_params"] = generated_params

                # Change all datetimes to string format
                for k, p in params_cleaned.items():
                    if isinstance(p, datetime):
                        params_cleaned[k] = p.isoformat()
                with params_path.open("w") as pfile:
                    json.dump(params_cleaned, pfile)
        finally:
            fcntl.lockf(lockfile, fcntl.LOCK_UN)

    params["generated_params"] = generated_params
    return params
