from typing import Optional

import copy
import hashlib
import json
import math
import pathlib
import threading
import os
import socket
import time
from urllib.parse import unquote
from datetime import datetime, timedelta

from flask import current_app, Response

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, AttrDict, Document
from elasticsearch.exceptions import NotFoundError, ConflictError

from .timeutil import quantize_time_range, convert_kibana_time
from .elastic import get_search_base, build_dsl_filter

def normalize_spread(spread) -> Optional[int]:
    # Handle text-value spread in both legacy and new format
    if spread in ("coarse", "large"): return 10
    if spread in ("fine", "medium"): return 3
    if spread in ("finest", "small"): return 1
    if spread == "auto": return None

    try:
        return int(spread)
    except (TypeError, ValueError):
        pass

    return None

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
        "highlight": None,
        "render_mode": None,
        "ellipse_major": "",
        "ellipse_minor": "",
        "ellipse_tilt": "",
        "ellipse_units": "",
        "search_distance": 50,
        "filter_distance": None,
        "track_connection": None,
        "spread": None,
        "span_range": None,
        "resolution": "finest",
        "use_centroid": False,
        "max_bins": int(current_app.config["MAX_BINS"]),
        "max_batch": int(current_app.config["MAX_BATCH"]),
        "max_ellipses_per_tile": int(current_app.config["MAX_ELLIPSES_PER_TILE"]),
        "debug": False,
    }

    # Extract user from headers

    # Argument Parameters
    arg_params = request.args.get("params")
    if arg_params and arg_params != "{params}":
        arg_params = unquote(arg_params)
        arg_params = json.loads(arg_params)
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
    params["render_mode"] = request.args.get("render_mode", default=params["render_mode"])
    
    # ensure backwards compatibility with older API
    if params["render_mode"] == None:
        if request.args.get("ellipses", default=None) == "true":
            params["render_mode"] = "ellipses"
        else:
            params["render_mode"] = "points"

    if params["render_mode"] == "ellipses":
        # Handle the other fields
        params["ellipse_major"] = request.args.get("ellipse_major", default="")
        params["ellipse_minor"] = request.args.get("ellipse_minor", default="")
        params["ellipse_tilt"] = request.args.get("ellipse_tilt", default="")
        params["ellipse_units"] = request.args.get("ellipse_units", default="")
        if (
            params["ellipse_major"] == ""
            or params["ellipse_minor"] == ""
            or params["ellipse_tilt"] == ""
        ):
            params["render_mode"] = "points"
    
    #Reduce this to just "search" -> "search_distance" once Kibana is changed
    if request.args.get("ellipse_search", default="") == "narrow":
        params["search_distance"] = 1.0
    elif request.args.get("ellipse_search", default="") == "normal":
        params["search_distance"] = 10.0
    elif request.args.get("ellipse_search", default="") == "wide":
        params["search_distance"] = 50.0
    if request.args.get("track_search", default="") == "narrow":
        params["search_distance"] = 1.0
    elif request.args.get("track_search", default="") == "normal":
        params["search_distance"] = 10.0
    elif request.args.get("track_search", default="") == "wide":
        params["search_distance"] = 50.0

    params["track_connection"] = request.args.get("track_connection", default=params["track_connection"])
    
    params["filter_distance"] = request.args.get("track_filter", default=params["filter_distance"])
    if params["filter_distance"] == "none":
        params["filter_distance"] = 0.0
    elif params["filter_distance"] == "short":
        params["filter_distance"] = 1.0
    elif params["filter_distance"] == "normal":
        params["filter_distance"] = 10.0
    elif params["filter_distance"] == "long":
        params["filter_distance"] = 50.0
    elif params["filter_distance"] in ("default", None):
        params["filter_distance"] = None
    else:
        params["filter_distance"] = float(params["filter_distance"])

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

    params["highlight"] = request.args.get("highlight")

    params["spread"] = normalize_spread(request.args.get("spread"))
    params["resolution"] = request.args.get("resolution", default=params["resolution"])
    params["use_centroid"] = request.args.get("use_centroid", default=params["use_centroid"])
    
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
            params["stop_time"] = convert_kibana_time(to_time, now, 'up')
        except ValueError:
            current_app.logger.exception("invalid to_time parameter")
            raise Exception("invalid to_time parameter")

    params["start_time"] = None
    if from_time:
        try:
            params["start_time"] = convert_kibana_time(from_time, now, 'down')
        except ValueError:
            current_app.logger.exception("invalid from_time parameter")
            raise Exception("invalid from_time parameter")

    if params.get("start_time") and params.get("stop_time"):
        params["start_time"], params["stop_time"] = quantize_time_range(
            params["start_time"], params["stop_time"]
        )

    if params["geopoint_field"] is None:
        current_app.logger.error("missing geopoint_field")
        raise Exception("missing geopoint_field")

    params["debug"] = ( request.args.get("debug", default=False) == 'true' )

    # Calculate a hash value for the specific parameter set
    parameter_hash = hashlib.md5()
    for k, p in sorted(params.items()):
        if isinstance(p, datetime):
            p = p.isoformat()
        parameter_hash.update(str(p).encode("utf-8"))
    parameter_hash = parameter_hash.hexdigest()

    # Unhashed parameters
    params["mapZoom"] = arg_params.get("zoom", None)
    params["extent"] = arg_params.get("extent", None)
    
    current_app.logger.debug("Parameters: %s (%s)", params, parameter_hash)
    return parameter_hash, params

def update_params(params, updates=None):
    if updates:
        params.update(updates)
    
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
    histogram_cnt = None
    global_doc_cnt = None
    field_min = None
    field_max = None

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
    if category_type == "number":
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

        if hasattr(bounds_resp.aggregations, "field_stats") and bounds_resp.aggregations.field_stats.count > 0:

            if bounds_resp.aggregations.field_stats.max is None:
                field_max = 0
            else:
                field_max = bounds_resp.aggregations.field_stats.max

            if bounds_resp.aggregations.field_stats.min is None:
                field_min = 0
            else:
                field_min = bounds_resp.aggregations.field_stats.min

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
                    histogram_cnt = 200
                else:
                    histogram_cnt = 500
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
                            histogram_interval = histogram_range / histogram_cnt
                            current_app.logger.info(
                                "histogram interval %s, category_cnt: %s ",
                                histogram_interval,
                                histogram_cnt,
                            )
                        else:
                            histogram_range = 0
    else:
        current_app.logger.debug("Skipping global query")

    # Return generated params dict
    generated_params = {
        "histogram_interval": histogram_interval,
        "histogram_cnt": histogram_cnt,
        "global_doc_cnt": global_doc_cnt,
        "global_bounds": global_bounds,
        "field_max": field_max,
        "field_min": field_min
    }

    return generated_params


def merge_generated_parameters(params, idx, hash):
    """

    :param params:
    :param paramsfile:
    :param idx:
    :return:
    """

    layer_id = "%s_%s" % (hash, socket.gethostname())
    es = Elasticsearch(
        current_app.config.get("ELASTIC").split(","),
        verify_certs=False,
        timeout=120
    )

    #See if the hash exists
    try:
        doc = Document.get(id=layer_id, using=es, index=".datashader_layers")
    except NotFoundError:
        doc = None

    if not doc:
        #if not, create the hash in the db but only if it does not already exist
        try:
            doc = Document(_id=layer_id,
                            creating_host=socket.gethostname(),
                            creating_pid=os.getpid(),
                            creating_timestamp=datetime.now(),
                            generated_params=None,
                            params=params)
            doc.save(using=es, index=".datashader_layers", op_type="create", skip_empty=False)
            current_app.logger.debug("Created Hash document")
        except ConflictError:
            current_app.logger.debug("Hash document now exists, continuing")

        #re-fetch to get sequence number correct
        doc = Document.get(id=layer_id, using=es, index=".datashader_layers")

    #Check for generator timeouts:
    if doc.to_dict().get("generated_params", {}).get("generation_start_time") and \
                datetime.now() > datetime.strptime(doc.to_dict().get("generated_params", {}).get("generation_start_time"),"%Y-%m-%dT%H:%M:%S.%f")+timedelta(seconds=5*60):
        #Something caused the worker generating the params to time out so clear that entry
        try:
            doc.update(using=es, index=".datashader_layers", retry_on_conflict=0, refresh=True, \
                generated_params=None)
        except ConflictError:
            current_app.logger.debug("Abandoned resetting parameters due to conflict, other process has completed.")

    #Loop-check if the generated params are in missing/in-process/complete
    timeout_at = datetime.now()+timedelta(seconds=45)
    while doc.to_dict().get("generated_params", {}).get("complete", False) == False:
        if datetime.now() > timeout_at:
            current_app.logger.info("Hit timeout waiting for generated parameters to be placed into database")
            break
        #If missing, mark them as in generation
        if not doc.to_dict().get("generated_params", None):
            #Mark them as being generated but do so with concurrenty control
            #https://www.elastic.co/guide/en/elasticsearch/reference/current/optimistic-concurrency-control.html
            current_app.logger.info("Discovering generated parameters")
            generated_params = dict()
            generated_params["complete"] = False
            generated_params["generation_start_time"] = datetime.now()
            generated_params["generating_host"] = socket.gethostname()
            generated_params["generating_pid"] = os.getpid()
            try:
                doc.update(using=es, index=".datashader_layers", retry_on_conflict=0, refresh=True, \
                    generated_params=generated_params)
            except ConflictError:
                current_app.logger.debug("Abandoned generating parameters due to conflict, will wait for other process to complete.")
                break
            #Generate and save off parameters
            current_app.logger.warn("Discovering generated params")
            generated_params.update(generate_global_params(params, idx))
            generated_params["generation_complete_time"] = datetime.now()
            generated_params["complete"] = True
            #Store off generated params
            doc.update(using=es, index=".datashader_layers", retry_on_conflict=0, refresh=True, \
                    generated_params=generated_params)
            break
        else:
            time.sleep(1)
            doc = Document.get(id=layer_id, using=es, index=".datashader_layers")

    #We now have params so use them
    params["generated_params"] = doc.to_dict().get("generated_params")
    return params
