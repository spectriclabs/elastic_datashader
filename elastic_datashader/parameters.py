from datetime import datetime, timedelta
from json import loads
from socket import gethostname
from time import sleep
from typing import Optional
from urllib.parse import unquote

import copy
import hashlib
import math
import os

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document
from elasticsearch.exceptions import NotFoundError, ConflictError

from .config import config
from .elastic import get_search_base, build_dsl_filter
from .timeutil import quantize_time_range, convert_kibana_time

def create_default_params() -> Dict[str, Any]:
    return {
        "category_field": None,
        "category_format": None,
        "category_histogram": None,
        "category_type": None,
        "cmap": None,
        "debug": False,
        "dsl_query": None,
        "dsl_filter": None,
        "ellipse_major": "",
        "ellipse_minor": "",
        "ellipse_tilt": "",
        "ellipse_units": "",
        "filter_distance": None,
        "geopoint_field": None,
        "highlight": None,
        "lucene_query": None,
        "max_batch": config.max_batch,
        "max_bins": config.max_bins,
        "max_ellipses_per_tile": config.max_ellipses_per_tile,
        "render_mode": None,
        "resolution": "finest",
        "search_distance": 50,
        "span_range": None,
        "spread": None,
        "timestamp_field": "@timestamp",
        "track_connection": None,
        "use_centroid": False,
        "user": None,
    }


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

def load_params_param(params: Optional[str]) -> Optional[Dict[Any, Any]]:
    if params and params != "{params}":
        return loads(unquote(params))

    return None

def get_from_time(params: Optional[Dict[Any, Any]]) -> Optional[str]:
    if params:
        return params.get("timeFilters", {}).get("from")

    return None

def get_to_time(params: Optional[Dict[Any, Any]]) -> str:
    if params:
        return params.get("timeFilters", {}).get("to")

    return "now"

def get_dsl_filter(params: Optional[Dict[Any, Any]]) -> Optional[Dict[str, Any]]:
    if params and "filters" in params:
        return build_dsl_filter(params["filters"])

    return None

def get_query(params: Optional[Dict[Any, Any]]) -> Dict[str, Any]:
    query = params.get("query", {})

    if query and query.get("language", None) in ("lucene", "kuery"):
        # accept 'kuery' for backwords compatibility...
        return {"lucene_query": query.get("query")}
    elif query and query.get("langauge", None) == "dsl":
        return {"dsl_query": query.get("query")}

    return {}

def get_render_mode(query_params: Dict[Any, Any]) -> str:
    # ensure backwards compatibility with older API
    render_mode = query_params.get("render_mode", None)

    if render_mode is not None:
        return render_mode

    if (
        query_params.get("ellipses", None) == "true" and
        query_params.get("ellipse_major", "") != "" and
        query_params.get("ellipse_minor", "") != "" and
        query_params.get("ellipse_tilt", "") != ""
    ):
        return "ellipses"

    return "points"

def get_ellipse_params(render_mode: str, query_params: Dict[Any, Any]) -> Dict[str, Any]:
    if render_mode != "ellipses":
        return {}

    param_names = ("ellipse_major", "ellipse_minor", "ellipse_tilt", "ellipse_units")
    return {name: query_params.get(name, "") for name in param_names}

def get_search_distance(query_params: Dict[Any, Any]) -> float:
    #Reduce this to just "search" -> "search_distance" once Kibana is changed
    if query_params.get("track_search", "") == "narrow":
        return 1.0

    if query_params.get("track_search", "") == "normal":
        return 10.0

    if query_params.get("track_search", "") == "wide":
        return 50.0

    if query_params.get("ellipse_search", "") == "narrow":
        return 1.0

    if query_params.get("ellipse_search", "") == "normal":
        return 10.0

    if query_params.get("ellipse_search", "") == "wide":
        return 50.0

    return 50.0

def extract_parameters(headers: Dict[Any, Any], query_params: Dict[Any, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Get the parameters from a request and return hash and dict of parameters
    """
    params = create_default_params()
    params['user'] = headers.get("es-security-runas-user", None)

    # There's a query parameter called "params"
    params_param = load_params_param(query_params.get("params"))

    from_time = get_from_time(params_param)
    to_time = get_to_time(params_param)
    render_mode = get_render_mode(query_params)

    params["dsl_filter"] = get_dsl_filter(params_param)
    params.update(get_query(params_param))
    params["render_mode"] = render_mode
    params.update(get_ellipse_params(render_mode, query_params))
    params["search_distance"] = get_search_distance(query_params)
    params["track_connection"] = query_params.get("track_connection", params["track_connection"])
    
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
        except ValueError as err:
            logger.exception("invalid to_time parameter")
            raise Exception("invalid to_time parameter") from err

    params["start_time"] = None
    if from_time:
        try:
            params["start_time"] = convert_kibana_time(from_time, now, 'down')
        except ValueError as err:
            logger.exception("invalid from_time parameter")
            raise Exception("invalid from_time parameter") from err

    if params.get("start_time") and params.get("stop_time"):
        params["start_time"], params["stop_time"] = quantize_time_range(
            params["start_time"], params["stop_time"]
        )

    if params["geopoint_field"] is None:
        logger.error("missing geopoint_field")
        raise Exception("missing geopoint_field")

    params["debug"] = ( request.args.get("debug", default=False) == 'true' )

    # Calculate a hash value for the specific parameter set
    parameter_hash = hashlib.md5()

    for _, p in sorted(params.items()):
        if isinstance(p, datetime):
            p = p.isoformat()
        parameter_hash.update(str(p).encode("utf-8"))

    parameter_hash = parameter_hash.hexdigest()

    # Unhashed parameters
    params["mapZoom"] = arg_params.get("zoom", None)
    params["extent"] = arg_params.get("extent", None)
    
    logger.debug("Parameters: %s (%s)", params, parameter_hash)
    return parameter_hash, params

def update_params(params, updates=None):
    if updates:
        params.update(updates)
    
    # Calculate a hash value for the specific parameter set
    parameter_hash = hashlib.md5()

    for _, p in sorted(params.items()):
        if isinstance(p, datetime):
            p = p.isoformat()
        parameter_hash.update(str(p).encode("utf-8"))

    parameter_hash = parameter_hash.hexdigest()

    logger.debug("Parameters: %s (%s)", params, parameter_hash)
    return parameter_hash, params

def generate_global_params(params, idx):
    geopoint_field = params["geopoint_field"]
    category_field = params["category_field"]
    category_type = params["category_type"]
    category_histogram = params["category_histogram"]
    span_range = params["span_range"]

    histogram_range = 0
    histogram_interval = None
    histogram_cnt = None
    global_doc_cnt = None
    field_min = None
    field_max = None

    # Create base search
    base_s = get_search_base(config.elastic_hosts, params, idx)

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
            logger.info("Generating histogram parameters")
            if hasattr(bounds_resp.aggregations, "field_stats"):
                logger.info(
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
                            logger.info(
                                "histogram interval %s, category_cnt: %s ",
                                histogram_interval,
                                histogram_cnt,
                            )
                        else:
                            histogram_range = 0
    else:
        logger.debug("Skipping global query")

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


def merge_generated_parameters(params, idx, param_hash):
    layer_id = "%s_%s" % (param_hash, gethostname())
    es = Elasticsearch(
        config.elastic_hosts.split(","),
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
                            creating_host=gethostname(),
                            creating_pid=os.getpid(),
                            creating_timestamp=datetime.now(),
                            generated_params=None,
                            params=params)
            doc.save(using=es, index=".datashader_layers", op_type="create", skip_empty=False)
            logger.debug("Created Hash document")
        except ConflictError:
            logger.debug("Hash document now exists, continuing")

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
            logger.debug("Abandoned resetting parameters due to conflict, other process has completed.")

    #Loop-check if the generated params are in missing/in-process/complete
    timeout_at = datetime.now()+timedelta(seconds=45)
    while doc.to_dict().get("generated_params", {}).get("complete", False) == False:
        if datetime.now() > timeout_at:
            logger.info("Hit timeout waiting for generated parameters to be placed into database")
            break
        #If missing, mark them as in generation
        if not doc.to_dict().get("generated_params", None):
            #Mark them as being generated but do so with concurrenty control
            #https://www.elastic.co/guide/en/elasticsearch/reference/current/optimistic-concurrency-control.html
            logger.info("Discovering generated parameters")
            generated_params = dict()
            generated_params["complete"] = False
            generated_params["generation_start_time"] = datetime.now()
            generated_params["generating_host"] = gethostname()
            generated_params["generating_pid"] = os.getpid()
            try:
                doc.update(using=es, index=".datashader_layers", retry_on_conflict=0, refresh=True, \
                    generated_params=generated_params)
            except ConflictError:
                logger.debug("Abandoned generating parameters due to conflict, will wait for other process to complete.")
                break
            #Generate and save off parameters
            logger.warn("Discovering generated params")
            generated_params.update(generate_global_params(params, idx))
            generated_params["generation_complete_time"] = datetime.now()
            generated_params["complete"] = True
            #Store off generated params
            doc.update(using=es, index=".datashader_layers", retry_on_conflict=0, refresh=True, \
                    generated_params=generated_params)
            break
        else:
            sleep(1)
            doc = Document.get(id=layer_id, using=es, index=".datashader_layers")

    #We now have params so use them
    params["generated_params"] = doc.to_dict().get("generated_params")
    return params
