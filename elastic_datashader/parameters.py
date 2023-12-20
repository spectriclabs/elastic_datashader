from datetime import datetime, timedelta, timezone
from hashlib import sha256
from json import loads
from time import sleep
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote

import copy
import math
import os

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConflictError
from elasticsearch_dsl import Document

from .config import config
from .elastic import get_search_base, build_dsl_filter
from .logger import logger
from .timeutil import quantize_time_range, convert_kibana_time

from pydantic import BaseModel, Field

class SearchParams(BaseModel):
    geopoint_field: str
    params: dict
    cmap: str = Field(default="bym")
    resolution:str = Field(default="finest")
    span_range:str = Field(default="auto", alias='span')
    spread:str = Field(default="auto") # Point Size
    timeOverlap:bool = Field(default=False)
    timeOverlapSize:str = Field(default="auto")
    timestamp_field:str = Field(default="@timestamp")
    search_nautical_miles: int = Field(default=50)
    geofield_type: str = Field(default='geo_point')
    bucket_max: float = Field(default=100, ge=0, le=100)
    bucket_min: float  = Field(default=0, ge=0, le=1)

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
        'geofield_type': 'geo_point',
        "highlight": None,
        "lucene_query": None,
        "max_batch": config.max_batch,
        "max_bins": config.max_bins,
        "max_ellipses_per_tile": config.max_ellipses_per_tile,
        "render_mode": None,
        "resolution": "finest",
        "search_nautical_miles": 50,
        "span_range": None,
        "spread": None,
        "timestamp_field": "@timestamp",
        "track_connection": None,
        "use_centroid": False,
        "user": None,
        "bucket_min": 0,
        "bucket_max": 1,
        "timeOverlap": False,
        "timeOverlapSize": "auto"
    }


def normalize_spread(spread: Optional[str]) -> Optional[int]:
    # Handle text-value spread in both legacy and new format
    if spread in ("coarse", "large"):
        return 10

    if spread in ("fine", "medium"):
        return 3

    if spread in ("finest", "small"):
        return 1

    if spread == "auto":
        return None

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

def get_to_time(params: Optional[Dict[Any, Any]]) -> Optional[str]:
    if params:
        if found := params.get("timeFilters", {}).get("to", None):
            return found

    return "now"

def get_dsl_filter(params: Optional[Dict[Any, Any]]) -> Optional[Dict[str, Any]]:
    if params and "filters" in params:
        return build_dsl_filter(params["filters"])

    return None

def get_query(params: Optional[Dict[Any, Any]]) -> Dict[str, Any]:
    query = params.get("query", {})

    if query and query.get("language", None) in ("lucene", "kuery"):
        # accept 'kuery' for backward compatibility...
        return {"lucene_query": query.get("query")}

    if query and query.get("language", None) == "dsl":
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
    '''
    Takes a string and converts it to a distance in
    nautical miles (nm).  If the input string does not
    have a mapping, then 50.0 nm is returned.
    '''
    # Reduce this to just "search" -> "search_distance" once Kibana is changed
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

def get_filter_distance(track_filter: Optional[str]) -> Optional[float]:
    '''
    Takes a string and converts it to a distance in
    nautical miles (nm).  If the input string cannot
    be converted to a number, then None is returned.
    '''
    if track_filter is None or track_filter == "default":
        return None

    if track_filter == "none":
        return 0.0

    if track_filter == "short":
        return 1.0

    if track_filter == "normal":
        return 10.0

    if track_filter == "long":
        return 50.0

    try:
        return float(track_filter)
    except (TypeError, ValueError):
        pass

    return None

def get_category_histogram(category_histogram: Optional[str]) -> Optional[bool]:
    if category_histogram and category_histogram.lower() == "true":
        return True

    if category_histogram and category_histogram.lower() == "false":
        return False

    return None

def get_cmap(cmap: Optional[str], category_field: Optional[str]) -> str:
    if cmap is None or cmap == "":
        if category_field is None or category_field == "":
            return "bmy"

        return "glasbey_category10"

    return cmap

def get_category_field(category_field: Optional[str]) -> Optional[str]:
    # Handle dumb javascript on the client side
    if category_field == "null":
        return None

    return category_field

def get_time_bounds(now: datetime, from_time: Optional[str], to_time: Optional[str]) -> Dict[str, datetime]:
    start_time = None
    stop_time = now

    if from_time:
        try:
            start_time = convert_kibana_time(from_time, now, 'down')
        except ValueError as err:
            logger.exception("invalid from_time parameter")
            raise ValueError("invalid from_time parameter") from err

    if to_time:
        try:
            stop_time = convert_kibana_time(to_time, now, 'up')
        except ValueError as err:
            logger.exception("invalid to_time parameter")
            raise ValueError("invalid to_time parameter") from err

    if start_time and stop_time:
        start_time, stop_time = quantize_time_range(start_time, stop_time)

    return {"start_time": start_time, "stop_time": stop_time}

def get_parameter_hash(params: Dict[str, Any]) -> str:
    """Calculates a hash value for the specific parameter set"""
    parameter_hash = sha256()

    for _, p in sorted(params.items()):
        if isinstance(p, datetime):
            p = p.isoformat()
        parameter_hash.update(str(p).encode("utf-8"))

    return parameter_hash.hexdigest()

def extract_parameters(headers: Dict[Any, Any], query_params: Dict[Any, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Get the parameters from a request and return hash and dict of parameters
    """
    params = create_default_params()
    unhashed_params = {}  # Some parameters aren't used to make the final hash
    params['user'] = headers.get("es-security-runas-user", None)

    # There's a query parameter called "params"
    params_param = load_params_param(query_params.get("params"))

    now = datetime.now(timezone.utc)
    from_time = get_from_time(params_param)
    to_time = get_to_time(params_param)
    render_mode = get_render_mode(query_params)
    category_field = get_category_field(query_params.get("category_field", params["category_field"]))

    unhashed_params["mapZoom"] = params_param.get("zoom", None)
    unhashed_params["extent"] = params_param.get("extent", None)

    params["dsl_filter"] = get_dsl_filter(params_param)
    params.update(get_query(params_param))
    params["render_mode"] = render_mode
    params.update(get_ellipse_params(render_mode, query_params))
    params["search_nautical_miles"] = get_search_distance(query_params)
    params["track_connection"] = query_params.get("track_connection", params["track_connection"])
    params["filter_distance"] = get_filter_distance(query_params.get("track_filter", None))
    params["category_field"] = category_field
    params["category_format"] = query_params.get("category_pattern", params["category_format"])
    params["category_type"] = query_params.get("category_type", params["category_type"])
    params["category_histogram"] = get_category_histogram(query_params.get("category_histogram", None))
    params["highlight"] = query_params.get("highlight")
    params["spread"] = normalize_spread(query_params.get("spread"))
    params["resolution"] = query_params.get("resolution", params["resolution"])
    params["use_centroid"] = query_params.get("use_centroid", params["use_centroid"])
    params["cmap"] = get_cmap(query_params.get("cmap", None), category_field)
    params["span_range"] = query_params.get("span", "auto")
    params["geopoint_field"] = query_params.get("geopoint_field", params["geopoint_field"])
    params["geofield_type"] = query_params.get("geofield_type", params["geofield_type"])
    params["timestamp_field"] = query_params.get("timestamp_field", params["timestamp_field"])
    params.update(get_time_bounds(now, from_time, to_time))
    params["bucket_min"] = float(query_params.get("bucket_min", 0))
    params["bucket_max"] = float(query_params.get("bucket_max", 1))
    params["timeOverlap"] = query_params.get("timeOverlap", "false") == "true"
    params["timeOverlapSize"] = query_params.get("timeOverlapSize", "auto")
    params["debug"] = query_params.get("debug", False) == 'true'

    if params["geofield_type"] == "undefined":
        params["geofield_type"] = "geo_point"

    if params["geopoint_field"] is None:
        logger.error("missing geopoint_field")
        raise ValueError("missing geopoint_field")

    parameter_hash = get_parameter_hash(params)
    all_params = {**params, **unhashed_params}
    logger.debug("Parameters: %s (%s)", all_params, parameter_hash)
    return parameter_hash, all_params

def generate_global_params(headers, params, idx):
    geopoint_field = params["geopoint_field"]
    category_field = params["category_field"]
    category_type = params["category_type"]
    category_histogram = params["category_histogram"]
    current_zoom = params["mapZoom"]
    span_range = params["span_range"]
    resolution = params["resolution"]
    histogram_range = 0
    histogram_interval = None
    histogram_cnt = None
    global_doc_cnt = None
    field_min = None
    field_max = None

    # Create base search
    base_s = get_search_base(config.elastic_hosts, headers, params, idx)
    base_s = base_s[0:0]
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

    field_type = params["geofield_type"] # CCS you cannot get mappings so we needed to push the field type from the client side
    # Execute and process search
    if len(list(bounds_s.aggs)) > 0 and field_type != "geo_shape":
        logger.info(bounds_s.to_dict())
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
    elif field_type == "geo_shape":
        zoom = 0
        if resolution == "coarse":
            zoom = 5
        elif resolution == "fine":
            zoom = 6
        elif resolution == "finest":
            zoom = 7
        geotile_precision = current_zoom+zoom
        histogram_cnt = 500

        if category_field:
            max_value_s = copy.copy(base_s)
            bucket = max_value_s.aggs.bucket("comp", "geotile_grid", field=geopoint_field, precision=geotile_precision, size=1)
            bucket.metric("sum", "sum", field=category_field, missing=0)
            resp = max_value_s.execute()
            estimated_points_per_tile = resp.aggregations.comp.buckets[0].sum['value']
            histogram_range = estimated_points_per_tile
        else:
            max_value_s = copy.copy(base_s)
            max_value_s.aggs.bucket("comp", "geotile_grid", field=geopoint_field, precision=geotile_precision, size=1)
            resp = max_value_s.execute()
            estimated_points_per_tile = resp.aggregations.comp.buckets[0].doc_count
            histogram_range = estimated_points_per_tile
        global_doc_cnt = estimated_points_per_tile
        if histogram_range > 0:
            # round to the nearest larger power of 10
            histogram_range = math.pow(
                10, math.ceil(math.log10(histogram_range))
            )
            histogram_interval = histogram_range / histogram_cnt
        field_min = 0
        field_max = estimated_points_per_tile
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


def merge_generated_parameters(headers, params, idx, param_hash):
    layer_id = f"{param_hash}_{config.hostname}"
    es = Elasticsearch(
        config.elastic_hosts.split(","),
        verify_certs=False,
        timeout=120
    )

    # See if the hash exists
    try:
        doc = Document.get(id=layer_id, using=es, index=".datashader_layers")
    except NotFoundError:
        doc = None

    if not doc:
        # if not, create the hash in the db but only if it does not already exist
        try:
            doc = Document(
                _id=layer_id,
                creating_host=config.hostname,
                creating_pid=os.getpid(),
                creating_timestamp=datetime.now(timezone.utc),
                generated_params=None,
                params=params,
            )
            doc.save(using=es, index=".datashader_layers", op_type="create", skip_empty=False)
            logger.debug("Created Hash document")
        except ConflictError:
            logger.debug("Hash document now exists, continuing")

        # re-fetch to get sequence number correct
        doc = Document.get(id=layer_id, using=es, index=".datashader_layers")

    # Check for generator timeouts:
    if doc.to_dict().get("generated_params", {}).get("generation_start_time") and \
                datetime.now(timezone.utc) > datetime.strptime(doc.to_dict().get("generated_params", {}).get("generation_start_time"), "%Y-%m-%dT%H:%M:%S.%f%z")+timedelta(seconds=5*60):
        # Something caused the worker generating the params to time out so clear that entry
        try:
            doc.update(
                using=es,
                index=".datashader_layers",
                retry_on_conflict=0,
                refresh=True,
                generated_params=None,
            )
        except ConflictError:
            logger.debug("Abandoned resetting parameters due to conflict, other process has completed.")

    # Loop-check if the generated params are in missing/in-process/complete
    timeout_at = datetime.now(timezone.utc)+timedelta(seconds=45)

    while doc.to_dict().get("generated_params", {}).get("complete", False) is False:
        if datetime.now(timezone.utc) > timeout_at:
            logger.info("Hit timeout waiting for generated parameters to be placed into database")
            break

        # If missing, mark them as in generation
        if not doc.to_dict().get("generated_params", None):
            # Mark them as being generated but do so with concurrenty control
            # https://www.elastic.co/guide/en/elasticsearch/reference/current/optimistic-concurrency-control.html
            logger.info("Discovering generated parameters")
            generated_params = {
                "complete": False,
                "generation_start_time": datetime.now(timezone.utc),
                "generating_host": config.hostname,
                "generating_pid": os.getpid(),
            }

            try:
                doc.update(
                    using=es,
                    index=".datashader_layers",
                    retry_on_conflict=0,
                    refresh=True,
                    generated_params=generated_params,
                )
            except ConflictError:
                logger.debug("Abandoned generating parameters due to conflict, will wait for other process to complete.")
                break

            # Generate and save off parameters
            logger.warning("Discovering generated params")
            generated_params.update(generate_global_params(headers, params, idx))
            generated_params["generation_complete_time"] = datetime.now(timezone.utc)
            generated_params["complete"] = True
            # Store off generated params
            doc.update(
                using=es,
                index=".datashader_layers",
                retry_on_conflict=0,
                refresh=True,
                generated_params=generated_params,
            )
            break

        sleep(1)
        doc = Document.get(id=layer_id, using=es, index=".datashader_layers")

    # We now have params so use them
    params["generated_params"] = doc.to_dict().get("generated_params")
    return params
