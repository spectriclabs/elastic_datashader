#!/usr/bin/env python3
import copy
import os
import threading
from typing import Any, Dict, List

import yaml
from datashader.utils import lnglat_to_meters
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, AttrDict
from flask import current_app, request


def get_search_base(
    elastic_uri: str,
    params: Dict[str, Any],
    idx: int
) -> Search:
    """

    :param elastic_uri:
    :param params:
    :param idx:
    :return:
    """
    timestamp_field = params["timestamp_field"]
    start_time = params["start_time"]
    stop_time = params["stop_time"]
    lucene_query = params["lucene_query"]
    dsl_query = params["dsl_query"]
    dsl_filter = params["dsl_filter"]
    user = params.get("user")

    # Connect to Elasticsearch
    es = Elasticsearch(
        elastic_uri.split(","),
        verify_certs=False,
        timeout=900,
        headers=get_es_headers(request.headers, user),
    )

    # Create base search
    base_s = Search(index=idx).using(es)

    # Add time bounds
    # Handle time calculations
    time_range = None
    if timestamp_field:
        time_range = {timestamp_field: {}}
        if start_time is not None:
            time_range[timestamp_field]["gte"] = start_time
        if stop_time is not None:
            time_range[timestamp_field]["lte"] = stop_time

    if time_range and time_range[timestamp_field]:
        current_app.logger.info("TIME RANGE: %s", time_range)
        base_s = base_s.filter("range", **time_range)

    # Add lucene query
    if lucene_query:
        base_s = base_s.filter("query_string", query=lucene_query)

    # Add dsl filtering
    if dsl_filter or dsl_query:
        # Need to convert to a dict, merge with filters then convert back to a search object
        base_dict = base_s.to_dict()
        # setup an empty filter list if necessary
        if base_dict.get("query", {}).get("bool", {}).get("filter") is None:
            base_dict["query"]["bool"]["filter"] = []
        # Add the dsl_query
        if dsl_query:
            base_dict["query"]["bool"]["filter"].append(dsl_query)
        # add dsl_filters
        if dsl_filter:
            for f in dsl_filter["filter"]:
                base_dict["query"]["bool"]["filter"].append(f)
            if base_dict.get("query", {}).get("bool", {}).get("must_not") is None:
                base_dict["query"]["bool"]["must_not"] = []
            for f in dsl_filter["must_not"]:
                base_dict["query"]["bool"]["must_not"].append(f)

        # convert back
        base_s = Search.from_dict(base_dict).index(idx).using(es)

    return base_s


def build_dsl_filter(filter_inputs):
    """

    :param filter_inputs:
    :return:
    """
    if len(filter_inputs) == 0:
        return None
    filter_dict = {"filter": [{"match_all": {}}], "must_not": []}

    for f in filter_inputs:
        current_app.logger.info("Filter %s\n %s", f.get("meta").get("type"), f)
        # Skip disabled filters
        if f.get("meta").get("disabled") in ("true", True):
            continue

        # Handle spatial filters
        if f.get("meta").get("type") == "spatial_filter":
            if f.get("geo_polygon"):
                geo_polygon_dict = {"geo_polygon": f.get("geo_polygon")}
                if f.get("meta").get("negate"):
                    filter_dict["must_not"].append(geo_polygon_dict)
                else:
                    filter_dict["filter"].append(geo_polygon_dict)
            elif f.get("geo_bounding_box"):
                geo_bbox_dict = {"geo_bounding_box": f.get("geo_bounding_box")}
                if f.get("meta").get("negate"):
                    filter_dict["must_not"].append(geo_bbox_dict)
                else:
                    filter_dict["filter"].append(geo_bbox_dict)
        # Handle phrase matching
        elif f.get("meta").get("type") in ("phrase", "phrases", "bool"):
            if f.get("meta").get("negate"):
                filter_dict["must_not"].append(f.get("query"))
            else:
                filter_dict["filter"].append(f.get("query"))
        elif f.get("meta").get("type") == "range":
            range_dict = {"range": f.get("range")}
            if f.get("meta").get("negate"):
                filter_dict["must_not"].append(range_dict)
            else:
                filter_dict["filter"].append(range_dict)
        elif f.get("meta").get("type") == "exists":
            exists_dict = {"exists": f.get("exists")}
            if f.get("meta").get("negate"):
                filter_dict["must_not"].append(exists_dict)
            else:
                filter_dict["filter"].append(exists_dict)
        else:
            raise ValueError("unsupported filter type %s" % f.get("meta").get("type"))
    current_app.logger.info("Filter output %s", filter_dict)
    return filter_dict


HEADERS = None
header_lock = threading.Lock()


def get_es_headers(request_headers=None, user=None):
    """

    :param request_headers:
    :param user:
    :return:
    """
    global HEADERS, header_lock

    with header_lock:
        if HEADERS is None:
            # Load HEADERS from the file if requested
            header_file = current_app.config.get("HEADER_FILE")
            if header_file and os.path.exists(header_file):
                try:
                    with open(header_file) as ff:
                        HEADERS = yaml.safe_load(ff)
                    if not isinstance(HEADERS, dict):
                        raise ValueError(
                            f"header YAML file must return a mapping, received {HEADERS}"
                        )
                except (OSError, IOError, ValueError, yaml.YAMLError):
                    current_app.logger.exception(
                        "Failed to load headers from %s", header_file
                    )
                    # in failure, headers are set to empty
                    HEADERS = {}
            else:
                HEADERS = {}

    result = copy.deepcopy(HEADERS)

    # Figure out what headers are allowed to pass-through
    whitelist_headers = current_app.config.get("WHITELIST_HEADERS")
    if whitelist_headers and request_headers:
        for hh in whitelist_headers.split(","):
            if hh in request_headers:
                result[hh] = request_headers[hh]

    # Set runas user based off user provided
    if user:
        result["es-security-runas-user"] = user

    return result


def convert(response):
    """

    :param response:
    :return:
    """
    if hasattr(response.aggregations, "categories"):
        for category in response.aggregations.categories:
            for bucket in category.grids:
                x, y = lnglat_to_meters(
                    bucket.centroid.location.lon, bucket.centroid.location.lat
                )
                yield {
                    "lon": bucket.centroid.location.lon,
                    "lat": bucket.centroid.location.lat,
                    "x": x,
                    "y": y,
                    "c": bucket.centroid.count,
                    "t": str(category.key),
                }
    else:
        for bucket in response.aggregations.grids:
            lon = bucket.centroid.location.lon
            lat = bucket.centroid.location.lat
            x, y = lnglat_to_meters(lon, lat)
            yield {"lon": lon, "lat": lat, "x": x, "y": y, "c": bucket.centroid.count}


def split_fieldname_to_list(field: str) -> List[str]:
    """Remove .raw and .keyword from ``field``

    :param field: Field name to split
    :return: List containing field name
    """
    field = field.split(".")
    # .raw and .keyword are common conventions, but the
    # only way to actually do this right is to lookup the
    # mapping
    if field[-1] in ("raw", "keyword"):
        field.pop()
    return field


def get_nested_field_from_hit(hit, field, default=None):
    """

    :param hit:
    :param field:
    :param default:
    :return:
    """
    # make it safe to call with a string or a list of strings
    if isinstance(field, str):
        field = [field]

    if len(field) == 0:
        raise ValueError("field must be provided")
    elif len(field) == 1:
        return getattr(hit, field[0], default)
    elif len(field) > 1:
        # iterate being careful if the field and the hit are not consistent
        v = hit.to_dict()
        for f in field:
            if isinstance(v, dict) or isinstance(v, AttrDict):
                v = v.get(f, None)
            else:
                return default
        return v
