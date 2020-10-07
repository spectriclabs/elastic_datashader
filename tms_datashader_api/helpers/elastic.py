#!/usr/bin/env python3
import copy
import logging
import os
import threading
import struct
import mercantile
import pynumeral
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from datashader.utils import lnglat_to_meters
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from elasticsearch_dsl import Search, AttrDict, Index
from flask import current_app, request
import tms_datashader_api.helpers.mercantile_util as mu


def to_32bit_float(number):
    return struct.unpack("f", struct.pack("f", float(number)))[0]

def verify_datashader_indices(elasticsearch_uri: str):
    """Verify the ES indices exist

    :param elasticsearch_uri:
    """
    es = Elasticsearch(
        elasticsearch_uri.split(","),
        verify_certs=False,
        timeout=120
    )
    try:
        Index(".datashader_layers", using=es).create()
    except RequestError:
        logging.debug("Index .datashader_layers already exists, continuing")
    try:
        Index(".datashader_tiles", using=es).create()
    except RequestError:
        logging.debug("Index .datashader_tiles already exists, continuing")

def get_search_base(
    elastic_uri: str,
    params: Dict[str, Any],
    idx: int,
    header_file: Optional[Union[str, Path]] = None,
) -> Search:
    """

    :param elastic_uri:
    :param params:
    :param idx:
    :param header_file:
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
        headers=get_es_headers(header_file, request.headers, user),
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
        elif f.get("meta", {}).get("type") == "custom" and f.get("meta", {}).get("key") is not None:
            filter_key = f.get("meta", {}).get("key")
            if f.get("meta", {}).get("negate"):
                if filter_key == "query":
                    filter_dict["must_not"].append( { "bool": f.get(filter_key).get("bool") } )
                else:
                    filter_dict["must_not"].append( { filter_key: f.get(filter_key) } )
            else:
                if filter_key == "query":
                    filter_dict["filter"].append( { "bool": f.get(filter_key).get("bool") } )
                else:
                    filter_dict["filter"].append( { filter_key: f.get(filter_key) } )
        else:
            raise ValueError("unsupported filter type %s" % f.get("meta").get("type"))
    current_app.logger.info("Filter output %s", filter_dict)
    return filter_dict


HEADERS = None
header_lock = threading.Lock()


def get_es_headers(header_file=None, request_headers=None, user=None):
    """

    :param header_file:
    :param request_headers:
    :param user:
    :return:
    """
    global HEADERS, header_lock

    with header_lock:
        if HEADERS is None:
            if header_file is None:
                header_file = current_app.config.get("HEADER_FILE")
            # Load HEADERS from the file if requested
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

def convert_composite(response, categorical, filter_buckets, histogram_interval, category_type, category_format):
    if categorical and filter_buckets == False:
        # Convert a regular terms aggregation
        for bucket in response:
            for category in bucket.categories:
                lon, lat = geotile_bucket_to_lonlat(bucket)
                x, y = lnglat_to_meters(lon, lat)

                raw = category.key
                # Bin the data
                if histogram_interval is not None:
                    # Format with pynumeral if provided
                    if category_format:
                        label = "%s-%s" % (
                            pynumeral.format(float(raw), category_format),
                            pynumeral.format(float(raw) + histogram_interval, category_format),
                        )
                    else:
                        label = "%s-%s" % (float(raw), float(raw) + histogram_interval)
                else:
                    if category_type == "number":
                        try:
                            label = pynumeral.format(to_32bit_float(raw), category_format)
                        except ValueError:
                            label = str(raw)                        
                    else:
                        label = str(raw)
                yield {
                    "lon": lon,
                    "lat": lat,
                    "x": x,
                    "y": y,
                    "c": category.doc_count,
                    "t": label,
                }
    elif categorical and filter_buckets == True:
        # Convert a filter bucket aggregation
        for bucket in response:
            for key in bucket.categories.buckets:
                category = bucket.categories.buckets[key]
                if category.doc_count > 0:
                    lon, lat = geotile_bucket_to_lonlat(bucket)
                    x, y = lnglat_to_meters(lon, lat)

                    if category_type == "number":
                        try:
                            label = pynumeral.format(to_32bit_float(key), category_format)
                        except ValueError:
                            label = str(key)                        
                    else:
                        label = str(key)

                    yield {
                        "lon": lon,
                        "lat": lat,
                        "x": x,
                        "y": y,
                        "c": category.doc_count,
                        "t": label,
                    }
    else:
        # Non-categorical
        for bucket in response:
            lon, lat = geotile_bucket_to_lonlat(bucket)
            x, y = lnglat_to_meters(lon, lat)
            yield {"lon": lon, "lat": lat, "x": x, "y": y, "c": bucket.doc_count}

def geotile_bucket_to_lonlat(bucket):
    if hasattr(bucket, "centroid"):
        lon = bucket.centroid.location.lon
        lat = bucket.centroid.location.lat
    else:
        z, x, y = [ int(x) for x in bucket.key.grids.split("/") ]
        lon, lat = mu.center(x, y, z)
    return lon, lat

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
        f = ".".join(field)
        if f in v:
            return v.get(f)
        else:
            for f in field:
                if isinstance(v, dict) or isinstance(v, AttrDict):
                    v = v.get(f, None)
                else:
                    return default
        return v

def chunk_iter(iterable, chunk_size):
    chunks = [ None ] * chunk_size
    for i, v in enumerate(iterable):
        idx = (i % chunk_size)
        if idx == 0 and i > 0:
            i = -1
            yield (True, chunks)
        chunks[idx] = v
    
    if i >= 0:
        last_written_idx =( i % chunk_size)
        yield (False, chunks[0:last_written_idx+1])

class ScanAggs(object):
    def __init__(self, search, source_aggs, inner_aggs={}, size=10):
        self.search = search
        self.source_aggs = source_aggs
        self.inner_aggs = inner_aggs
        self.size = size
        self.num_searches = 0
        self.total_took = 0

    def execute(self):
        """
        Helper function used to iterate over all possible bucket combinations of
        ``source_aggs``, returning results of ``inner_aggs`` for each. Uses the
        ``composite`` aggregation under the hood to perform this.
        """
        self.num_searches = 0
        self.total_took = 0

        def run_search(**kwargs):
            s = self.search[:0]
            s.aggs.bucket("comp", "composite", sources=self.source_aggs, size=self.size, **kwargs)
            for agg_name, agg in self.inner_aggs.items():
                s.aggs["comp"][agg_name] = agg
            try:
                return s.execute()
            except:
                print(s.to_dict())
                raise

        response = run_search()
        self.num_searches += 1
        while response.aggregations.comp.buckets:
            num_buckets = 0
            for b in response.aggregations.comp.buckets:
                num_buckets += 1
                yield b
            if "after_key" in response.aggregations.comp:
                after = response.aggregations.comp.after_key
            else:
                after = response.aggregations.comp.buckets[-1].key
            # If we got fewer buckets than requested, no reason to ask for more
            #if num_buckets < self.size:
            #    break
            response = run_search(after=after)
            self.num_searches += 1
            self.total_took += response.took
            num_buckets = 0

def get_tile_categories(base_s, x, y, z, geopoint_field, category_field, size):

    category_filters = {}
    category_legend = {}

    bounds = mercantile.bounds(x, y, z)
    bb_dict = {
        "top_left": {
            "lat": min(90, max(-90, bounds.north)),
            "lon": min(180, max(-180, bounds.west)),
        },
        "bottom_right": {
            "lat": min(90, max(-90, bounds.south)),
            "lon": min(180, max(-180, bounds.east)),
        },
    }

    cat_s = copy.copy(base_s)
    cat_s = cat_s.params(size=0)
    cat_s = cat_s.filter("geo_bounding_box", **{geopoint_field: bb_dict})
    cat_s.aggs.bucket("categories", "terms", field=category_field, size=size)
    response = cat_s.execute()
    for ii, category in enumerate(response.aggregations.categories):
        category_filters[str(category.key)] = { "term": {category_field: category.key} }
        category_legend[str(category.key)] = category.doc_count
    category_legend["Other"] = response.aggregations.categories.sum_other_doc_count

    return category_filters, category_legend