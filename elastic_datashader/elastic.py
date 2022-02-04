from logging import getLogger
from pathlib import Path
from typing import Any, Dict, List, Optional

import copy
import struct
import pynumeral
import time

from datashader.utils import lnglat_to_meters
from elasticsearch import Elasticsearch
from elasticsearch_dsl import AttrDict, Search

import yaml

from . import mercantile_util as mu
from .config import config

logger = getLogger(__name__)

def to_32bit_float(number):
    return struct.unpack("f", struct.pack("f", float(number)))[0]

def scan(search, use_scroll=False, size=10000):
    # Scroll searches sorted by _doc are faster
    search = search.sort("_doc")
    if use_scroll:
        for hit in search.scan():
            yield hit
    else:
        _search = search.params(size=size).extra(track_total_hits=False)
        while _search is not None:
            hit = None
            for hit in _search:
                yield hit
            if hit is not None:
                _search = search.extra(search_after=list(hit.meta.sort))
            else:
                _search = None


def verify_datashader_indices(elasticsearch_hosts: str):
    """Verify the ES indices exist

    :param elasticsearch_hosts:
    """
    es = Elasticsearch(
        elasticsearch_hosts.split(","),
        verify_certs=False,
        timeout=120
    )

    layer_mapping = {
        "mappings": {
            "properties": {
                "creating_host": {
                    "type": "keyword"
                },
                "creating_pid": {
                    "type": "long"
                },
                "creating_timestamp": {
                    "type": "date"
                },
                "generated_params": {
                    "properties": {
                        "complete": {
                            "type": "boolean"
                        },
                        "generating_host": {
                            "type": "keyword"
                        },
                        "generation_pid": {
                            "type": "long"
                        },
                        "generation_complete_time": {
                            "type": "date"
                        },
                        "generation_start_time": {
                            "type": "date"
                        },
                        "global_bounds": {
                            "type": "long"
                        },
                        "global_doc_cnt": {
                            "type": "long"
                        },
                        "histogram_cnt": {
                            "type": "long"
                        },
                        "histogram_interval": {
                            "type": "float"
                        }
                    }
                },
                "params": {
                    "properties": {
                        "dsl_filter": {
                            "type": "object",
                            "enabled": False
                        },
                        "dsl_query": {
                            "type": "object",
                            "enabled": False
                        }
                    }
                }
            }
        }
    }

    tile_mapping = {
        "mappings": {
            "properties": {
                "params": {
                    "properties": {
                        "dsl_filter": {
                            "type": "object",
                            "enabled": False
                        },
                        "dsl_query": {
                            "type": "object",
                            "enabled": False
                        }
                    }
                }
            }
        }
    }

    es.indices.create(  # pylint: disable=E1123
        index=".datashader_layers",
        body=layer_mapping,
        ignore=400
    )
    es.indices.create(  # pylint: disable=E1123
        index=".datashader_tiles",
        body=tile_mapping,
        ignore=400
    )

def get_search_base(
    elastic_hosts: str,
    headers: Optional[str],
    params: Dict[str, Any],
    idx: int,
) -> Search:
    """

    :param elastic_hosts:
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
        elastic_hosts.split(","),
        verify_certs=False,
        timeout=900,
        headers=get_es_headers(headers, user),
    )

    # Create base search
    base_s = Search(index=idx, using=es)

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

    base_s = base_s.params(ignore_unavailable=True)
    if user:
        base_s = base_s.params(preference=user)

    return base_s


def build_dsl_filter(filter_inputs) -> Optional[Dict[str, Any]]:
    """

    :param filter_inputs:
    :return:
    """
    if len(filter_inputs) == 0:
        return None
    filter_dict = {"filter": [{"match_all": {}}], "must_not": []}

    for f in filter_inputs:
        logger.info("Filter %s\n %s", f.get("meta").get("type"), f)
        # Skip disabled filters
        if f.get("meta").get("disabled") in ("true", True):
            continue

        is_spatial_filter = (
            f.get("meta").get("type") == "spatial_filter" or 
            f.get("geo_polygon") or 
            f.get("geo_bounding_box") or 
            f.get("geo_shape") or
            f.get("geo_distance")
        )
        # Handle spatial filters
        if is_spatial_filter:
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
            elif f.get("geo_shape"):
                geo_bbox_dict = {"geo_shape": f.get("geo_shape")}
                if f.get("meta").get("negate"):
                    filter_dict["must_not"].append(geo_bbox_dict)
                else:
                    filter_dict["filter"].append(geo_bbox_dict)
            elif f.get("geo_distance"):
                geo_bbox_dict = {"geo_distance": f.get("geo_distance")}
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
    logger.info("Filter output %s", filter_dict)
    return filter_dict

def load_datashader_headers(header_file_path_str: Optional[str]) -> Dict[Any, Any]:
    if header_file_path_str is None:
        return {}

    header_file_path = Path(header_file_path_str)

    if not header_file_path.exists():
        return {}

    try:
        loaded_yaml = yaml.safe_load(header_file_path.read_text(encoding='utf8'))
    except (OSError, IOError, yaml.YAMLError) as ex:
        raise Exception(f"Failed to load HEADER_FILE from {header_file_path_str}") from ex

    if type(loaded_yaml) is not dict:
        raise ValueError(f"HEADER_FILE YAML should be a dict mapping, but received {loaded_yaml}")

    return loaded_yaml

def get_es_headers(request_headers=None, user=None):
    """

    :param request_headers:
    :param user:
    :return:
    """

    # Copy so we don't mutate the headers in the config
    result = copy.deepcopy(config.datashader_headers)

    # Figure out what headers are allowed to pass-through
    allowlist_headers = config.allowlist_headers

    if allowlist_headers and request_headers:
        for hh in allowlist_headers.split(","):
            if hh in request_headers:
                result[hh] = request_headers[hh]

    # Set runas user based off user provided
    if user:
        result["es-security-runas-user"] = user

    return result


def convert(response, category_formatter=str):
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
                    "t": category_formatter(category.key),
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
    i = -1
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
    def __init__(self, search, source_aggs, inner_aggs=None, size=10, timeout=None):
        self.search = search
        self.source_aggs = source_aggs
        self.inner_aggs = inner_aggs if inner_aggs is not None else {}
        self.size = size
        self.num_searches = 0
        self.total_took = 0
        self.total_shards = 0
        self.total_skipped = 0
        self.total_successful = 0
        self.total_failed = 0
        self.timeout = timeout
        self.aborted = False

    def execute(self):
        """
        Helper function used to iterate over all possible bucket combinations of
        ``source_aggs``, returning results of ``inner_aggs`` for each. Uses the
        ``composite`` aggregation under the hood to perform this.
        """
        self.num_searches = 0
        self.total_took = 0
        self.aborted = False

        def run_search(**kwargs):
            _timeout_at = kwargs.pop("timeout_at", None)
            s = self.search[:0]
            if _timeout_at:
                _time_remaining = _timeout_at - time.time()
                s = s.params(timeout="%ds" % _time_remaining)
            s.aggs.bucket("comp", "composite", sources=self.source_aggs, size=self.size, **kwargs)

            for agg_name, agg in self.inner_aggs.items():
                s.aggs["comp"][agg_name] = agg

            return s.execute()

        timeout_at = None
        if self.timeout:
            timeout_at = time.time() + self.timeout

        response = run_search(timeout_at=timeout_at)
        self.num_searches += 1
        self.total_took += response.took
        self.total_shards += response._shards.total  # pylint: disable=W0212
        self.total_skipped += response._shards.skipped  # pylint: disable=W0212
        self.total_successful += response._shards.successful  # pylint: disable=W0212
        self.total_failed += response._shards.failed  # pylint: disable=W0212
        
        while response.aggregations.comp.buckets:
            for b in response.aggregations.comp.buckets:
                yield b
            if "after_key" in response.aggregations.comp:
                after = response.aggregations.comp.after_key
            else:
                after = response.aggregations.comp.buckets[-1].key
            
            if timeout_at and time.time() > timeout_at:
                self.aborted = True
                break

            response = run_search(after=after, timeout_at=timeout_at)
            self.num_searches += 1
            self.total_took += response.took
            self.total_shards += response._shards.total  # pylint: disable=W0212
            self.total_skipped += response._shards.skipped  # pylint: disable=W0212
            self.total_successful += response._shards.successful  # pylint: disable=W0212
            self.total_failed += response._shards.failed  # pylint: disable=W0212

def get_tile_categories(base_s, x, y, z, geopoint_field, category_field, size):

    category_filters = {}
    category_legend = {}

    west, south, east, north = mu.bounds(x, y, z)
    bb_dict = {
        "top_left": {
            "lat": min(90, max(-90, north)),
            "lon": min(180, max(-180, west)),
        },
        "bottom_right": {
            "lat": min(90, max(-90, south)),
            "lon": min(180, max(-180, east)),
        },
    }

    cat_s = copy.copy(base_s)
    cat_s = cat_s.params(size=0)
    cat_s = cat_s.filter("geo_bounding_box", **{geopoint_field: bb_dict})
    cat_s.aggs.bucket("categories", "terms", field=category_field, size=size)
    cat_s.aggs.bucket("missing", "filter", bool={ "must_not" : { "exists": { "field": category_field } } })
    response = cat_s.execute()
    if hasattr(response.aggregations, "categories"):
        for category in response.aggregations.categories:
            # this if prevents bools from using 0/1 instead of true/false
            if hasattr(category, "key_as_string"):
                category_filters[str(category.key)] = { "term": {category_field: category.key_as_string} }
            else:
                category_filters[str(category.key)] = { "term": {category_field: category.key} }
            category_legend[str(category.key)] = category.doc_count
        category_legend["Other"] = response.aggregations.categories.sum_other_doc_count
    if hasattr(response.aggregations, "missing") and response.aggregations.missing.doc_count > 0:
        category_filters["N/A"] = { "bool": { "must_not" : { "exists": { "field": category_field } } } }
        category_legend["N/A"] = response.aggregations.missing.doc_count

    return category_filters, category_legend
