#!/usr/bin/env python

from flask import Flask, Response, current_app
from flask import request, render_template, redirect
from flask import Blueprint
from flask_apscheduler import APScheduler

import time
import math
import io
import os
import pathlib
import copy
import argparse
import logging
import yaml
import hashlib
import subprocess
import shutil
import threading
import copy
import collections
import png
import socket
import urllib3
import json
import fcntl
from functools import lru_cache
import ssl
from datetime import datetime, timedelta
from pprint import pprint, pformat
import pynumeral

from numba import jit
from numpy import linspace, pi, sin, cos 
from PIL import Image, ImageDraw
import mercantile

import datashader as ds
import pandas as pd
import colorcet as cc
import datashader.transfer_functions as tf
import datashader.reductions as rd
from datashader_helpers import sum_cat

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.aggs import Bucket
from elasticsearch_dsl.utils import AttrList, AttrDict






#Disable warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)
urllib3.disable_warnings(urllib3.exceptions.SNIMissingWarning)
urllib3.disable_warnings(UserWarning)

#Logging for non-Flask items
logging.basicConfig(level=logging.INFO)
logging.getLogger("elasticsearch").setLevel(logging.WARN)
logging.getLogger("urllib3").setLevel(logging.WARN)

# Preconfigured tile size
tile_height_px = 256
tile_width_px = 256

class GeotileGrid(Bucket):
    name = 'geotile_grid'

class Config(object):
    """
    The default configuration; configuration parameters need
    to be in all upper case to be loaded correctly by
    the flask helpers
    """

    # Configuration that can be modifed by the user
    LOG_LEVEL =   os.environ.get("DATASHADER_LOG_LEVEL", None)
    CACHE_DIRECTORY =   os.environ.get("DATASHADER_CACHE_DIRECTORY", "./tms-cache/")
    CACHE_TIMEOUT = int(os.environ.get("DATASHADER_CACHE_TIMEOUT", 60*60))
    ELASTIC = os.environ.get("DATASHADER_ELASTIC", "http://localhost:9200")
    PROXY_HOST = os.environ.get("DATASHADER_PROXY_HOST", None)
    PROXY_PREFIX = os.environ.get("DATASHADER_PROXY_PREFIX", "")
    TMS_KEY = os.environ.get("DATASHADER_TMS_KEY", None)
    MAX_BINS = int(os.environ.get("DATASHADER_MAX_BINS", 10000))
    MAX_BATCH = int(os.environ.get("DATASHADER_MAX_BATCH", 10000))
    MAX_ELLIPSES_PER_TILE = int(os.environ.get("DATASHADER_MAX_BATCH", 100000))
    HEADER_FILE = os.environ.get("DATASHADER_HEADER_FILE", "./headers.yaml")
    WHITELIST_HEADERS = os.environ.get("DATASHADER_WHITELIST_HEADERS", None)
    DEBUG_TILES = os.environ.get("DATASHADER_DEBUG_TILES", False)
    PORT = None
    HOSTNAME = socket.getfqdn()

# Globals
_color_key_map = []
#config_lock = threading.Lock()

##############################################################################
# API
##############################################################################

api = Blueprint('rest_api', __name__, template_folder='templates')

@api.route('/')
@api.route('/index')
def index():
    #Calc Cache Size
    cache_size = subprocess.check_output(['du','-sh', current_app.config["CACHE_DIRECTORY"]]).split()[0].decode('utf-8')
    #Build Layer Info
    layer_info = {}
    layers = os.listdir(current_app.config["CACHE_DIRECTORY"])
    for l in layers:
        if not os.path.isfile(current_app.config["CACHE_DIRECTORY"]+l):
            hashes = os.listdir(current_app.config["CACHE_DIRECTORY"]+l+"/")
            for h in hashes:
                if os.path.exists(current_app.config["CACHE_DIRECTORY"]+l+"/"+h+"/params.json"):
                    with open(current_app.config["CACHE_DIRECTORY"]+l+'/'+h+"/params.json") as f:
                        params = json.loads(f.read())
                    #Check age of hash
                    params['age_timestamp'] = os.path.getmtime(current_app.config["CACHE_DIRECTORY"]+l+"/"+h+"/params.json")
                    params['age'] = pretty_time_delta(time.time()-params['age_timestamp'])
                    #Check size of hash
                    try:
                        params['size'] = subprocess.check_output(['du','-sh', current_app.config["CACHE_DIRECTORY"]+l+"/"+h]).split()[0].decode('utf-8')
                    except OSError:
                        params['size'] = "Error"
                    if layer_info.get(l) == None:
                        layer_info[l] = collections.OrderedDict()
                    layer_info[l][h] = params
            
            #Order hashes based off age, newest to oldest
            if layer_info.get(l):
                layer_info[l] = collections.OrderedDict(reversed(sorted(layer_info[l].items(), key=lambda x: x[1]['age_timestamp'])))
    return render_template('index.html', title='Elastic Data Shader Server', cache_size=cache_size, layer_info=layer_info)

@api.route('/parameters', methods=['GET'])
def display_parameters():
    color_file = os.path.join(current_app.config["CACHE_DIRECTORY"]+"/%s/%s-colormap.json"%(request.args.get('name'), request.args.get('field')))
    
    #Build Layer Info
    layer_info = {}
    layers = os.listdir(current_app.config["CACHE_DIRECTORY"])
    for l in layers:
        if l == request.args.get('name'):
            if not os.path.isfile(current_app.config["CACHE_DIRECTORY"]+l):
                hashes = os.listdir(current_app.config["CACHE_DIRECTORY"]+l+"/")
                for h in hashes:
                    if h == request.args.get('hash'):
                        if os.path.exists(current_app.config["CACHE_DIRECTORY"]+l+"/"+h+"/params.json"):
                            with open(current_app.config["CACHE_DIRECTORY"]+l+'/'+h+"/params.json") as f:
                                params = json.loads(f.read())
                                generated_params = pformat(params.get("generated_params", {}))
                                return render_template('parameters.html', title='Elastic Data Shader Server', params=params, generated_params=generated_params, name=request.args.get('name'), hash=request.args.get('hash'))
    return render_template('parameters.html', title='Elastic Data Shader Server', params={}, name=request.args.get('name'), hash=request.args.get('hash'))

@api.route('/clear_cache', methods=['GET'])
def clear_cache():
    if request.args.get('name') is not None:
        #delete a specific cache
        tile_cache_path = os.path.join(current_app.config.get("CACHE_DIRECTORY"), request.args.get('name'))
        if request.args.get('hash') is not None:
            tile_cache_path = os.path.join(current_app.config.get("CACHE_DIRECTORY"), request.args.get('name'), request.args.get('hash'))
        
        #Check if it exists
        if os.path.exists(tile_cache_path):
            shutil.rmtree(tile_cache_path)
            current_app.logger.info("Clearing hash/layer : %s"%(tile_cache_path))
        
        #Not needed with the hashing approach?
        #current_app.logger.warn("Recreating cache path %s", tile_cache_path)
        #pathlib.Path(os.path.join(tile_cache_path)).mkdir(parents=True, exist_ok=True)
        
        return redirect(request.referrer)   
    return Response("Unknown request: %s / %s"%(request.args.get('name'), request.args.get('hash')), status=500)

@api.route('/age_cache', methods=['GET'])
def age_cache():
    #Either the index name or age must be set.  We do not allow blanket deletes  
    if request.args.get('age') is not None:
        age_limit = int(request.args.get('age'))
        cache_dir = current_app.config["CACHE_DIRECTORY"]
        check_cache_age(cache_dir, age_limit)
        return redirect(request.referrer)   
    return Response("Unknown request: %s / %s"%(request.args.get('name'), request.args.get('hash')), status=500)

@api.route('/<idx>/<field_name>/legend.json', methods=['GET'])
def provide_legend(idx, field_name):
    #Extract out special extent parameter that is independent from hash
    extent = None
    params = request.args.get('params')
    if params and params != '{params}':
        params = json.loads(request.args.get('params'))
        if params.get("extent"):
            extent = params.get("extent")

    #Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request)
    except Exception as e:
        current_app.logger.exception("Error while extracting parameters")
        resp = Response("[]", status=200)
        resp.headers['Content-Type'] = 'application/json'
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Error'] = str(e)
        resp.cache_control.max_age = 60
        return resp
    
    #If not in category mode, just return nothing
    if params["category_field"] == None:
        resp = Response("[]", status=200)
        resp.headers['Content-Type'] = 'application/json'
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.cache_control.max_age = 60
        return resp

    #Get or generate extended parameters
    paramsfile = os.path.join(current_app.config["CACHE_DIRECTORY"], idx, "%s/params.json"%(parameter_hash))
    params = merge_generated_parameters(params, paramsfile, idx)

    #Assign param value to legacy keyword values
    geopoint_field = params["geopoint_field"]
    category_type = params["category_type"]
    category_format = params["category_format"]
    cmap = params["cmap"]
    histogram_interval=params.get("generated_params", {}).get("histogram_interval", None)

    #Get search object
    base_s = get_search_base(params, idx)
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
            }
        }
        current_app.logger.info("legend_bbox: %s", legend_bbox)
        legend_s = legend_s.filter("geo_bounding_box",
            **{
                geopoint_field: legend_bbox
            }  )

    max_legend_categories = 50
    if histogram_interval != None:
        #Put in the histogram search
        legend_s.aggs.bucket(
                        'categories',
                        'histogram',
                        field=field_name,
                        interval=histogram_interval,
                        min_doc_count=1
                    )
    else:
        #Non-histogram legend
        legend_s.aggs.bucket(
            'categories',
            'terms',
            field=field_name,
            size=max_legend_categories
        )
    # Perform the execution
    response = legend_s.execute()
    #If no categories then return blank list
    if not hasattr(response.aggregations, 'categories'):
        resp = Response("[]", status=200)
        resp.headers['Content-Type'] = 'application/json'
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.cache_control.max_age = 60
        return resp

    # Generate the legend list
    color_key_legend = []
    for category in response.aggregations.categories:
        if histogram_interval and category_type == "number":
            #Bin the data
            raw = float(category.key)
            #Format with pynumeral if provided
            if category_format:
                k = "%s-%s"%(pynumeral.format(raw, category_format), pynumeral.format(raw+histogram_interval, category_format))
            else:
                k = "%s-%s"%(raw, raw+histogram_interval)
        else:
            k = str(category.key)
        c = create_color_key([str(category.key)], cmap=cmap).get(str(category.key), "#000000")
        color_key_legend.append(dict(key=k, color=c, count=category.doc_count))

    data = json.dumps(color_key_legend)
    resp = Response(data, status=200)
    resp.headers['Content-Type'] = 'application/json'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.cache_control.max_age = 60
    return resp

@api.route('/tms/<idx>/<int:z>/<int:x>/<int:y>.png', methods=['GET'])
def get_tms(idx, x, y, z):

    #Validate request is from proxy if proxy mode is enabled
    if current_app.config.get("TMS_KEY") is not None:
        if current_app.config.get("TMS_KEY") != request.headers.get("TMS_PROXY_KEY"):
            current_app.logger.warning("TMS must be accessed via reverse proxy: keys %s != %s", current_app.config.get("TMS_KEY"), request.headers.get("TMS_PROXY_KEY"))
            resp = Response("TMS must be accessed via reverse proxy", status=403)
            return resp

    # TMS tile coordinates
    x = int(x)
    y = int(y)
    z = int(z)

    #Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request)
    except Exception as e:
        current_app.logger.exception("Error while extracting parameters")
        img = gen_error(tile_height_px, tile_width_px)
        resp = Response(img, status=200)
        resp.headers['Content-Type'] = 'image/png'
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Error'] = str(e)
        resp.cache_control.max_age = 60
        return resp

    #Check if the cached image already exists
    c = get_cache( "/%s/%s/%s/%s/%s.png"%(idx, parameter_hash, z, x, y), current_app.config["CACHE_DIRECTORY"])
    if c is not None and request.args.get('force') is None:
        current_app.logger.info("Hit cache (%s), returning"%parameter_hash)
        #Return Cached Value
        img = c
    else:
        #Generate a tile
        if request.args.get('force') is not None:
            current_app.logger.info("Forced cache flush, generating a new tile %s/%s/%s"%(z,x,y))
        else:
            current_app.logger.info("No cache (%s), generating a new tile %s/%s/%s"%(parameter_hash,z,x,y))
        
        check_cache_dir(idx)

        headers = get_es_headers(request.headers)
        current_app.logger.info("Loaded elasticsearch headers %s", headers)

        #Get or generate extended parameters
        paramsfile = os.path.join(current_app.config["CACHE_DIRECTORY"], idx, "%s/params.json"%(parameter_hash))
        params = merge_generated_parameters(params, paramsfile, idx)
        #Separate call for ellipse
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
            resp.headers['Content-Type'] = 'image/png'
            resp.headers['Access-Control-Allow-Origin'] = '*'
            resp.headers['Error'] = str(e.args)
            resp.cache_control.max_age = 60
            return resp
        
        #Store image as well
        set_cache("/%s/%s/%s/%s/%s.png"%(idx, parameter_hash, z, x, y), img, current_app.config["CACHE_DIRECTORY"])

    resp = Response(img, status=200)
    resp.headers['Content-Type'] = 'image/png'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.cache_control.max_age = 60
    return resp

###########################################################################
# Utility Functions
###########################################################################

def extract_parameters(request):
    #Get the parameters from a request and return hash and dict of parameters

    #Default values
    from_time = None
    to_time = "now"
    params = {
        "geopoint_field": None,
        "timestamp_field": "@timestamp",
        
        "lucene_query": None,
        "dsl_query": None,
        "dsl_filter": None,
        
        "cmap": None,
        "category_field": None,
        "category_type": None,
        "category_format": None,
        
        "ellipses": False,
        "ellipse_major": "",
        "ellipse_minor": "",
        "ellipse_tilt": "",
        "ellipse_units": "",
        "ellipse_max_cep": 50,
        
        "spread": None,
        "span_range": None,
        "resolution": "finest",

        #Config items that we pass for ease
        "max_bins":  int(current_app.config["MAX_BINS"]),
        "max_batch": int(current_app.config["MAX_BATCH"]),
        "max_ellipses_per_tile":  int(current_app.config["MAX_ELLIPSES_PER_TILE"])
    }

    #Argument Parameter, NB. These overwrite what is in index config
    arg_params = request.args.get('params')
    if arg_params and arg_params != '{params}':
        arg_params = json.loads(request.args.get('params'))
        if arg_params.get("timeFilters",{}).get("from"):
            from_time = arg_params.get("timeFilters",{}).get("from")
        if arg_params.get("timeFilters",{}).get("to"):
            to_time = arg_params.get("timeFilters",{}).get("to")
        if arg_params.get("filters"):
            params["dsl_filter"] = build_dsl_filter(arg_params.get("filters"))
        if arg_params.get("query") and arg_params.get("query", {}).get("language", None) in ("lucene", "kuery"):
            # accept 'kuery' for backwords compatibility...
            params["lucene_query"] = arg_params.get("query").get("query")
        elif arg_params.get("query") and arg_params.get("query", {}).get("language", None) == "dsl":
            params["dsl_query"] = arg_params.get("query").get("query")
    elif arg_params and arg_params == '{params}':
        #If the parameters haven't been provided yet
        resp = Response("TMS parameters not yet provided", status=204)
        return resp

    # Custom parameters can be provided by the URL
    params["ellipses"] = request.args.get('ellipses', default=params["ellipses"])
    if params["ellipses"] == "false" or params["ellipses"]=="False":
        params["ellipses"] = False
    else:
        #Handle the other fields
        params["ellipse_major"] = request.args.get('ellipse_major', default="")
        params["ellipse_minor"] = request.args.get('ellipse_minor', default="")
        params["ellipse_tilt"] = request.args.get('ellipse_tilt', default="")
        params["ellipse_units"] = request.args.get('ellipse_units', default="")
        if params["ellipse_major"] == "" or params["ellipse_major"] == "" or params["ellipse_major"] == "":
            params["ellipses"] = False
    if request.args.get('ellipse_search', default="") == "narrow":
        params["ellipse_max_cep"] = 1.0
    elif request.args.get('ellipse_search', default="") == "normal":
        params["ellipse_max_cep"] = 10.0
    elif request.args.get('ellipse_search', default="") == "wide":
        params["ellipse_max_cep"] = 50.0

    params["category_field"] = request.args.get('category_field', default=params["category_field"])
    params["category_format"] = request.args.get('category_pattern', default=params["category_format"])
    params["category_type"] = request.args.get('category_type', default=params["category_type"])
    params["spread"] = request.args.get('spread')
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
    params["resolution"] = request.args.get('resolution', default=params['resolution'])

    params["cmap"] = request.args.get('cmap', default=params["cmap"])
    if params["cmap"] == None:
        if params["category_field"] == None:
            params["cmap"] = "bmy"
        else:
            params["cmap"] = "glasbey_category10"

    params["span_range"] = request.args.get('span', default="auto")
    params["geopoint_field"] = request.args.get('geopoint_field', default=params["geopoint_field"])
    params["timestamp_field"] = request.args.get('timestamp_field', default=params["timestamp_field"])

    # Handle dumb javascript on the client side
    if params["category_field"] == "null":
        params["category_field"] = None

    #Handle time bounding
    now = datetime.utcnow()   
    params["stop_time"] = now
    if to_time:
        try:
            params["stop_time"] = convertKibanaTime(to_time, now)
        except ValueError:
            current_app.logger.exception("invalid to_time parameter")
            raise Exception('invalid to_time parameter') 

    params["start_time"] = None
    if from_time:
        try:
            params["start_time"] = convertKibanaTime(from_time, now)
        except ValueError:
            current_app.logger.exception("invalid from_time parameter")
            raise Exception('invalid from_time parameter') 

    params["start_time"], params["stop_time"] = quantizeTimeRange(params["start_time"], params["stop_time"])

    if params["geopoint_field"] is None:
        current_app.logger.error("missing geopoint_field")
        raise Exception('missing geopoint_field') 
        

    #Calculate a hash value for the specific parameter set
    parameter_hash = hashlib.md5()
    for k, p in sorted(params.items()):
        if isinstance(p, datetime):
                p = p.isoformat()
        parameter_hash.update(str(p).encode("utf-8"))
    parameter_hash = parameter_hash.hexdigest()
    
    current_app.logger.debug("Parameters: %s (%s)", params, parameter_hash)
    return parameter_hash, params


def generate_global_params(params, idx):  
    geopoint_field=params["geopoint_field"]
    timestamp_field=params["timestamp_field"]
    start_time=params["start_time"]
    stop_time=params["stop_time"]
    category_field=params["category_field"]
    category_type=params["category_type"]
    spread=params["spread"]
    span_range=params["span_range"]
    lucene_query=params["lucene_query"]
    dsl_query=params["dsl_query"]
    dsl_filter=params["dsl_filter"]

    histogram_range = 0
    histogram_interval = None
    global_doc_cnt = None
    global_bounds = None
    
    #Create base search 
    base_s = get_search_base(params, idx)

    #west, south, east, north
    global_bounds = [ -180, -90, 180, 90 ]
    global_doc_cnt = 0

    bounds_s = copy.copy(base_s)
    bounds_s = bounds_s.params(size=0)

    # We only need to do a global query if we are in span 'auto' or
    # using a numeric category    

    # if span_range is auto we need to estimate the density
    if span_range == None or span_range == "auto":
        #See how far the data spans and how many points are in it
        bounds_s.aggs.metric(
            'viewport','geo_bounds',field=geopoint_field
        ).metric(
            'point_count','value_count',field=geopoint_field
        )
    #If the field is a number, we need to figure out it's min/max globally
    if category_type == "number":
        bounds_s.aggs.metric(
            'field_stats', 'stats', field=category_field
        )

    #Execute and process search
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
        if category_type == "number":
            current_app.logger.info("Generating histogram parameters")
            if hasattr(bounds_resp.aggregations, 'field_stats'):
                current_app.logger.info("field stats %s", bounds_resp.aggregations.field_stats)
                # to prevent strain on the cluster, if there are over 1million
                # documents given the current parameters, reduce the number of histogram
                # bins.  Note this is kinda a wag...maybe something smarter can be done
                if global_doc_cnt > 100000:
                    category_cnt = 200
                else:
                    category_cnt = 500
                # determine the range of category values
                if (bounds_resp.aggregations.field_stats.count > 0):
                    if (bounds_resp.aggregations.field_stats.max is None):
                        histogram_range = 0
                    elif (bounds_resp.aggregations.field_stats.min is None):
                        histogram_range = 0
                    else:
                        histogram_range = (bounds_resp.aggregations.field_stats.max - bounds_resp.aggregations.field_stats.min)
                        if histogram_range > 0:
                            # round to the nearest larger power of 10
                            histogram_range = math.pow(10, math.ceil(math.log10(histogram_range)))
                            histogram_interval = histogram_range / category_cnt
                            current_app.logger.info("histogram interval %s, category_cnt: %s ", histogram_interval, category_cnt)
                        else:
                            histogram_range = 0
    else:
        current_app.logger.debug("Skipping global query")

    #Return generated params dict
    generated_params = {}
    generated_params["histogram_interval"] = histogram_interval
    generated_params["global_doc_cnt"] = global_doc_cnt
    generated_params["global_bounds"] = global_bounds

    return generated_params

def merge_generated_parameters(params, paramsfile, idx):
    #Lock and open file
    pathlib.Path(os.path.dirname(os.path.join(paramsfile))).mkdir(parents=True, exist_ok=True) 
    generated_params = None
    with open(paramsfile+".lock", 'w') as lockfile:
        fcntl.flock(lockfile, fcntl.LOCK_EX)
        try:
            if os.path.exists(os.path.join(paramsfile)) == True:
                current_app.logger.warn("Found parameters file, using generated params from that")
                #Params file exists so read it in
                with open(paramsfile, 'r') as stream:
                    full_params = json.load(stream)
                #update timestamp for cache cleanup purposes
                os.utime(os.path.join(paramsfile))
                generated_params = full_params.get("generated_params", None)
            
            if generated_params == None:
                current_app.logger.warn("Discovering generated params")
                #Params file either does not exists or does not have generated parameters in it
                generated_params = generate_global_params(params, idx)
                #Write extended params to file
                params_cleaned = copy.copy(params)
                params_cleaned["generated_params"] = generated_params
                #Change all datetimes to string format
                for k, p in sorted(params_cleaned.items()):
                    if isinstance(p, datetime):
                            params_cleaned[k] = p.isoformat()
                with open(os.path.join(paramsfile), 'w') as i:
                    i.write(json.dumps(params_cleaned))
        finally:
            fcntl.lockf(lockfile, fcntl.LOCK_UN)

    params["generated_params"] = generated_params
    return params

def get_search_base(params, idx):
    timestamp_field=params["timestamp_field"]
    start_time=params["start_time"]
    stop_time=params["stop_time"]
    lucene_query=params["lucene_query"]
    dsl_query=params["dsl_query"]
    dsl_filter=params["dsl_filter"]
    
    # Connect to Elasticsearch
    es = Elasticsearch(
        current_app.config.get("ELASTIC"),
        verify_certs=False,
        timeout=900,
        headers=get_es_headers(request)
    )
    #Create base search 
    base_s = Search(index=idx).using(es)
    #Add time bounds
    #Handle time calculations
    time_range = None
    if timestamp_field:
        time_range = { timestamp_field: {} }
        if start_time != None:
            time_range[timestamp_field]["gte"] = start_time
        if stop_time != None:
            time_range[timestamp_field]["lte"] = stop_time
    if time_range and time_range[timestamp_field]:
        current_app.logger.info("TIME RANGE: %s", time_range)
        base_s = base_s.filter("range", **time_range)

    #Add lucene query
    if lucene_query:
        base_s = base_s.filter('query_string', query=lucene_query)
    

    #Add dsl filtering
    if dsl_filter or dsl_query:
        #Need to convert to a dict, merge with filters then convert back to a search object
        base_dict = base_s.to_dict()
        # setup an empty filter list if necessary
        if base_dict.get("query",{}).get("bool",{}).get("filter") == None:
            base_dict["query"]["bool"]["filter"] = []
        # Add the dsl_query
        if dsl_query:
            base_dict["query"]["bool"]["filter"].append(dsl_query)
        # add dsl_filters
        if dsl_filter:
            for f in dsl_filter["filter"]:
                base_dict["query"]["bool"]["filter"].append(f)
            if base_dict.get("query",{}).get("bool",{}).get("must_not") == None:
                base_dict["query"]["bool"]["must_not"] = []
            for f in dsl_filter["must_not"]:
                base_dict["query"]["bool"]["must_not"].append(f)
        # convert back          
        base_s = Search.from_dict(base_dict)          
        base_s = base_s.index(idx).using(es)
    
    return base_s

def build_dsl_filter(filter_inputs):
    if len(filter_inputs) == 0:
        return None
    filter = {"filter":[{"match_all":{}}], "must_not":[]}
    
    for f in filter_inputs:
        current_app.logger.info("Filter %s\n %s", f.get("meta").get("type"), f)
        # Skip disabled filters
        if f.get("meta").get("disabled") in ("true", True):
            continue

        #Handle spatial filters
        if f.get("meta").get("type") == "spatial_filter":
            if f.get("geo_polygon"):
                if f.get("meta").get("negate"):
                    filter["must_not"].append(dict(geo_polygon=f.get("geo_polygon")))
                else:
                    filter["filter"].append(dict(geo_polygon=f.get("geo_polygon")))
            elif f.get("geo_bounding_box"):
                if f.get("meta").get("negate"):
                    filter["must_not"].append(dict(geo_bounding_box=f.get("geo_bounding_box")))
                else:
                    filter["filter"].append(dict(geo_bounding_box=f.get("geo_bounding_box")))
        #Handle phrase matching
        elif f.get("meta").get("type") in ("phrase", "phrases", "bool"):
            if f.get("meta").get("negate"):
                filter["must_not"].append( f.get("query"))
            else:
                filter["filter"].append(f.get("query"))
        elif f.get("meta").get("type") == "range":
            if f.get("meta").get("negate"):
                filter["must_not"].append(dict(range=f.get("range")))
            else:
                filter["filter"].append(dict(range=f.get("range")))
        elif f.get("meta").get("type") == "exists":
            if f.get("meta").get("negate"):
                filter["must_not"].append(dict(exists=f.get("exists")))
            else:
                filter["filter"].append(dict(exists=f.get("exists")))
        else:
            raise ValueError("unsupported filter type %s", f.get("meta").get("type"))
    current_app.logger.info("Filter output %s", filter)
    return filter

def quantizeTimeRange(start_time, stop_time):
    #Goal here is to quantize the start and end times so when Kibana uses "now" we do not constantly invalidate cache
    if stop_time is None:
        raise ValueError('stop time must be provided')
    
    #If the range is all time, jsut truncate to rayday
    if start_time == None:
        stop_time = stop_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_time, stop_time

    #Calculate the span
    delta_time = stop_time - start_time

    if delta_time > timedelta(days=29):
        #delta > 29 days, truncate to rayday
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        stop_time = stop_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_time, stop_time
    elif delta_time > timedelta(days=1):
        #More than a day, truncate to an hour
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        stop_time = stop_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_time, stop_time
    else:
        #truncate to 5 min
        start_time = start_time.replace(minute=math.floor(start_time.minute/5.0)*5, second=0, microsecond=0)
        stop_time = stop_time.replace(minute=math.floor(stop_time.minute/5.0)*5, second=0, microsecond=0)
        return start_time, stop_time

def convertKibanaTime(time_string, current_time):
    if time_string.startswith("now"):
        if time_string == "now":
            return current_time
        elif time_string.startswith("now-"):
            offset = time_string.split('-')[1]
            unit = offset[-1]
            value = int(offset[0:-1])
            if unit == 's':
                return current_time - timedelta(seconds=value)
            elif unit == 'm':
                return current_time - timedelta(minutes=value)
            elif unit == 'h' or unit == 'H':
                return current_time - timedelta(hours=value)
            elif unit == 'd':
                return current_time - timedelta(days=value)
            elif unit == 'w':
                return current_time - timedelta(weeks=value)
            elif unit == 'M':
                return current_time - timedelta(days=value*30) #Kind of a hack
            elif unit == "y":
                return current_time - timedelta(days=value*365) #Kind of a hack
            else:
                raise ValueError("%s is not a valid time offset" % unit)
        elif time_string.startswith("now+"):
            raise ValueError("now+ time strings are not currently supported")
    elif time_string[10] == 'T':
        # fromisoformat doesn't support the 'Z'
        if time_string[-1] == 'Z':
            time_string = time_string[:-1]

        try:
             t = datetime.fromisoformat(time_string)
             return t
        except ValueError:
            raise ValueError("error parsing isoformat time %s", time_string)
    
    raise ValueError("unknown time string %s" % time_string)

def pretty_time_delta(seconds):
    sign_string = '-' if seconds < 0 else ''
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%s%dd%dh%dm%ds' % (sign_string, days, hours, minutes, seconds)
    elif hours > 0:
        return '%s%dh%dm%ds' % (sign_string, hours, minutes, seconds)
    elif minutes > 0:
        return '%s%dm%ds' % (sign_string, minutes, seconds)
    else:
        return '%s%ds' % (sign_string, seconds)

def get_cache(tile, cache_dir, lifespan=60*60):
    #See if tile exists
    if os.path.exists(os.path.join(cache_dir+tile)):
        with open(os.path.join(cache_dir+tile), 'rb') as i:
            return i.read()
    return None

def set_cache(tile, img, cache_dir):
    pathlib.Path(os.path.dirname(os.path.join(cache_dir+tile))).mkdir(parents=True, exist_ok=True) 
    with open(os.path.join(cache_dir+tile), 'wb') as i:
            i.write(img)

def check_cache_dir(layer_name):
    tile_cache_path = os.path.join(current_app.config.get("CACHE_DIRECTORY"), layer_name)
    if not os.path.exists(tile_cache_path):
        pathlib.Path(os.path.join(tile_cache_path)).mkdir(parents=True, exist_ok=True)

def check_cache_age(cache_dir, age_limit):
    layers = os.listdir(cache_dir)
    for l in layers:
        if not os.path.isfile(cache_dir+l):
            hashes = os.listdir(cache_dir+l+"/")
            for h in hashes:
                if os.path.exists(cache_dir+l+"/"+h+"/params.json"):
                    #Check age of hash
                    age_timestamp = time.time() - os.path.getmtime(cache_dir+l+'/'+h+"/params.json")
                    if age_timestamp > age_limit:
                        shutil.rmtree(cache_dir+l+"/"+h)
                        logging.info("Removing hash due to age: %s (%s>%s)"%(cache_dir+l+"/"+h, age_timestamp, age_limit))

def convert(response):
    if hasattr(response.aggregations, 'categories'):
        for category in response.aggregations.categories:
            for bucket in category.grids:
                x,y = ds.utils.lnglat_to_meters(bucket.centroid.location.lon, bucket.centroid.location.lat)
                yield dict(
                    lon=bucket.centroid.location.lon,
                    lat=bucket.centroid.location.lat,
                    x=x,
                    y=y,
                    c=bucket.centroid.count,
                    t=str(category.key)

                )
    else:
        for bucket in response.aggregations.grids:
            x,y = ds.utils.lnglat_to_meters(bucket.centroid.location.lon, bucket.centroid.location.lat)
            yield dict(
                lon=bucket.centroid.location.lon,
                lat=bucket.centroid.location.lat,
                x=x,
                y=y,
                c=bucket.centroid.count
            )


def create_color_key(categories, cmap='glasbey_category10'):
    color_key = {}
    for k in set(categories):
        color_key[k] = cc.palette[cmap][int(hashlib.md5(k.encode('utf-8')).hexdigest()[0:2], 16)]
    return color_key

HEADERS = None
header_lock = threading.Lock()
def get_es_headers(request_headers=None):
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
                            raise ValueError("header YAML file must return a mapping, received %s", HEADERS)
                except:
                    current_app.logger.exception("Failed to load headers from %s", header_file)
                    # in failure, headers are set to empty
                    HEADERS = {}

    result = copy.deepcopy(HEADERS)

    # Figure out what headers are allowed to pass-through
    whitelist_headers = current_app.config.get("WHITELIST_HEADERS")
    if whitelist_headers and request_headers:
        for hh in whitelist_headers.split(","):
            if hh in request_headers:
                result[hh] = request_headers[hh]

    return result

@lru_cache(10)
def gen_overlay_img(width, height, thickness):
    """
    Create an overlay hash image, using an lru_cache since the same
    overlay can be generated once and then reused indefinately
    """
    overlay = Image.new('RGBA', (width, height))
    draw = ImageDraw.Draw(overlay)
    color = (255,0,0,64)
    for s in range(0, max(height,width), thickness*2):
        draw.line( [(s-width,s+height), (s+width,s-height)], color, thickness)
    return overlay

@lru_cache(10)
def gen_debug_img(width, height, text, thickness=2):
    """
    Create an overlay hash image, using an lru_cache since the same
    overlay can be generated once and then reused indefinately
    """
    overlay = Image.new('RGBA', (width, height))
    draw = ImageDraw.Draw(overlay)
    color = (0,0,0,127)
    draw.rectangle( [0, 0, width, height], outline=color, width=thickness)
    draw.text( [10, 10], text, fill=color)
    return overlay

def gen_overlay(img, thickness=8):
    base = Image.open(io.BytesIO(img))
    overlay = gen_overlay_img(*base.size, thickness=thickness)
    out = Image.alpha_composite(base, overlay)
    with io.BytesIO() as output:
        out.save(output, format='PNG')
        return output.getvalue()

def gen_debug_overlay(img, text):
    base = Image.open(io.BytesIO(img))
    overlay = gen_debug_img(*base.size, text)
    out = Image.alpha_composite(base, overlay)
    with io.BytesIO() as output:
        out.save(output, format='PNG')
        return output.getvalue()

@lru_cache(10)
def gen_error(width, height, thickness=8):
    overlay = Image.new('RGBA', (width, height))
    draw = ImageDraw.Draw(overlay)
    color = (255,0,0,255)
    draw.line( [(0,0), (width,height)], color, thickness)
    draw.line( [(width,0), (0,height)], color, thickness)

    with io.BytesIO() as output:
        overlay.save(output, format='PNG')
        return output.getvalue()

@lru_cache(10)
def gen_empty(width, height):
    overlay = Image.new('RGBA', (width, height))
    with io.BytesIO() as output:
        overlay.save(output, format='PNG')
        return output.getvalue()

#Accelerated helper function for generating ellipses from point data
@jit(nopython=True)
def ellipse(ra,rb,ang,x0,y0,Nb=16):
    xpos,ypos=x0,y0
    radm,radn=ra,rb
    an=ang

    co,si=cos(an),sin(an)
    the=linspace(0,2*pi,Nb)
    X=radm*cos(the)*co-si*radn*sin(the)+xpos
    Y=radm*cos(the)*si+co*radn*sin(the)+ypos
    return X,Y

NAN_LINE = {'x':None, 'y': None, 'c':"None"}
def create_datashader_ellipses_from_search(search, geopoint_fields, maximum_ellipses_per_tile, extend_meters,
                                           metrics=None, histogram_interval=None):
    if metrics is None:
        metrics = {}
    metrics["over_max"] = False
    metrics["hits"] = 0
    metrics["ellipses"] = 0

    geopoint_center = geopoint_fields["geopoint_center"]
    ellipse_major = geopoint_fields["ellipse_major"]
    ellipse_minor = geopoint_fields["ellipse_minor"]
    ellipse_tilt = geopoint_fields["ellipse_tilt"]
    ellipse_units = geopoint_fields["ellipse_units"]
    category_field = geopoint_fields.get("category_field")

    for i, hit in enumerate(search.scan()):
        metrics["hits"] += 1
        # NB. this actually isn't maximum ellipses per tile, but rather
        # maximum number of records iterated.  We might want to keep this behavior
        # because if you ask for ellipses on a index where none of the records have ellipse
        # point fields you could end up iterating over the entire index
        if i >= maximum_ellipses_per_tile:
            metrics["over_max"] = True
            break

        #Get all the ellipse fields
        locs = getattr(hit, geopoint_center, None)
        majors = getattr(hit, ellipse_major, None)
        minors = getattr(hit, ellipse_minor, None)
        angles = getattr(hit, ellipse_tilt, None)

        #Check that we have all the fields
        if locs is None:
            current_app.logger.debug("hit field %s has no values", geopoint_center)
            continue
        if majors is None:
            current_app.logger.debug("hit field %s has no values", ellipse_major)
            continue
        if minors is None:
            current_app.logger.debug("hit field %s has no values", ellipse_minor)
            continue
        if angles is None:
            current_app.logger.debug("hit field %s has no values", ellipse_tilt)
            continue

        #If its a list determine if there are multiple geos or just a single geo in list format
        if isinstance(locs, list) or isinstance(locs, AttrList):
            if len(locs) == 2 and isinstance(locs[0], float) and isinstance(locs[1], float):
                locs = [locs]
                majors = [majors]
                minors = [minors]
                angles = [angles]
        else:
            #All other cases are single ellipses
            locs = [locs]
            majors = [majors]
            minors = [minors]
            angles = [angles]

        #verify same length
        if not (len(locs) == len(majors) == len(minors) == len(angles)):
            current_app.logger.warning("ellipse parameters and length are not consistent")
            continue
        
        #process each ellipse
        for ii in range(len(locs)):
            loc = locs[ii]

            if isinstance(loc, str):
                if "," not in loc:
                    current_app.logger.warning("skipping loc with invalid str format %s", loc)
                    continue
                lat, lon = loc.split(",", 1)
                loc = dict(lat=float(lat), lon=float(lon))
            elif isinstance(loc, list) or isinstance(loc, AttrList):
                if len(loc) != 2:
                    current_app.logger.warning("skipping loc with invalid list format %s", loc)
                    continue
                lon, lat = loc
                loc = dict(lat=float(lat), lon=float(lon))
            elif not (isinstance(loc, dict) or isinstance(loc, AttrDict)):
                current_app.logger.warning("skipping loc with invalid format %s %s %s", loc, isinstance(loc, list), type(loc))
                continue

            major = majors[ii]
            minor = minors[ii]
            angle = angles[ii]

            #Handle deg->Meters conversion and everything else
            x0,y0 = ds.utils.lnglat_to_meters(loc["lon"], loc["lat"])
            angle = angle * ((2.0*pi)/360.0) #Convert degrees to radians
            if ellipse_units == "majmin_nm":
                major = major * 1852 #nm to meters
                minor = minor * 1852 #nm to meters
            elif ellipse_units == "semi_majmin_nm":
                major = major * (2 * 1852) #nm to meters, semi to full
                minor = minor * (2 * 1852) #nm to meters, semi to full
            elif ellipse_units == "semi_majmin_m":
                major = major * 2 #semi to full
                minor = minor * 2 #semi to full
            #NB. assume "majmin_m" if any others

            #expel above CEP limit
            if major > extend_meters or minor > extend_meters:
                continue

            X,Y = ellipse(minor/2.0,major/2.0,angle,x0,y0, Nb=16) #Points per ellipse, NB. this takes semi-maj/min
            if category_field:
                if histogram_interval:
                    #Do quantization
                    raw = getattr(hit, category_field, 0.0)
                    quantized = math.floor(raw/histogram_interval)*histogram_interval
                    c = str(quantized)
                else:
                    #Just use the value
                    c = str(getattr(hit, category_field, "None"))
            else:
                c = "None"
        
            for p in zip(X,Y,len(X)*[c]):
                yield {'x':p[0], 'y':p[1], 'c':p[2]}
            yield NAN_LINE #Break between ellipses
            metrics["ellipses"] += 1

def generate_nonaggregated_tile(idx, x, y, z, params):
    #Handle legacy parameters
    geopoint_field=params["geopoint_field"]
    timestamp_field=params["timestamp_field"]
    start_time=params["start_time"]
    stop_time=params["stop_time"]
    category_field=params["category_field"]
    category_type=params["category_type"]
    cmap=params["cmap"]
    spread=params["spread"]
    span_range=params["span_range"]
    lucene_query=params["lucene_query"]
    dsl_query=params["dsl_query"]
    dsl_filter=params["dsl_filter"]
    ellipse_major=params["ellipse_major"] 
    ellipse_minor=params["ellipse_minor"]
    ellipse_tilt=params["ellipse_tilt"]
    ellipse_units=params["ellipse_units"]
    ellipse_max_cep=params["ellipse_max_cep"]
    max_batch=params["max_batch"]
    max_bins=params["max_bins"]
    max_ellipses_per_tile=params["max_ellipses_per_tile"]
    histogram_interval=params.get("generated_params", {}).get("histogram_interval", None)
    global_doc_cnt=params.get("generated_params", {}).get("global_doc_cnt", None)
    global_bounds=params.get("generated_params", {}).get("global_bounds", None)
                    
    current_app.logger.info("Generating ellipse tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s"%(idx, z, x, y, geopoint_field, timestamp_field, category_field, start_time, stop_time))
    try:
        # Get the web mercador bounds for the tile
        xy_bounds = mercantile.xy_bounds(x, y, z)
        # Calculate the x/y range in meters
        x_range = xy_bounds.left, xy_bounds.right
        y_range = xy_bounds.bottom, xy_bounds.top
        # Swap the numbers so that [0] is always lowest
        if x_range[0] > x_range[1]:
            x_range = x_range[1], x_range[0]
        if y_range[0] > y_range[1]:
            y_range = y_range[1], y_range[0]
        
        #Expand this by maximum CEP value to get adjacent geos that overlap into our tile
        extend_meters = ellipse_max_cep * 1852


        # Get the top_left/bot_rght for the tile
        top_left = mercantile.lnglat(x_range[0]-extend_meters, y_range[1]+extend_meters)
        bot_rght = mercantile.lnglat(x_range[1]+extend_meters, y_range[0]-extend_meters)        
        
        bb_dict = {
            "top_left": {
                "lat": min(90, max(-90, top_left[1])),
                "lon": min(180, max(-180, top_left[0]))
            },
            "bottom_right": {
                "lat": min(90, max(-90, bot_rght[1])),
                "lon": min(180, max(-180, bot_rght[0]))
            } }

        # Figure out how big the tile is in meters
        xwidth = (x_range[1] - x_range[0])
        yheight = (y_range[1] - y_range[0])
        # And now the area of the tile
        area = xwidth * yheight

        #Create base search 
        base_s = get_search_base(params, idx).params(size=max_batch)

        # Add expanded bounding box
        count_s = copy.copy(base_s)
        count_s = count_s.filter("geo_bounding_box",
            **{
                geopoint_field: bb_dict
            }
        )

        # Per ES documentation, sorting by _doc improves scroll speed
        count_s.sort("_doc")

        #trim category field postfixes
        if category_field:
            if category_field.endswith(".keyword"):
                category_field = category_field[:-len(".keyword")]
            elif category_field.endswith(".raw"):
                category_field = category_field[:-len(".raw")]
        
        #Handle the limiting to only the fields required for processing
        geopoint_fields = {
            "geopoint_center": geopoint_field,
            "ellipse_major": ellipse_major,
            "ellipse_minor": ellipse_minor,
            "ellipse_tilt": ellipse_tilt,
            "ellipse_units": ellipse_units,
            "category_field": category_field
        }
        includes_fields = list( filter( lambda x: x is not None, 
            [ geopoint_field, ellipse_major, ellipse_minor, ellipse_tilt, category_field ]
        ))
        count_s = count_s.source(includes=includes_fields)

        #Process the hits (geos) into a list of points
        s1 = time.time()
        metrics = dict(over_max=False)
        df = pd.DataFrame.from_dict(
            create_datashader_ellipses_from_search(
                count_s,
                geopoint_fields,
                max_ellipses_per_tile,
                extend_meters,
                metrics,
                histogram_interval            )
        )           
        s2 = time.time()
        
        current_app.logger.debug("ES took %s for ellipses: %s   hits: %s", (s2-s1), metrics.get("ellipses",0), metrics.get("hits",0) )        
        
        #Estimate the number of points per tile assuming uniform density
        estimated_points_per_tile = None
        if span_range == 'auto' or span_range == None:
            num_tiles_at_level = sum( 1 for _ in mercantile.tiles(*global_bounds, zooms=z, truncate=False) )
            estimated_points_per_tile = global_doc_cnt / num_tiles_at_level
            current_app.logger.debug("Doc Bounds %s %s %s %s", global_bounds, z, num_tiles_at_level, estimated_points_per_tile)

        #If count is zero then return a null image
        if len(df) == 0:
            current_app.logger.debug("No points in bounding box")
            img = gen_empty(tile_width_px, tile_height_px)
            if metrics.get("over_max"):
                img = gen_overlay(img)
        else:
            #Generate the image
            df["C"] = df["c"].astype('category')
            
            # Find number of pixels in required image
            pixels = tile_height_px * tile_width_px            

            if len(df.index) == 0:
                img = gen_empty(tile_width_px, tile_height_px)
            else:
                agg = ds.Canvas(
                    plot_width=tile_width_px,
                    plot_height=tile_height_px,
                    x_range=x_range,
                    y_range=y_range
                ).line(df, 'x', 'y', agg=rd.count_cat('C'))
        
                span = None
                if span_range == 'flat':
                    min_alpha = 255
                elif span_range == 'narrow':
                    span=[0, math.log(1e3)]
                    min_alpha = 200
                elif span_range == 'normal':
                    span=[0, math.log(1e6)]
                    min_alpha = 100
                elif span_range == 'wide':
                    span=[0,  math.log(1e9)]
                    min_alpha = 50
                else:
                    assert estimated_points_per_tile != None
                    span=[0, math.log(max(estimated_points_per_tile*2, 2))]
                    alpha_span = int(span[1]) * 25
                    min_alpha = 255 - min(alpha_span, 225)

                img = tf.shade(
                            agg, 
                            cmap=cc.palette[cmap], 
                            color_key=create_color_key(df["C"], cmap=cmap),
                            min_alpha=min_alpha,
                            how="log",
                            span=span)

                #NB. No spread on ellipses, could be added here if visibility is an issue
                
                img = img.to_bytesio().read()
                if metrics.get("over_max"):
                    #Put hashing on image to indicate that it is over maximum
                    current_app.logger.info("Generating overlay for tile")
                    img = gen_overlay(img)

        if current_app.config.get("DEBUG_TILES"):
            img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
        #Set headers and return data 
        return img
    except Exception:
        current_app.logger.exception("An exception occured while attempting to generate a tile:")
        raise

####################################################

def generate_tile(idx, x, y, z, params):

    #Handle legacy keywords               
    geopoint_field=params["geopoint_field"]
    timestamp_field=params["timestamp_field"]
    start_time=params["start_time"]
    stop_time=params["stop_time"]
    category_field=params["category_field"]
    category_type=params["category_type"]
    cmap=params["cmap"] 
    spread=params["spread"]
    resolution=params["resolution"]
    span_range=params["span_range"]
    lucene_query=params["lucene_query"]
    dsl_query=params["dsl_query"]
    dsl_filter=params["dsl_filter"]
    max_bins=params["max_bins"]
    histogram_interval=params.get("generated_params", {}).get("histogram_interval", None)
    global_doc_cnt=params.get("generated_params", {}).get("global_doc_cnt", None)
    global_bounds=params.get("generated_params", {}).get("global_bounds", None)

    current_app.logger.debug("Generating tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s"%(idx, z, x, y, geopoint_field, timestamp_field, category_field, start_time, stop_time))
    try:
        # Preconfigured tile size
        tile_height_px = 256
        tile_width_px = 256

        # Get the web mercador bounds for the tile
        xy_bounds = mercantile.xy_bounds(x, y, z)
        # Calculate the x/y range in meters
        x_range = xy_bounds.left, xy_bounds.right
        y_range = xy_bounds.bottom, xy_bounds.top
        # Swap the numbers so that [0] is always lowest
        if x_range[0] > x_range[1]:
            x_range = x_range[1], x_range[0]
        if y_range[0] > y_range[1]:
            y_range = y_range[1], y_range[0]
        # Get the top_left/bot_rght for the tile
        top_left = mercantile.lnglat(x_range[0], y_range[1])
        bot_rght = mercantile.lnglat(x_range[1], y_range[0])
        # Constrain exactly to map boundaries
        bb_dict = {
            "top_left": {
                "lat": min(90, max(-90, top_left[1])),
                "lon": min(180, max(-180, top_left[0]))
            },
            "bottom_right": {
                "lat": min(90, max(-90, bot_rght[1])),
                "lon": min(180, max(-180, bot_rght[0]))
            } }


        # Figure out how big the tile is in meters
        xwidth = (x_range[1] - x_range[0])
        yheight = (y_range[1] - y_range[0])
        # And now the area of the tile
        area = xwidth * yheight

        #Create base search 
        base_s = get_search_base(params, idx)

        # Now find out how many documents 
        count_s = copy.copy(base_s)
        count_s = count_s.filter("geo_bounding_box",
            **{
                geopoint_field: bb_dict
            }
        )

        category_cnt = 0
        if category_field:
            #Also need to calculate the number of categories
            count_s = count_s.params(size=0)
            count_s.aggs.metric(
                'term_count','cardinality',field=category_field
            ).metric(
                'point_count','value_count',field=geopoint_field
            )
            resp = count_s.execute()
            assert len(resp.hits) == 0
            if hasattr(resp.aggregations, "term_count"):
                category_cnt = resp.aggregations.term_count.value
                if category_cnt <= 0:
                    category_cnt = 1
            if hasattr(resp.aggregations, "point_count"):
                doc_cnt = resp.aggregations.point_count.value

            # circuit breaker, if someone wants to color by category and there are
            # more than 1000, they will only get the first 1000 cateogries
            category_cnt = min(category_cnt, 1000)
            current_app.logger.info("Document Count: %s, Category Count: %s", doc_cnt, category_cnt)
        else:
            category_cnt = 1  #Heat mode effectively has one category
            doc_cnt = count_s.count()
            current_app.logger.info("Document Count: %s", doc_cnt)

        #If count is zero then return a null image
        if doc_cnt == 0:
            current_app.logger.debug("No points in bounding box")
            img = gen_empty(tile_width_px, tile_height_px)
        else:
            # Find number of pixels in required image
            pixels = tile_height_px * tile_width_px

            current_zoom = z

            # calculate the geo precision that ensure we have at most one bin per 'pixel'
            # every zoom level halves the number of pixels per bin
            # assuming a square tile
            agg_zooms = math.ceil(math.log(pixels, 4))

            # TODO consider adding 'grid resolution' coarse, fine, finest (pixel-lock)
            # In category-mode, zoom out if max_bins has not been increased
            min_auto_spread = 0 # by default we don't need to spread
            if category_field and max_bins < 65536:
                agg_zooms -= 1
                # if we back out agg_zooms we need to spread a little to make things
                # look correct
                min_auto_spread += 2

            if resolution == "coarse":
                agg_zooms -= 2
                min_auto_spread += 4
            elif resolution == "fine":
                agg_zooms -= 1
                min_auto_spread += 2
            elif resolution == "finest":
                pass # finest needs to do nothing
            else:
                raise ValueError("invalid resolution value")

            # don't allow geotile precision to be anyworse than current zoom
            geotile_precision = max(current_zoom, current_zoom + agg_zooms)

            # calculate how many sub_frames are required to avoid more than max_bins per
            # sub frame.  The number of bins in a sub-frame is 4**Z_delta so we need
            # to move up the zoom-level no further than that
            sub_frame_backout = int( math.log( max_bins, 4) )

            # adding more categories limits how big a sub_frame can be
            if category_cnt <= max_bins:
                max_sub_frame_backout = math.floor( math.log ( max_bins / category_cnt, 4) )
            else:
                # if there are more categories than max_bins, the situation is hopeless
                max_sub_frame_backout = 0

            sub_frame_backout = min(sub_frame_backout, max_sub_frame_backout)
            sub_frame_level = max(current_zoom, geotile_precision - sub_frame_backout)

            geo_bins_per_subframe = 4 ** sub_frame_backout

            current_app.logger.debug(
                "GeoTile Zoom Info: pixels %s, current %s, agg %s, backout %s, sub frame level %s, precision %s, bins %s",
                pixels, current_zoom, agg_zooms, sub_frame_backout, sub_frame_level, geotile_precision, geo_bins_per_subframe 
            )

            #generate n subframe bounding boxes
            subframes = mercantile.tiles(   
                bb_dict["top_left"]["lon"], # west
                bb_dict["bottom_right"]["lat"], # south
                bb_dict["bottom_right"]["lon"], # east
                bb_dict["top_left"]["lat"], # north
                sub_frame_level
            )

            partial_data = False
            df = pd.DataFrame()
            s1 = time.time()
            for _, subframe in enumerate(subframes):
                subframe_bounds = mercantile.bounds(subframe)
                subframe_bbox = {
                    "top_left": {
                        "lat": subframe_bounds.north,
                        "lon": subframe_bounds.west,
                    },
                    "bottom_right": {
                        "lat": subframe_bounds.south,
                        "lon": subframe_bounds.east,
                    }
                }

                subframe_s = copy.copy(base_s)
                subframe_s = subframe_s.params(size=0)
                subframe_s = subframe_s.filter("geo_bounding_box",
                                **{
                                    geopoint_field: subframe_bbox
                                }  )
                
                #Set up the aggregations and the dataframe extraction
                if category_field and histogram_interval == None:  #Category Mode
                    assert (category_cnt * geo_bins_per_subframe) <= max_bins
                    subframe_s.aggs.bucket(
                        'categories',
                        'terms',
                        field=category_field,
                        size=category_cnt
                    ).bucket(
                        'grids',
                        'geotile_grid',
                        field=geopoint_field,
                        precision=geotile_precision,
                        size=geo_bins_per_subframe
                    ).metric(
                        'centroid',
                        'geo_centroid',
                        field=geopoint_field
                    )
                elif category_field and histogram_interval != None:  #Histogram Mode
                    assert histogram_interval != None
                    assert (category_cnt * geo_bins_per_subframe) <= max_bins
                    subframe_s.aggs.bucket(
                        'categories',
                        'histogram',
                        field=category_field,
                        interval=histogram_interval,
                        min_doc_count=1,
                    ).bucket(
                        'grids',
                        'geotile_grid',
                        field=geopoint_field,
                        precision=geotile_precision,
                        size=geo_bins_per_subframe
                    ).metric(
                        'centroid',
                        'geo_centroid',
                        field=geopoint_field
                    )
                else: #Heat Mode
                    assert geo_bins_per_subframe <= max_bins
                    subframe_s.aggs.bucket(
                        'grids',
                        'geotile_grid',
                        field=geopoint_field,
                        precision=geotile_precision,
                        size=geo_bins_per_subframe
                    ).metric(
                        "centroid",
                        'geo_centroid',
                        field=geopoint_field
                    )        
                
                try:
                    resp = subframe_s.execute()
                except:
                    current_app.logger.exception("failed to generate subframe %s subframe %s categories %s %s %s %s", 
                    subframe, subframe_bounds, category_cnt, current_zoom, sub_frame_level, request)
                    raise

                assert len(resp.hits) == 0   

                if hasattr(resp.aggregations, 'categories') and hasattr(resp.aggregations.categories, 'sum_other_doc_count'):
                    partial_data = ( resp.aggregations.categories.sum_other_doc_count > 0 )

                df = df.append(pd.DataFrame(convert(resp)), sort=False)
                
            s2 = time.time()
            current_app.logger.debug("ES took %s for %s" % ((s2-s1), len(df)))

            #Estimate the number of points per tile assuming uniform density
            estimated_points_per_tile = None
            if span_range == 'auto' or span_range == None:
                num_tiles_at_level = sum( 1 for _ in mercantile.tiles(*global_bounds, zooms=z, truncate=False) )
                estimated_points_per_tile = global_doc_cnt / num_tiles_at_level
                current_app.logger.debug("Doc Bounds %s %s %s %s", global_bounds, z, num_tiles_at_level, estimated_points_per_tile)

            if len(df.index) == 0:
                img = gen_empty(tile_width_px, tile_height_px)
            else:
                ###############################################################
                # Category Mode
                if category_field:
                    df["T"] = df["t"].astype('category')
                    agg = ds.Canvas(
                        plot_width=tile_width_px,
                        plot_height=tile_height_px,
                        x_range=x_range,
                        y_range=y_range
                    ).points(df, 'x', 'y', agg=sum_cat('T', 'c'))
                    
                    span = None
                    if span_range == 'flat':
                        min_alpha = 255
                    elif span_range == 'narrow':
                        span=[0, math.log(1e3)]
                        min_alpha = 200
                    elif span_range == 'normal':
                        span=[0, math.log(1e6)]
                        min_alpha = 100
                    elif span_range == 'wide':
                        span=[0,  math.log(1e9)]
                        min_alpha = 50
                    else:
                        assert estimated_points_per_tile != None
                        span=[0, math.log(max(estimated_points_per_tile*2, 2))]
                        alpha_span = int(span[1]) * 25
                        min_alpha = 255 - min(alpha_span, 225)

                    current_app.logger.debug("MinAlpha:%s Span:%s", min_alpha, span)
                    img = tf.shade(
                            agg, 
                            cmap=cc.palette[cmap], 
                            color_key=create_color_key(df["T"], cmap=cmap), 
                            min_alpha=min_alpha,
                            how="log",
                            span=span)

                ###############################################################
                # Heat Mode
                else: #Heat Mode
                    agg = ds.Canvas(
                        plot_width=tile_width_px,
                        plot_height=tile_height_px,
                        x_range=x_range,
                        y_range=y_range
                    ).points(df, 'x', 'y', agg=ds.sum('c'))
                    
                    # Handle span range, the span applies the color map across 
                    # the span range, so for example, if span is narrow, any
                    # bins that have 1000 or more items will be colored full
                    # scale
                    span = None
                    if span_range == 'flat':
                        span=[0, 0]
                    elif span_range == 'narrow':
                        span=[0, math.log(1e3)]
                    elif span_range == 'normal':
                        span=[0, math.log(1e6)]
                    elif span_range == 'wide':
                        span=[0,  math.log(1e9)]
                    else:
                        assert estimated_points_per_tile != None
                        span=[0, math.log(max(estimated_points_per_tile*2, 2))]

                    current_app.logger.debug("Span %s %s", span, span_range)
                    img = tf.shade(agg, cmap=cc.palette[cmap], how="log", span=span)

                ###############################################################
                # Common

                #Below zoom threshold spread to make individual dots large enough
                if spread is None or spread < 0:
                    spread_threshold = 11
                    # Always spread at least min_auto_spread
                    spread = min_auto_spread
                    if z >= spread_threshold:
                        # Increase spread at high zoom levels, with a min spread of 2
                        spread = math.floor(min_auto_spread +(z-(spread_threshold-1))*.25)
                    current_app.logger.info("Calculated auto-spread %s (min %s)", spread, min_auto_spread)
                else:
                    current_app.logger.info("Spreading by fixed %s", spread)

                if spread > 0:
                    img = tf.spread(img, spread)

                img = img.to_bytesio().read()

                if partial_data:
                    current_app.logger.info("Generating overlay for tile due to partial category data")
                    img = gen_overlay(img)

        if current_app.config.get("DEBUG_TILES"):
            img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))

        #Set headers and return data 
        return img
    except Exception:
        current_app.logger.exception("An exception occured while attempting to generate a tile:")
        raise

##############################################################################
# Application Factory
##############################################################################

def create_app(args=None):
    """
    Use factory pattern as shown in:

    https://flask.palletsprojects.com/en/1.1.x/tutorial/factory/
    """

    flask_app = Flask(__name__)

    # Load default settings
    flask_app.config.from_object(Config())

    # Load from configuration file
    if os.environ.get("ELASTIC_DATASHADER_SETTINGS"):
        flask_app.config.from_envvar("ELASTIC_DATASHADER_SETTINGS")

    # Load command-line arguments (if provided)
    if args:
        for k,v in vars(args).items():
            if k.upper() in flask_app.config:
                flask_app.config[k.upper()] = v
    flask_app.config["SECRET_KEY"] = 'CSRFProtectionKey'
 
 
    #Limit logging at INFO, reduce if needed for debugging
    if flask_app.config["LOG_LEVEL"]:
        flask_app.logger.setLevel(getattr(logging, flask_app.config["LOG_LEVEL"]))

    flask_app.logger.info("Loaded configuration %s", flask_app.config)
    flask_app.logger.info("Loaded environment %s", os.environ)

    # Register the API
    flask_app.logger.info("Registering API")
    flask_app.register_blueprint(api)


    # If ElasticAPM can be loaded, then attempt to configure
    # if via environment variable.  To install APM
    # run `pip install elastic-apm[flask]` then before
    # running the application set the following environment
    # variables:
    #
    #    ELASTIC_APM_SERVICE_NAME
    #    ELASTIC_APM_SERVER_URL
    #
    # Additional parameters can be found here:
    #    https://www.elastic.co/guide/en/apm/agent/python/current/configuration.html
    try:
        if os.environ.get("ELASTIC_APM_SERVER_URL"):
            from elasticapm.contrib.flask import ElasticAPM
            apm = ElasticAPM(flask_app, logging=logging.ERROR)
    except ImportError:
        apm = None

    scheduler = APScheduler()
    scheduler.init_app(flask_app)
    scheduler.start()
    job_id = 'CleanupThread_'+str(os.getpid())
    scheduler.add_job(func=scheduled_task, trigger='interval',  seconds=5*60, args=[job_id, flask_app.config.get("CACHE_DIRECTORY")], id=job_id)
    return flask_app

#@flask_app.scheduler.task('interval', id='CleanupThread_'+str(os.getpid()), seconds=30, misfire_grace_time=900)
def scheduled_task(id, cache_dir):
    #See last update file
    logging.info("Checking for old cache (%s) at %s" % (id, cache_dir))
    if not os.path.exists(cache_dir+"/cache.age.check"):
        logging.info("Had to recreate check file (%s) at %s" % (id, cache_dir))
        open(cache_dir+"/cache.age.check", 'a').close()

    check_age = time.time() - os.path.getmtime(cache_dir+"/cache.age.check")
    logging.info("Checking age(%s) at %s" % (id, check_age))
    if check_age > 5*60:
        os.utime(cache_dir+"/cache.age.check")
        logging.info("Doing age check (%s)" % (id))
        check_cache_age(cache_dir, 24*60*60) #24 hours cleanup
        logging.info("Cache check complete (%s)" % (id))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TMS Server with Cache')

    # App configuration
    parser.add_argument('-d', '--cache_directory', default=Config.CACHE_DIRECTORY, help="Directory for tile cache")
    parser.add_argument('-t', '--cache_timeout', default=Config.CACHE_TIMEOUT, help="Cache lifespan in sec")
    parser.add_argument('-e', '--elastic', default=Config.ELASTIC, help="Elasticsearch URL")
    parser.add_argument('--hostname', default=socket.getfqdn(), help="node hostname")
    parser.add_argument('-H', '--proxy_host', default=Config.PROXY_HOST, help="Proxy host")
    parser.add_argument('-P', '--proxy_prefix', default=Config.PROXY_PREFIX, help="Proxy prefix")
    parser.add_argument('-k', '--tms_key', default=Config.TMS_KEY, help="TMS key required in header")
    parser.add_argument('--header-file', default=Config.HEADER_FILE, help="configured headers to include in ES requests")
    parser.add_argument('-W', '--whitelist-headers', default=Config.WHITELIST_HEADERS, help="whitelist headers to pass along")
    parser.add_argument('--debug-tiles', default=Config.DEBUG_TILES, action="store_true", help="render tiles with debug overlay")

    # Development server arguments
    parser.add_argument('--debug', default=False, action='store_true', help="Enable Flask debug mode")

    parser.add_argument('-p', '--port', default=5000, help="Port to run TMS server")
    parser.add_argument('-n', '--num_processes', default=32, type=int, help="Number of concurrent Flask processes to run")

    parser.add_argument('--ssl_adhoc', default=False, action='store_true', help="Enable SSL in ad-hoc mode")
    parser.add_argument('-s', '--ssl', default=False, action='store_true', help="Enable SSL, set environment variables to confgure: \
                                                                                SSL_SERVER_KEY, SSL_SERVER_CERT, SSL_CA_CHAIN")
    args = parser.parse_args()

    app = create_app(args)

    #Set all the flask arguments as a dictionary
    flask_args = {}
    flask_args["host"] = "0.0.0.0"
    flask_args["port"] = args.port
    flask_args["processes"] = args.num_processes
    flask_args["threaded"] = False

    logging.getLogger().setLevel(logging.INFO)

    #Handle Debug
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        flask_args["debug"] = True
        flask_args["processes"] = 1

    #Handle SSL
    if args.ssl_adhoc:
        flask_args["ssl_context"] = 'adhoc'
    elif args.ssl:
        flask_args["ssl_context"] = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        flask_args["ssl_context"].load_verify_locations(os.environ.get("SSL_CA_CHAIN"))
        flask_args["ssl_context"].load_cert_chain(os.environ.get("SSL_SERVER_CERT"), os.environ.get("SSL_SERVER_KEY") )

    #Run Flask
    app.run(**flask_args)
