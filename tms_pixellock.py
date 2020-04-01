#!/usr/bin/env python

from flask import Flask, Response, current_app
from flask import request, render_template, redirect
from flask import Blueprint
from flask_wtf import FlaskForm
import wtforms

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
from functools import lru_cache

from datetime import datetime, timedelta
from pprint import pprint, pformat
import traceback

import datashader as ds
import pandas as pd
import colorcet as cc
import datashader.transfer_functions as tf
import datashader.reductions as rd

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.aggs import Bucket

import mercantile

import png
import tempfile
import socket
import urllib3
import json
import fcntl

from flask_apscheduler import APScheduler

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)
urllib3.disable_warnings(urllib3.exceptions.SNIMissingWarning)
urllib3.disable_warnings(UserWarning)

from numba import jit
from numpy import linspace, pi, sin, cos 

from PIL import Image, ImageDraw


#from OpenSSL import SSL
import ssl

#Import helpers to assist with datashader
from datashader_helpers import sum_cat

#Logging for non-Flask items
logging.basicConfig(level=logging.INFO)
logging.getLogger("elasticsearch").setLevel(logging.WARN)
logging.getLogger("urllib3").setLevel(logging.WARN)

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
    MAX_BINS = os.environ.get("DATASHADER_MAX_BINS", 10000)
    PORT = None
    HOSTNAME = socket.getfqdn()

# Globals
default_justification = "Software Development Testing"
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

@api.route('/color_map', methods=['GET'])
def display_color_map():
    color_key_map = {}
    color_file = os.path.join(current_app.config["CACHE_DIRECTORY"]+"/%s/%s-colormap.json"%(request.args.get('name'), request.args.get('field')))
    if os.path.exists(color_file):
        with open(color_file, 'r') as c:
            color_key_map = yaml.safe_load(c)
    else:
        current_app.logger.warning("No colormap found at: %s", color_file)
    
    color_key_hash = {}
    for k in color_key_map.keys():
        color_key_hash[k] = int(hashlib.md5(k.encode('utf-8')).hexdigest()[0:2], 16)

    return render_template('color_map.html', name=request.args.get('name'), field=request.args.get('field'), color_key_map=color_key_map)

class ConfigForm(FlaskForm):
    name = wtforms.StringField('Name', description="Name of map layer", validators=[wtforms.validators.DataRequired()])
    idx = wtforms.StringField('Index', description="Index name", validators=[wtforms.validators.DataRequired()])
    mode = wtforms.SelectField('Mode', choices=[('heat', 'Heat Map'), ('category', 'Category Map')] )
    geopoint_field = wtforms.StringField('Geopoint Field', description="Required", validators=[wtforms.validators.DataRequired()])
    timestamp_field = wtforms.StringField('Timestamp Field', description="Optional, needed if Date Range is not All")
    category_field = wtforms.StringField('Category Field', description="Optional, needed if mode is category")
    submit = wtforms.SubmitField('Add Config')

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


@api.route('/<idx>/<field_name>/legend.json', methods=['GET'])
def provide_legend(idx, field_name):
    color_key_map = {}
    color_file = os.path.join(current_app.config["CACHE_DIRECTORY"]+"%s/%s-colormap.json"%(idx, field_name))
    if os.path.exists(color_file):
        with open(color_file, 'r') as c:
            color_key_map = yaml.safe_load(c)
    else:
        current_app.logger.warning("No colormap found at: %s", color_file)
    
    color_key_hash = {}
    for k in color_key_map.keys():
        color_key_hash[k] = int(hashlib.md5(k.encode('utf-8')).hexdigest()[0:2], 16)
    
    data = json.dumps(color_key_map)
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

    #Default values
    ellipses = False
    justification = default_justification
    lucene_query = None
    from_time = None
    to_time = "now"
    dsl_filter = None
    cmap = None
    geopoint_field = None
    timestamp_field = None
    category_field = None
    category_type = None
    ellipse_major = ""
    ellipse_minor = ""
    ellipse_tilt = ""
    ellipse_units = ""

    #Argument Parameter, NB. These overwrite what is in index config
    params = request.args.get('params')
    if params and params != '{params}':
        params = json.loads(request.args.get('params'))
        if params.get("timeFilters",{}).get("from"):
            from_time = params.get("timeFilters",{}).get("from")
        if params.get("timeFilters",{}).get("to"):
            to_time = params.get("timeFilters",{}).get("to")
        if params.get("filters") and lucene_query is None:
            dsl_filter = build_dsl_filter(params.get("filters"))
        if params.get("query") and lucene_query is None:
            lucene_query = params.get("query").get("query")
    elif params and params == '{params}':
        #If the parameters haven't been provided yet
        resp = Response("TMS parameters not yet provided", status=204)
        return resp

    # Custom parameters can be provided by the URL
    ellipses = request.args.get('ellipses', default=ellipses)
    if ellipses == "false" or ellipses==False:
        ellipses = False
    else:
        #Handle the other fields
        ellipse_major = request.args.get('ellipse_major', default="")
        ellipse_minor = request.args.get('ellipse_minor', default="")
        ellipse_tilt = request.args.get('ellipse_tilt', default="")
        ellipse_units = request.args.get('ellipse_units', default="")
        if ellipse_major == "" or ellipse_major == "" or ellipse_major == "":
            ellipses = False
    
    category_field = request.args.get('category_field', default=category_field)
    category_type = request.args.get('category_type', default=category_type)
    cmap = request.args.get('cmap', default=cmap)
    try:
        spread = int(request.args.get('spread'))
    except (TypeError, ValueError):
        spread = None
    span_range = request.args.get('span', default="auto")
    geopoint_field = request.args.get('geopoint_field', default=geopoint_field)
    timestamp_field = request.args.get('timestamp_field', default=timestamp_field)

    # TODO handle dumb javascript on the client side
    if category_field == "null":
        category_field = None
    current_app.logger.info("geopoint %s timestamp %s", geopoint_field, timestamp_field)

    # TMS tile coordinates
    x = int(x)
    y = int(y)
    z = int(z)

    #Handle time bounding
    now = datetime.utcnow()   
    stop_time = now
    if to_time:
        try:
            stop_time = convertKibanaTime(to_time, now)
        except ValueError:
            current_app.logger.exception("invalid to_time parameter")
            resp = Response("invalid to_time parameter", status=500)
            return resp

    start_time = None
    if from_time:
        try:
            start_time = convertKibanaTime(from_time, now)
        except ValueError:
            current_app.logger.exception("invalid from_time parameter")
            resp = Response("invalid from_time parameter", status=500)
            return resp

    start_time, stop_time = quantizeTimeRange(start_time, stop_time)

    #Calculate a hash value for the specific parameter set
    hashable_params = {
        "start_time":start_time,
        "stop_time":stop_time,
        "dsl_filter":dsl_filter,
        "lucene_query":lucene_query,
        "cmap":cmap,
        "spread":spread,
        "span_range":span_range,
        "category_field":category_field,
        "ellipses":ellipses,
        "ellipse_major":ellipse_major,
        "ellipse_minor":ellipse_minor,
        "ellipse_tilt":ellipse_tilt,
        "ellipse_units":ellipse_units
    }
    for k in hashable_params.keys():
        if isinstance(hashable_params[k], datetime):
                hashable_params[k] = hashable_params[k].isoformat()

    parameter_hash = hashlib.md5()
    for key, param in sorted(hashable_params.items()):
        parameter_hash.update(str(param).encode("utf-8"))
    parameter_hash = parameter_hash.hexdigest()
    current_app.logger.debug("Parameters: %s (%s)", hashable_params, parameter_hash)
    
    set_cache_params("/%s/%s/params.json"%(idx, parameter_hash), current_app.config["CACHE_DIRECTORY"], hashable_params)

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
        color_map_filename = os.path.join(current_app.config["CACHE_DIRECTORY"], idx, "%s-colormap.json"%category_field)
        
        #Separate call for ellipse
        try:
            if ellipses:
                img = generate_nonaggregated_tile(idx, x, y, z, 
                        geopoint_field=geopoint_field, time_field=timestamp_field, 
                        start_time=start_time, stop_time=stop_time,
                        category_field=category_field, category_type=category_type, map_filename=color_map_filename,
                        cmap=cmap, spread=spread, span_range=span_range,
                        lucene_query=lucene_query, dsl_filter=dsl_filter,
                        max_bins=current_app.config["MAX_BINS"],
                        justification=justification,
                        ellipse_major=ellipse_major, ellipse_minor=ellipse_minor, 
                        ellipse_tilt=ellipse_tilt, ellipse_units=ellipse_units,
                        maximum_cep = 50, maximum_ellipses_per_tile = 100000 )
            else:
                img = generate_tile(idx, x, y, z, 
                        geopoint_field=geopoint_field, time_field=timestamp_field, 
                        start_time=start_time, stop_time=stop_time,
                        category_field=category_field, category_type=category_type, map_filename=color_map_filename,
                        cmap=cmap, spread=spread, span_range=span_range,
                        lucene_query=lucene_query, dsl_filter=dsl_filter,
                        max_bins=current_app.config["MAX_BINS"],
                        justification=justification )
        except:
            logging.exception("Exception Generating Tile for request %s", request)
            resp = Response("Exception Generating Tile", status=500)
            return resp
        
        set_cache("/%s/%s/%s/%s/%s.png"%(idx, parameter_hash, z, x, y), img, current_app.config["CACHE_DIRECTORY"])

    resp = Response(img, status=200)
    resp.headers['Content-Type'] = 'image/png'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.cache_control.max_age = 60
    return resp

###########################################################################
# Utility Functions
###########################################################################

def get_connection_base():
    # TODO - this incorrectly assumes that proxy always implies HTTP an no-proxy is always HTTP
    if current_app.config.get("PROXY_HOST"):
        connection_base = "https://" + current_app.config.get('PROXY_HOST') + "/" + current_app.config.get("PROXY_PREFIX") + "/tms/"
    else:
        connection_base = "http://" + current_app.config.get("HOSTNAME") + ":%s/tms/"%current_app.config.get('PORT')

    return connection_base

def build_dsl_filter(filter_inputs):
    if len(filter_inputs) == 0:
        return None
    filter = {"filter":[{"match_all":{}}], "must_not":[]}
    
    for f in filter_inputs:
        #Handle spatial filters
        if f.get("meta").get("type") == "spatial_filter":
            if f.get("geo_polygon"):
                if f.get("meta").get("negate"):
                    filter["must_not"].append( {"geo_polygon":f.get("geo_polygon")})
                else:
                    filter["filter"].append( {"geo_polygon":f.get("geo_polygon")})
            elif f.get("geo_bounding_box"):
                if f.get("meta").get("negate"):
                    filter["must_not"].append( {"geo_bounding_box":f.get("geo_bounding_box")})
                else:
                    filter["filter"].append( {"geo_bounding_box":f.get("geo_bounding_box")})
        else:
            #Handle phrase matching
            if f.get("meta").get("negate"):
                filter["must_not"].append( f.get("query"))
            elif f.get("meta").get("disabled") in ("true", True):
                continue
            else:
                #Handle "is one of"
                is_phrase_match = False
                for match_field, match_params in f.get("query", {}).get("match", {}).items():
                    if match_params.get("type") == "phrase":
                        match_params.pop("type", None)
                        is_phrase_match = True
                        break
                if is_phrase_match:
                    f["query"]["match_phrase"] = f.get("query", {}).pop("match", {})
                
                filter["filter"].append(f.get("query"))

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


class GeotileGrid(Bucket):
    name = 'geotile_grid'

def set_cache_params(paramsfile, cache_dir, params):
    
    pathlib.Path(os.path.dirname(os.path.join(cache_dir+paramsfile))).mkdir(parents=True, exist_ok=True) 
    if os.path.exists(os.path.join(cache_dir+paramsfile)) == False:
        #Write params dict if it does not exist
        with open(os.path.join(cache_dir+paramsfile), 'w') as i:
            i.write(json.dumps(params))
    else:
        #Touch the file to update last accessed time
        os.utime(os.path.join(cache_dir+paramsfile))
    return None

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

color_key_hash_lock = threading.Lock()
def create_color_key_hash_file(categories, color_file, cmap='glasbey_light'):
    color_key_map = {}
    

    #Load the file
    with open(color_file, 'w+') as stream:
        color_key_map = yaml.safe_load(stream)
        try:
            fcntl.flock(stream, fcntl.LOCK_EX)     
            #If file is blank load a blank dictionary
            if color_key_map == None:
                color_key_map = {}
    
            changed = False
            color_key = {}
            for k in set(categories):
                # Set the global color key for this category
                color_key[k] = cc.palette[cmap][int(hashlib.md5(k.encode('utf-8')).hexdigest()[0:2], 16)]
                if k not in color_key_map:
                    #Add it to the map to return
                    color_key_map[k] = cc.palette[cmap][int(hashlib.md5(k.encode('utf-8')).hexdigest()[0:2], 16)]
                    changed = True
            if changed:
                stream.seek(0)
                yaml.dump(color_key_map, stream)
        finally:
            fcntl.lockf(stream, fcntl.LOCK_UN)

    return color_key

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
def create_datashader_ellipses_from_search(search, geopoint_fields, maximum_ellipses_per_tile, extend_meters, metrics=None):
    if metrics is None:
        metrics = {}
    metrics["over_max"] = False

    geopoint_center = geopoint_fields["geopoint_center"]
    ellipse_major = geopoint_fields["ellipse_major"]
    ellipse_minor = geopoint_fields["ellipse_minor"]
    ellipse_tilt = geopoint_fields["ellipse_tilt"]
    ellipse_units = geopoint_fields["ellipse_units"]
    category_field = geopoint_fields.get("category_field")

    for i, hit in enumerate(search.scan()):
        # TODO this actually isn't maximum ellipses per tile, but rather
        # maximum number of records iterated.  We might want to keep this behavior
        # because if you ask for ellipses on a index where none of the records have ellipse
        # point fields you could end up iterating over the entire index
        if i >= maximum_ellipses_per_tile:
            metrics["over_max"] = True
            break
        #Handle deg->Meters conversion and everything else
        x0,y0 = ds.utils.lnglat_to_meters(hit[geopoint_center]["lon"], hit[geopoint_center]["lat"])
        major = hit[ellipse_major]
        minor = hit[ellipse_minor]
        angle = hit[ellipse_tilt] * ((2.0*pi)/360.0) #Convert degrees to radians
        if ellipse_units == "majmin_nm":
            major *= 1852 #nm to meters
            minor *= 1852 #nm to meters
        elif ellipse_units == "semi_majmin_nm":
            major *= 2 * 1852 #nm to meters, semi to full
            minor *= 2 * 1852 #nm to meters, semi to full
        elif ellipse_units == "semi_majmin_m":
            major *= 2 #semi to full
            minor *= 2 #semi to full
        #NB. assume "majmin_m" if any others
        
        #expel above CEP limit
        #TODO: Figure out how we will handle CEP maximums
        if major > extend_meters or minor > extend_meters:
            continue

        X,Y = ellipse(minor,major,angle,x0,y0, Nb=16) #Points per ellipse
        if category_field:
            c = hit[category_field]
        else:
            c = "None"
        
        for p in zip(X,Y,len(X)*[c]):
            yield {'x':p[0], 'y':p[1], 'c':p[2]}
        yield NAN_LINE #Break between ellipses

def generate_nonaggregated_tile(idx, x, y, z, 
                    geopoint_field="location", time_field='@timestamp', 
                    start_time=None, stop_time=None,
                    category_field=None, category_type=None, map_filename=None, cmap='bmy', spread=None,
                    span_range='auto',
                    lucene_query=None, dsl_filter=None,
                    max_bins=10000,
                    justification=default_justification,
                    ellipse_major="", ellipse_minor="", 
                    ellipse_tilt="", ellipse_units=None,
                    maximum_cep = 50, maximum_ellipses_per_tile = 100000):

    current_app.logger.info("Generating ellipse tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s"%(idx, z, x, y, geopoint_field, time_field, category_field, start_time, stop_time))
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
        
        #Expand this by maximum CEP value to get adjacent geos that overlap into our tile
        extend_meters = maximum_cep * 1852


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

        #Handle time calculations
        time_range = { time_field: {} }
        if start_time != None:
            time_range[time_field]["gte"] = start_time
        if stop_time != None:
            time_range[time_field]["lte"] = stop_time

        # Connect to Elasticsearch (TODO is it faster if this is global?)
        es = Elasticsearch(
            current_app.config.get("ELASTIC"),
            verify_certs=False,
            timeout=900,
            headers={"acecard-justification":justification}
        )

        #Create base search 
        base_s = Search(index=idx).using(es).params(size=max_bins)

        #Add time bounds
        if time_range[time_field]:
            base_s = base_s.filter("range", **time_range)

        #Add lucene query
        if lucene_query:
            base_s = base_s.filter('query_string', query=lucene_query)

        #Add dsl filtering
        if dsl_filter:
            #Need to convert to a dict, merge with filters then convert back to a search object
            base_dict = base_s.to_dict()
            if base_dict.get("query",{}).get("bool",{}).get("filter") == None:
                base_dict["query"]["bool"]["filter"] = []
            for f in dsl_filter["filter"]:
                base_dict["query"]["bool"]["filter"].append(f)
            if base_dict.get("query",{}).get("bool",{}).get("must_not") == None:
                base_dict["query"]["bool"]["must_not"] = []
            for f in dsl_filter["must_not"]:
                base_dict["query"]["bool"]["must_not"].append(f)
            base_s = Search.from_dict(base_dict)
            base_s = base_s.index(idx).using(es)

        #west, south, east, north
        doc_bounds = [ -180, -90, 180, 90 ]
        global_doc_cnt = 0

        # if span_range is auto we need to estimate the density
        bounds_s = copy.copy(base_s)
        bounds_s = bounds_s.params(size=0)

        if span_range == "auto":
            #See how far the data spans and how many points are in it
            bounds_s.aggs.metric(
                'viewport','geo_bounds',field=geopoint_field
            ).metric(
                'point_count','value_count',field=geopoint_field
            )

        #If the field is a number, we need to figure out it's min/max globally
        if category_type == "number":
            bounds_s.metric(
                'field_stats', 'stats', field=category_field
            )

        # We only need to do a global query if we are in span 'auto' or
        # using a numeric category    
        if len(list(bounds_s.aggs)) > 0:
            bounds_resp = bounds_s.execute()
            assert len(bounds_resp.hits) == 0

            if hasattr(bounds_resp.aggregations, "viewport"):
                if hasattr(bounds_resp.aggregations.viewport, "bounds"):
                    doc_bounds = [ 
                        bounds_resp.aggregations.viewport.bounds.top_left.lon,
                        bounds_resp.aggregations.viewport.bounds.bottom_right.lat,
                        bounds_resp.aggregations.viewport.bounds.bottom_right.lon,
                        bounds_resp.aggregations.viewport.bounds.top_left.lat,
                    ]
            if hasattr(bounds_resp.aggregations, "point_count"):
                global_doc_cnt = bounds_resp.aggregations.point_count.value
        else:
            current_app.logger.debug("Skipping global query")

        estimated_points_per_tile = None
        # Estimate the number of points per tile assuming uniform density
        if span_range == "auto":
            num_tiles_at_level = sum( 1 for _ in mercantile.tiles(*doc_bounds, zooms=z, truncate=False) )
            estimated_points_per_tile = global_doc_cnt / num_tiles_at_level
            current_app.logger.debug("Doc Bounds %s %s %s %s", doc_bounds, z, num_tiles_at_level, estimated_points_per_tile)


        # Add expanded bounding box
        count_s = copy.copy(base_s)
        count_s = count_s.filter("geo_bounding_box",
            **{
                geopoint_field: bb_dict
            }
        )

        # Per ES documentation, sorting by _doc improves scroll speed
        # TODO allow users to specify other sort field
        count_s.sort("_doc")

        #trim category field postfixes
        if category_field:
            if category_field.endswith(".keyword"):
                category_field = category_field[:-len(".keyword")]
            elif category_field.endswith(".raw"):
                category_field = category_field[:-len(".raw")]
        #TODO: Handle the limiting to only the fields required for processing

        geopoint_fields = {
            "geopoint_center": geopoint_field,
            "ellipse_major": ellipse_major,
            "ellipse_minor": ellipse_minor,
            "ellipse_tilt": ellipse_tilt,
            "ellipse_units": ellipse_units,
            "category_field": category_field
        }
        #Process the hits (geos) into a list of points
        s1 = time.time()
        metrics = dict(over_max=False)
        df = pd.DataFrame.from_dict(
            create_datashader_ellipses_from_search(
                count_s,
                geopoint_fields,
                maximum_ellipses_per_tile,
                extend_meters,
                metrics
            )
        )           
        s2 = time.time()
        
        current_app.logger.debug("ES took %s for %s" % ((s2-s1), len(df)))        
        
        #If count is zero then return a null image
        if len(df) == 0:
            current_app.logger.debug("No points in bounding box")
            img = b""
        else:
            #Generate the image
            df["C"] = df["c"].astype('category')
            
            # Find number of pixels in required image
            pixels = tile_height_px * tile_width_px            

            if len(df.index) == 0:
                img = b""
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
                            cmap=cc.glasbey_category10, 
                            color_key=create_color_key_hash_file(df["C"], map_filename),
                            min_alpha=min_alpha,
                            how="log",
                            span=span)
                
                #TODO: Handle spread?
                #spread = 1
                #img = tf.spread(img, spread)
                
                
                img = img.to_bytesio().read()

                if metrics.get("over_max"):
                    #Put hashing on image to indicate that it is over maximum
                    current_app.logger.info("Generating overlay for tile")
                    img = gen_overlay(img)

        #Set headers and return data 
        return img
    except Exception:
        current_app.logger.exception("An exception occured while attempting to generate a tile:")
        raise

@lru_cache(10)
def gen_overlay_img(width, height, thickness):
    """
    Create an overlay hash image, using an lru_cache since the same
    overlay can be generated once and then reused indefinately
    """
    overlay = Image.new('RGBA', (width, height))
    draw = ImageDraw.Draw(overlay)
    for s in range(0, max(height,width), thickness*2):
        draw.line( [(s-width,s+height), (s+width,s-height)], (255,0,0,64), thickness)
    return overlay

def gen_overlay(img, thickness=8):
    base = Image.open(io.BytesIO(img))
    overlay = gen_overlay_img(*base.size, thickness=thickness)
    out = Image.alpha_composite(base, overlay)
    with io.BytesIO() as output:
        out.save(output, format='PNG')
        return output.getvalue()

####################################################

def generate_tile(idx, x, y, z, 
                    geopoint_field="location", time_field='@timestamp', 
                    start_time=None, stop_time=None,
                    category_field=None, category_type=None, map_filename=None, cmap='bmy', spread=None,
                    span_range='auto',
                    lucene_query=None, dsl_filter=None,
                    max_bins=10000,
                    justification=default_justification ):
    
    current_app.logger.debug("Generating tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s"%(idx, z, x, y, geopoint_field, time_field, category_field, start_time, stop_time))
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

        #Handle time calculations
        time_range = { time_field: {} }
        if start_time != None:
            time_range[time_field]["gte"] = start_time
        if stop_time != None:
            time_range[time_field]["lte"] = stop_time

        # Connect to Elasticsearch (TODO is it faster if this is global?)
        es = Elasticsearch(
            current_app.config.get("ELASTIC"),
            verify_certs=False,
            timeout=900,
            headers={"acecard-justification":justification}
        )

        #Create base search 
        base_s = Search(index=idx).using(es)
        #base_s = base_s.params(size=0)
        #Add time bounds
        if time_range[time_field]:
            base_s = base_s.filter("range", **time_range)

        #Add lucene query
        if lucene_query:
            base_s = base_s.filter('query_string', query=lucene_query)

        #Add dsl filtering
        if dsl_filter:
            #Need to convert to a dict, merge with filters then convert back to a search object
            base_dict = base_s.to_dict()
            if base_dict.get("query",{}).get("bool",{}).get("filter") == None:
                base_dict["query"]["bool"]["filter"] = []
            for f in dsl_filter["filter"]:
                base_dict["query"]["bool"]["filter"].append(f)
            if base_dict.get("query",{}).get("bool",{}).get("must_not") == None:
                base_dict["query"]["bool"]["must_not"] = []
            for f in dsl_filter["must_not"]:
                base_dict["query"]["bool"]["must_not"].append(f)            
            base_s = Search.from_dict(base_dict)          
            base_s = base_s.index(idx).using(es)

        #See how far the data spans and how many points are in it
        bounds_s = copy.copy(base_s)
        bounds_s = bounds_s.params(size=0)
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

        #west, south, east, north
        doc_bounds = [ -180, -90, 180, 90 ]
        global_doc_cnt = 0

        # We only need to do a global query if we are in span 'auto' or
        # using a numeric category
        if len(list(bounds_s.aggs)) > 0:
            bounds_resp = bounds_s.execute()
            assert len(bounds_resp.hits) == 0

            if hasattr(bounds_resp.aggregations, "viewport"):
                if hasattr(bounds_resp.aggregations.viewport, "bounds"):
                    doc_bounds = [ 
                        bounds_resp.aggregations.viewport.bounds.top_left.lon,
                        bounds_resp.aggregations.viewport.bounds.bottom_right.lat,
                        bounds_resp.aggregations.viewport.bounds.bottom_right.lon,
                        bounds_resp.aggregations.viewport.bounds.top_left.lat,
                    ]
            if hasattr(bounds_resp.aggregations, "point_count"):
                global_doc_cnt = bounds_resp.aggregations.point_count.value

        # Now find out how many documents 
        count_s = copy.copy(base_s)
        count_s = count_s.filter("geo_bounding_box",
            **{
                geopoint_field: bb_dict
            }
        )

        category_cnt = 0
        histogram_interval = None
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
            # more than 10000, they will only get the first 1000 cateogries
            category_cnt = min(category_cnt, 1000)
            current_app.logger.info("Document Count: %s, Category Count: %s", doc_cnt, category_cnt)

            # In a numeric field, we can fall back to histogram mode if there are too many unique values
            if category_cnt >= 1000 and category_type == "number":
                current_app.logger.info("attempting histogram")
                if hasattr(bounds_resp.aggregations, 'field_stats'):
                    current_app.logger.info("field stats %s", bounds_resp.aggregations.field_stats)
                    # to prevent strain on the cluster, if there are over 1million
                    # documents given the current parameters, reduce the number of histogram
                    # bins.  Note this is kinda a wag...maybe something smarter can be done
                    if global_doc_cnt > 1000000:
                        category_cnt = 100
                    else:
                        category_cnt = 1000
                    # determine the range of category values
                    histogram_range = (bounds_resp.aggregations.field_stats.max - bounds_resp.aggregations.field_stats.min)
                    # round to the nearest larger power of 10
                    histogram_range = math.pow(10, math.ceil(math.log10(histogram_range)))
                    histogram_interval = histogram_range / category_cnt
                    current_app.logger.info("histogram params %s %s %s", histogram_range, histogram_interval, category_cnt)
        else:
            category_cnt = 1  #Heat mode effectively has one category
            doc_cnt = count_s.count()
            current_app.logger.info("Document Count: %s", doc_cnt)

        #If count is zero then return a null image
        if doc_cnt == 0:
            current_app.logger.debug("No points in bounding box")
            img = b""
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
            if category_field and max_bins < 65536:
                agg_zooms -= 1
            geotile_precision = current_zoom + agg_zooms

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
            sub_frame_level = geotile_precision - sub_frame_backout

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

                df = df.append(pd.DataFrame(convert(resp)), sort=False)
                
            s2 = time.time()
            current_app.logger.debug("ES took %s for %s" % ((s2-s1), len(df)))

            #Estimate the number of points per tile assuming uniform density
            estimated_points_per_tile = None
            if span_range == 'auto':
                num_tiles_at_level = sum( 1 for _ in mercantile.tiles(*doc_bounds, zooms=z, truncate=False) )
                estimated_points_per_tile = global_doc_cnt / num_tiles_at_level
                current_app.logger.debug("Doc Bounds %s %s %s %s", doc_bounds, z, num_tiles_at_level, estimated_points_per_tile)

            if len(df.index) == 0:
                img = b""
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
                            cmap=cc.glasbey_category10, 
                            color_key=create_color_key_hash_file(df["T"], map_filename), 
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
                    img = tf.shade(agg, cmap=getattr(cc, cmap, cc.bmy), how="log", span=span)

                ###############################################################
                # Common

                #Below zoom threshold spread to make individual dots large enough
                if spread is None or spread < 0:
                    # Automatic spread
                    spread_threshold = 11
                    if z >= spread_threshold:
                        spread_factor = math.floor(2 +(z-(spread_threshold-1))*.25)
                        img = tf.spread(img, spread_factor)
                else:
                    current_app.logger.info("Spreading by fixed %s", spread)
                    img = tf.spread(img, spread)

                img = img.to_bytesio().read()

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
