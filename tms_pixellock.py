#!/usr/bin/env python

from flask import Flask, Response
from flask import request, render_template, redirect
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

from datetime import datetime, timedelta
from pprint import pprint, pformat
import traceback

import datashader as ds
import pandas as pd
import colorcet as cc
import datashader.transfer_functions as tf

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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)
urllib3.disable_warnings(urllib3.exceptions.SNIMissingWarning)
urllib3.disable_warnings(UserWarning)


#from OpenSSL import SSL
import ssl

#Import helpers to assist with datashader
from datashader_helpers import sum_cat


default_justification = "Software Development Testing"
_color_key_map = []

flask_app = Flask(__name__)
config_lock = threading.Lock()
flask_app.config["index_config"] = (0, {})

def get_index_config(force=False, refresh_interval=60):
    #Handles multiprocess access to the index_config.  Checks for updates every 1 minute
    with config_lock:
        next_check, index_config = flask_app.config["index_config"]
        if (not index_config) or (time.time() >= next_check) or force:
            flask_app.logger.info("Reloading index config")
            try:
                with open(flask_app.config.get("index_config_file"), 'r') as stream:
                    index_config = yaml.safe_load(stream)
                    flask_app.config["index_config"] = ((time.time() + refresh_interval), index_config)
            except:
                flask_app.logger.exception("Error loading index config")
    return index_config

def get_connection_base():
    # TODO - this incorrectly assumes that proxy always implies HTTP an no-proxy is always HTTP
    if flask_app.config.get("proxy_host"):
        connection_base = "https://" + flask_app.config.get('proxy_host') + "/" + flask_app.config.get("proxy_prefix") + "/tms/"
    else:
        connection_base = "http://" + socket.getfqdn() + ":%s/tms/"%flask_app.config.get('port')

    return connection_base

@flask_app.route('/')
@flask_app.route('/index')
def index():
    #Calc Cache Size
    cache_size = subprocess.check_output(['du','-sh', flask_app.config["cache_directory"]]).split()[0].decode('utf-8')
    #TODO: Add other info?    
    return render_template('index.html', title='Status', cache_size=cache_size)

@flask_app.route('/config')
@flask_app.route('/display_config')
def display_config():
    cache_info = {}
    index_config = get_index_config()
    for c in index_config:
        tile_cache_path = os.path.join(flask_app.config["cache_directory"], c)
        if os.path.exists(tile_cache_path):
            try:
                cache_info[c] = subprocess.check_output(['du','-sh', os.path.join(flask_app.config["cache_directory"], c)]).split()[0].decode('utf-8')
            except OSError:
                cache_info[c] = "Error"
        else:
            cache_info[c] = "N/A"

    connection_base = get_connection_base()

    return render_template('display_config.html', config_contents = index_config, connection_base=connection_base, cache_info=cache_info)

@flask_app.route('/color_map', methods=['GET'])
def display_color_map():
    color_key_map = {}
    color_file = os.path.join(flask_app.config["cache_directory"]+"/%s/colormap.json"%(request.args.get('name')))
    if os.path.exists(color_file):
        with open(color_file, 'r') as c:
            color_key_map = yaml.safe_load(c)
    
    color_key_hash = {}
    for k in color_key_map.keys():
        color_key_hash[k] = int(hashlib.md5(k.encode('utf-8')).hexdigest()[0:2], 16)

    return render_template('color_map.html', color_key_map=color_key_map, color_key_hash=color_key_hash)

class ConfigForm(FlaskForm):
    name = wtforms.StringField('Name', description="Name of map layer", validators=[wtforms.validators.DataRequired()])
    idx = wtforms.StringField('Index', description="Index name", validators=[wtforms.validators.DataRequired()])
    daterange = wtforms.SelectField('Date Range', choices=[('1d', 'Yesterday'), ('7d', 'Last 7 Days'), ('30d', 'Last 30 Days'), ('all', 'All Time')] )
    mode = wtforms.SelectField('Mode', choices=[('heat', 'Heat Map'), ('category', 'Category Map')] )
    geopoint_field = wtforms.StringField('Geopoint Field', description="Required", validators=[wtforms.validators.DataRequired()])
    timestamp_field = wtforms.StringField('Timestamp Field', description="Optional, needed if Date Range is not All")
    category_field = wtforms.StringField('Category Field', description="Optional, needed if mode is category")
    justification_field = wtforms.StringField('Justification', description="Required, Justification for ES search", validators=[wtforms.validators.DataRequired()])
    submit = wtforms.SubmitField('Add Config')

@flask_app.route('/add_config', methods=['GET', 'POST'])
def add_config():
    index_config = get_index_config()
    form = ConfigForm()
    if form.validate_on_submit():
        cfg = {'idx':form.idx.data,
               'date_range':form.daterange.data,
               'mode':form.mode.data,
               'geopoint_field':form.geopoint_field.data,
               'timestamp_field':form.timestamp_field.data,
               'category_field':form.category_field.data,
               'justification':form.justification_field.data}
        index_config[form.name.data] = cfg
        
        #Store to file
        with open(flask_app.config.get("index_config_file"), 'w') as file:
            yaml.dump(index_config, file)
        
        return redirect('/display_config')

    return render_template('add_config.html', title='Add Config', form=form)

@flask_app.route('/remove_config', methods=['GET'])
def remove_config():
    index_config = get_index_config()
    if request.args.get('name') is not None:
        index_config.pop(request.args.get('name'), None)
        
        #Store to file
        with open(flask_app.config.get("index_config_file"), 'w') as file:
            yaml.dump(index_config, file)
        
        return redirect('/display_config')

    return render_template('add_config.html', title='Add Config', form=form)

@flask_app.route('/clear_cache', methods=['GET'])
def clear_cache():
    if request.args.get('name') is not None:
        #delete the cache
        tile_cache_path = os.path.join(flask_app.config.get("cache_directory"), request.args.get('name'))
        try:
            shutil.rmtree(tile_cache_path)
        except FileNotFoundError:
            pass
        flask_app.logger.warn("Recreating cache path %s", tile_cache_path)
        pathlib.Path(os.path.join(tile_cache_path)).mkdir(parents=True, exist_ok=True)
        return Response("Completed clearing cache for: %s"%(request.args.get('name')), status=200)
    return Response("Unknown config: %s"%(request.args.get('name')), status=500)

@flask_app.route('/tms/<config_name>/tile.json', methods=['GET'])
def get_tile_json(config_name):
    connection_base = get_connection_base()
    tiles_url = connection_base + config_name + "/{z}/{x}/{y}.png"

    tile_json = {
        "tilejson": "2.2.0",
        "name": config_name,
        "legend": "<ul><li>Item 1</li><li>Item 2</li></ul>", # TODO make this a legend the renders pretty
        "tiles": [
            tiles_url
        ],
    }

    data = json.dumps(tile_json)
    resp = Response(data, status=200)
    resp.headers['Content-Type'] = 'application/json'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.cache_control.max_age = 60

    return resp

@flask_app.route('/tms/<config_name>/<int:z>/<int:x>/<int:y>.png', methods=['GET'])
def get_tms(config_name, x, y, z):
    index_config = get_index_config()
    #Validate the request against the config
    if config_name not in index_config.keys():
        #Index not supported
        flask_app.logger.warning("Selected configuration is not in known configurations: %s"%(config_name))
        resp = Response("Selected configuration is not in known configurations: %s"%(config_name), status=500)
        return resp

    #Validate request is from proxy if proxy mode is enabled
    if flask_app.config.get("tms_key") is not None:
        if flask_app.config.get("tms_key") != request.headers.get("TMS_PROXY_KEY"):
            flask_app.logger.warning("TMS must be accessed via reverse proxy: keys %s != %s", flask_app.config.get("tms_key"), request.headers.get("TMS_PROXY_KEY"))
            resp = Response("TMS must be accessed via reverse proxy", status=403)
            return resp

    #Get params from config file
    idx = index_config.get(config_name, {}).get("idx", None)
    geopoint_field = index_config.get(config_name, {}).get("geopoint_field", None)
    timestamp_field = index_config.get(config_name, {}).get("timestamp_field", None)
    category_field = index_config.get(config_name, {}).get("category_field", None)
    
    #date_range = index_config.get(config_name, {}).get("date_range", None)
    mode = index_config.get(config_name, {}).get("mode", None)
    justification = index_config.get(config_name, {}).get('justification', default_justification)
    lucene_query = index_config.get(config_name, {}).get("lucene_query", None)
    from_time = index_config.get(config_name, {}).get("from_time", None)
    to_time = index_config.get(config_name, {}).get("to_time", "now")
    dsl_filter=index_config.get(config_name, {}).get("dsl_filter", None)
    cmap=index_config.get(config_name, {}).get("cmap", "bmy")

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

    # TMS tile coordinates
    x = int(x)
    y = int(y)
    z = int(z)

    #Handle time bounding
    now = datetime.utcnow()   
    stop_time = now
    if to_time:
        stop_time = convertKibanaTime(to_time, now)
    start_time = None
    if from_time:
        start_time = convertKibanaTime(from_time, now)

    start_time, stop_time = quantizeTimeRange(start_time, stop_time)


    #Calculate a hash value for the specific parameter set
    #These will likely be the things that are passed as arguments from Kibana in the eventual setup
    #This includes start_time + stop_time + lucene_query at the moment
    parameter_string = str(start_time)+str(stop_time)+str(dsl_filter)+str(lucene_query)
    parameter_hash = hashlib.md5(parameter_string.encode('utf-8')).hexdigest()
    flask_app.logger.debug("Parameters: (%s) %s"%(parameter_hash, parameter_string))

    c = get_cache( "/%s/%s/%s/%s/%s.png"%(config_name, parameter_hash, z, x, y), flask_app.config["cache_directory"])
    if c is not None and request.args.get('force') is None:
        flask_app.logger.info("Hit cache (%s), returning"%parameter_hash)
        #Return Cached Value
        img = c
    else:
        #Generate a tile
        if request.args.get('force') is not None:
            flask_app.logger.info("Forced cache flush, generating a new tile %s/%s/%s"%(z,x,y))
        else:
            flask_app.logger.info("No cache (%s), generating a new tile %s/%s/%s"%(parameter_hash,z,x,y))
        
        check_cache_dir(config_name)
        color_map_filename = os.path.join(flask_app.config["cache_directory"], config_name, "colormap.json")
        try:
            img = generate_tile(idx, x, y, z, 
                    geopoint_field=geopoint_field, time_field=timestamp_field, 
                    start_time=start_time, stop_time=stop_time,
                    category_field=category_field, map_filename=color_map_filename, cmap=cmap,
                    lucene_query=lucene_query, dsl_filter=dsl_filter,
                    max_bins=10000,  #TODO: Make this configurable
                    justification=justification )
        except:
            logging.exception("Exception Generating Tile")
            resp = Response("Exception Generating Tile", status=500)
            return resp
        
        set_cache("/%s/%s/%s/%s/%s.png"%(config_name, parameter_hash, z, x, y), img, flask_app.config["cache_directory"])

    resp = Response(img, status=200)
    resp.headers['Content-Type'] = 'image/png'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.cache_control.max_age = 60
    return resp

###########################################################################
def build_dsl_filter(filter_inputs):
    if len(filter_inputs) == 0:
        return None
    filter = {"filter":[{"match_all":{}}], "must_not":[]}
    for f in filter_inputs:
        if f.get("meta").get("negate"):
            filter["must_not"].append( f.get("query"))
        elif f.get("meta").get("disabled") in ("true", True):
            continue
        else:
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
    #Can be either a ISO time string or a now-XXX style string
    try:
        t = datetime.fromisoformat(time_string)
        return t
    except:
        pass

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
                flask_app.logging.error("%s is not a valid time offset" % unit)
                return None     
        elif time_string.startswith("now+"):
            flask_app.logging.error("now+ time strings are not currently supported")
            return None
    
    flask_app.logging.error("Unknown time string %s"%time_string)
    return None

class GeotileGrid(Bucket):
    name = 'geotile_grid'

def get_cache(tile, cache_dir, lifespan=60*60):
    #See if tile exists
    if os.path.exists(os.path.join(cache_dir+tile)):
        #TODO: check if its too old
        with open(os.path.join(cache_dir+tile), 'rb') as i:
            return i.read()
    return None

def set_cache(tile, img, cache_dir):
    pathlib.Path(os.path.dirname(os.path.join(cache_dir+tile))).mkdir(parents=True, exist_ok=True) 
    with open(os.path.join(cache_dir+tile), 'wb') as i:
            i.write(img)

def check_cache_dir(layer_name):
    tile_cache_path = os.path.join(flask_app.config.get("cache_directory"), layer_name)
    if not os.path.exists(tile_cache_path):
        pathlib.Path(os.path.join(tile_cache_path)).mkdir(parents=True, exist_ok=True)

def check_cache_dirs():
    for c in get_index_config():
        check_cache_dir(c)

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

def generate_sub_frames(level, bb_dict):
    l = int(math.pow(2, level)) #l = expansion in each dimensions
    subframes = []
    lon_range = bb_dict["bottom_right"]["lon"] - bb_dict["top_left"]["lon"]
    lat_range = bb_dict["top_left"]["lat"] - bb_dict["bottom_right"]["lat"]
    
    lon_origin = bb_dict["top_left"]["lon"]
    lat_origin = bb_dict["bottom_right"]["lat"]
    for lon_i in range(l):
        for lat_i in range(l):
            subframes.append({
                    "top_left": {
                        "lat": lat_origin + ((lat_i+1)/l)*lat_range,
                        "lon": lon_origin + (lon_i/l)*lon_range,
                    },
                    "bottom_right": {
                        "lat": lat_origin + (lat_i/l)*lat_range,
                        "lon": lon_origin + ((lon_i+1)/l)*lon_range,
                    }
            })

    return subframes

color_key_hash_lock = threading.Lock()
def create_color_key_hash_file(categories, color_file, cmap='glasbey_light'):
    with color_key_hash_lock:
        color_key_map = {}
        
        #See if you need to load the file
        if os.path.exists(color_file):
            #Load the file
            with open(color_file, 'r') as c:
                color_key_map = yaml.safe_load(c)        
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
            with open(color_file, 'w') as f:
                fcntl.lockf(f, fcntl.LOCK_EX)
                try:
                    yaml.dump(color_key_map, f)
                finally:
                    fcntl.lockf(f, fcntl.LOCK_UN)

        return color_key

def generate_tile(idx, x, y, z, 
                    geopoint_field="location", time_field='@timestamp', 
                    start_time=None, stop_time=None,
                    category_field=None, map_filename=None, cmap='bmy',
                    lucene_query=None, dsl_filter=None,
                    max_bins=10000,
                    justification=default_justification ):
    
    flask_app.logger.debug("Generating tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s"%(idx, z, x, y, geopoint_field, time_field, category_field, start_time, stop_time))
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
            flask_app.config.get("elastic"),
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

        #Get global document count for this index
        global_doc_cnt = base_s.count()

        # See how many documents are in the bounding box
        count_s = copy.copy(base_s)
        count_s = count_s.filter("geo_bounding_box",
                **{
                    geopoint_field: bb_dict
                } )
        doc_cnt = count_s.count()


        if category_field:
            #Also need to calculate the number of categories
            count_s = count_s.params(size=0)
            count_s.aggs.metric('term_count','cardinality',field=category_field)
            resp = count_s.execute()
            assert len(resp.hits) == 0
            category_cnt = 0
            if hasattr(resp.aggregations, "term_count"):
                category_cnt = resp.aggregations.term_count.value
                if category_cnt <= 0:
                    category_cnt = 1
            flask_app.logger.debug("Document Count: %s, Category Count: %s"%(doc_cnt, category_cnt))
        else:
            category_cnt = 1  #Heat mode effectively has one category

        #If count is zero then return a null image
        flask_app.logger.debug("Count: %s"%doc_cnt)
        if doc_cnt == 0:
            flask_app.logger.debug("No points in bounding box")
            img = b""
        else:
            # Find number of pixels in required image
            pixels = tile_height_px * tile_width_px
            if category_field:
                #Spread the category mode x3
                pixels = pixels/9.0
            
            sub_frame_level = math.ceil( math.log( (pixels*category_cnt) /max_bins,4) )
            flask_app.logger.debug("SubFrame math: %spx, %s subframe level"%(pixels, sub_frame_level) )
            current_zoom = z
            #max_zooms = int(math.log(max_bins, 2) / 2)  #TODO: Confirm this is not correct
            max_zooms = int(math.log(max_bins, 4))
            geotile_precision = current_zoom + max_zooms + sub_frame_level 
            flask_app.logger.debug("GeoTile Zoom Info: current %s, max %s, sub frame level %s, precision %s"% (current_zoom, max_zooms, sub_frame_level, geotile_precision) )

            #generate n subframe bounding boxes
            subframes = generate_sub_frames(sub_frame_level, bb_dict)
            df = pd.DataFrame()
            s1 = time.time()
            for _, subframe in enumerate(subframes):
                subframe_s = copy.copy(base_s)
                subframe_s = subframe_s.params(size=0)
                subframe_s = subframe_s.filter("geo_bounding_box",
                                **{
                                    geopoint_field: subframe
                                }  )
                
                #Set up the aggregations and the dataframe extraction
                if category_field:  #Category Mode  
                    subframe_s.aggs.bucket(
                        'categories',
                        'terms',
                        field=category_field,
                    ).bucket(
                        'grids',
                        'geotile_grid',
                        field=geopoint_field,
                        precision=geotile_precision,
                    ).metric(
                        'centroid',
                        'geo_centroid',
                        field=geopoint_field
                    )                
                else: #Heat Mode
                    subframe_s.aggs.bucket(
                        'grids',
                        'geotile_grid',
                        field=geopoint_field,
                        precision=geotile_precision,
                        size=max_bins   #TODO:  Is this needed for the category mode?  Is precision sufficient
                    ).metric(
                        "centroid",
                        'geo_centroid',
                        field=geopoint_field
                    )        
                
                resp = subframe_s.execute()
                assert len(resp.hits) == 0
                df = df.append(pd.DataFrame(convert(resp)), sort=False)
                
            s2 = time.time()
            flask_app.logger.debug("ES took %s for %s" % ((s2-s1), len(df)))

            if len(df.index) == 0:
                img = b""
            else:
                if category_field: #Category Mode
                    df["T"] = df["t"].astype('category')
                    agg = ds.Canvas(
                        plot_width=tile_width_px,
                        plot_height=tile_height_px,
                        x_range=x_range,
                        y_range=y_range
                    ).points(df, 'x', 'y', agg=sum_cat('T', 'c'))
                    
                    #Estimate the number of points per tile assuming uniform density
                    num_tiles_at_level = 2**z
                    num_bins_at_level = num_tiles_at_level * pixels
                    estimated_bin_cnt = global_doc_cnt / num_bins_at_level
                    min_span = 0
                    
                    #Increase min_alpha as zoom levels increase
                    if estimated_bin_cnt < 0.1:
                        min_alpha = 200
                        max_span = 1
                        spread_factor = 2
                    else:
                        if z <= 6:
                            max_span = math.ceil( math.log(estimated_bin_cnt * 2) )
                            spread_factor = 1
                        elif z <= 9:
                            max_span = math.ceil( math.log(estimated_bin_cnt * 2) )
                            spread_factor = 1
                        elif z <= 11:
                            max_span = math.ceil( math.log(estimated_bin_cnt * 2) )
                            spread_factor = 2
                        else:
                            max_span = math.ceil( math.log(estimated_bin_cnt * 2) )
                            spread_factor = 3
                        if max_span <= 0:
                            max_span = 1
                        #Increase dynamic range for larger spans
                        alpha_span = int(max_span) * 25
                        min_alpha = 255 - min(alpha_span, 225)

                    flask_app.logger.debug("MinAlpha:%s MaxSpan:%s Spread:%s z:%s GlobalDocs:%s Docs:%s", min_alpha, max_span, spread_factor, z, global_doc_cnt, doc_cnt)
                    img = tf.shade(
                            agg, 
                            cmap=cc.glasbey_category10, 
                            color_key=create_color_key_hash_file(df["T"], map_filename), 
                            min_alpha=min_alpha,
                            how="log",
                            span=[min_span, max_span])

                    #Spread to reduce pixel count needs
                    if spread_factor > 1:
                        img = tf.spread(img, spread_factor)
                    
                else: #Heat Mode
                    agg = ds.Canvas(
                        plot_width=tile_width_px,
                        plot_height=tile_height_px,
                        x_range=x_range,
                        y_range=y_range
                    ).points(df, 'x', 'y', agg=ds.sum('c'))
                    
                    img = tf.shade(agg, cmap=getattr(cc, cmap, cc.bmy), how="log", span=[0,500])

                    #Below zoom threshold spread to make individual dots large enough
                    spread_threshold = 11
                    if z >= spread_threshold:
                        spread_factor = math.floor(2 +(z-(spread_threshold-1))*.25)
                        print("Spreading by %s, z=%s"%(spread_factor, z))
                        img = tf.spread(img, spread_factor)

                img = img.to_bytesio().read()

        #Set headers and return data 
        return img
    except Exception:
        flask_app.logger.exception("An exception occured while attempting to generate a tile:")
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TMS Server with Cache')
    parser.add_argument('--debug', default=False, action='store_true', help="Enable Flask debug mode")
    parser.add_argument('-d', '--cache_directory', default='./tms-cache/', help="Directory for tile cache")
    parser.add_argument('-f', '--index_config_file', default='./index_config.yaml', help="YAML file containing information about each index")
    parser.add_argument('-t', '--cache_timeout', default=60*60, help="Cache lifespan in sec")
    parser.add_argument('-e', '--elastic', default=None, help="Elasticsearch URL")
    parser.add_argument('-p', '--port', default=5000, help="Port to run TMS server")
    parser.add_argument('-n', '--num_processes', default=32, help="Number of concurrent Flask processes to run")

    #Reverse Proxy Modes
    parser.add_argument('-H', '--proxy_host', default=None, help="Proxy host")
    parser.add_argument('-P', '--proxy_prefix', default="", help="Proxy prefix")
    parser.add_argument('-k', '--tms_key', default=None, help="TMS key required in header")
    
    #SSL Modes
    parser.add_argument('--ssl_adhoc', default=False, action='store_true', help="Enable SSL in ad-hoc mode")
    parser.add_argument('-s', '--ssl', default=False, action='store_true', help="Enable SSL, set environment variables to confgure: \
                                                                                SSL_SERVER_KEY, SSL_SERVER_CERT, SSL_CA_CHAIN")
    args = parser.parse_args()

    #Flask App Configuration
    for k,v in vars(args).items():
        flask_app.config[k] = v
    flask_app.config["SECRET_KEY"] = 'CSRFProtectionKey'

    #Limit logging at INFO, reduce if needed for debugging
    flask_app.logger.setLevel(logging.INFO)        

    #Create cache directories for all layers
    check_cache_dirs()

    #TODO: Figure out colormap

    #Set all the flask arguments as a dictionary
    flask_args = {}
    flask_args["host"] = "0.0.0.0"
    flask_args["port"] = flask_app.config.get("port", 5000)
    flask_args["processes"] = flask_app.config.get("num_processes", 32)
    flask_args["threaded"] = False


    #Handle Debug
    if flask_app.config.get("debug"):
        flask_args["debug"] = True
        flask_app.logger.setLevel(logging.DEBUG)
        flask_args["processes"] = 1

    #Handle SSL
    if args.ssl_adhoc:
        flask_args["ssl_context"] = 'adhoc'
    elif args.ssl:
        flask_args["ssl_context"] = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        flask_args["ssl_context"].load_verify_locations(os.environ.get("SSL_CA_CHAIN"))
        flask_args["ssl_context"].load_cert_chain(os.environ.get("SSL_SERVER_CERT"), os.environ.get("SSL_SERVER_KEY") )

    #Run Flask
    flask_app.run(**flask_args)
