from functools import lru_cache

import copy
import math
import time
import json

import mercantile
import pynumeral
import colorcet as cc
import datashader as ds
import pandas as pd
from datashader import reductions as rd, transfer_functions as tf
from elasticsearch_dsl import AttrList, AttrDict, A
from elasticsearch_dsl.aggs import Bucket
from flask import current_app, request
from numpy import pi
from datashader.utils import lnglat_to_meters
import numpy as np

import elastic_datashader.helpers.mercantile_util as mu
from elastic_datashader.helpers.drawing import (
    ellipse,
    gen_empty,
    gen_overlay,
    create_color_key,
    gen_debug_overlay,
    generate_ellipse_points
)
from elastic_datashader.helpers.elastic import (
    get_search_base,
    convert,
    convert_composite,
    split_fieldname_to_list,
    get_nested_field_from_hit,
    to_32bit_float,
    ScanAggs,
    get_tile_categories,
    scan
)
from elastic_datashader.helpers.pandas_util import simplify_categories

NAN_LINE = {"x": None, "y": None, "c": "None"}


class GeotileGrid(Bucket):
    name = "geotile_grid"


def create_datashader_ellipses_from_search(
    search,
    geopoint_fields,
    maximum_ellipses_per_tile,
    search_meters,
    metrics=None,
    histogram_interval=None,
    category_format=None
):
    """

    :param search:
    :param geopoint_fields:
    :param maximum_ellipses_per_tile:
    :param search_meters:
    :param metrics:
    :param histogram_interval:
    :return:
    """
    if metrics is None:
        metrics = {}
    metrics.update({"over_max": False, "hits": 0, "locations": 0})

    geopoint_center = geopoint_fields["geopoint_center"]
    ellipse_major = geopoint_fields["ellipse_major"]
    ellipse_minor = geopoint_fields["ellipse_minor"]
    ellipse_tilt = geopoint_fields["ellipse_tilt"]
    ellipse_units = geopoint_fields["ellipse_units"]
    category_field = geopoint_fields.get("category_field")
    category_type = geopoint_fields.get("category_type")

    _geopoint_center = split_fieldname_to_list(geopoint_center)
    _ellipse_major = split_fieldname_to_list(ellipse_major)
    _ellipse_minor = split_fieldname_to_list(ellipse_minor)
    _ellipse_tilt = split_fieldname_to_list(ellipse_tilt)

    if category_field:
        category_field = split_fieldname_to_list(category_field)

    timeout_at = None
    if current_app.config["QUERY_TIMEOUT"]:
        timeout_at = time.time() + current_app.config["QUERY_TIMEOUT"]
        search = search.params(timeout="%ds" % current_app.config["QUERY_TIMEOUT"])

    for i, hit in enumerate(scan(search, use_scroll=current_app.config.get("USE_SCROLL", False))):
        if timeout_at and (time.time() > timeout_at):
            current_app.logger.warning("ellipse generation hit query timeout")
            metrics["aborted"] = True
            break

        metrics["hits"] += 1
        # NB. this actually isn't maximum ellipses per tile, but rather
        # maximum number of records iterated.  We might want to keep this behavior
        # because if you ask for ellipses on a index where none of the records have ellipse
        # point fields you could end up iterating over the entire index
        if i >= maximum_ellipses_per_tile:
            metrics["over_max"] = True
            break

        # Get all the ellipse fields
        locs = get_nested_field_from_hit(hit, _geopoint_center, None)
        majors = get_nested_field_from_hit(hit, _ellipse_major, None)
        minors = get_nested_field_from_hit(hit, _ellipse_minor, None)
        angles = get_nested_field_from_hit(hit, _ellipse_tilt, None)

        # Check that we have all the fields
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

        # If its a list determine if there are multiple geos or just a single geo in list format
        if isinstance(locs, list) or isinstance(locs, AttrList):
            if (
                len(locs) == 2
                and isinstance(locs[0], float)
                and isinstance(locs[1], float)
            ):
                locs = [locs]
                majors = [majors]
                minors = [minors]
                angles = [angles]
        else:
            # All other cases are single ellipses
            locs = [locs]
            majors = [majors]
            minors = [minors]
            angles = [angles]

        # verify same length
        if not (len(locs) == len(majors) == len(minors) == len(angles)):
            current_app.logger.warning(
                "ellipse parameters and length are not consistent"
            )
            continue

        # process each ellipse
        for ii in range(len(locs)):
            loc = locs[ii]

            if isinstance(loc, str):
                if "," not in loc:
                    current_app.logger.warning(
                        "skipping loc with invalid str format %s", loc
                    )
                    continue
                lat, lon = loc.split(",", 1)
                loc = dict(lat=float(lat), lon=float(lon))
            elif isinstance(loc, list) or isinstance(loc, AttrList):
                if len(loc) != 2:
                    current_app.logger.warning(
                        "skipping loc with invalid list format %s", loc
                    )
                    continue
                lon, lat = loc
                loc = dict(lat=float(lat), lon=float(lon))
            elif not (isinstance(loc, dict) or isinstance(loc, AttrDict)):
                current_app.logger.warning(
                    "skipping loc with invalid format %s %s %s",
                    loc,
                    isinstance(loc, list),
                    type(loc),
                )
                continue

            major = majors[ii]
            minor = minors[ii]
            angle = angles[ii]

            try:
                major = float(major)
            except ValueError:
                current_app.logger.warning(
                    "skipping major with invalid major %s %s",
                    major,
                    type(major),
                )
                continue

            try:
                minor = float(minor)
            except ValueError:
                current_app.logger.warning(
                    "skipping minor with invalid minor %s %s",
                    minor,
                    type(minor),
                )
                continue

            try:
                angle = float(angle)
            except ValueError:
                current_app.logger.warning(
                    "skipping angle with invalid angle %s %s",
                    angle,
                    type(angle),
                )
                continue

            # Handle deg->Meters conversion and everything else
            x0, y0 = lnglat_to_meters(loc["lon"], loc["lat"])
            if ellipse_units == "majmin_nm":
                major = major * 1852  # nm to meters
                minor = minor * 1852  # nm to meters
            elif ellipse_units == "semi_majmin_nm":
                major = major * (2 * 1852)  # nm to meters, semi to full
                minor = minor * (2 * 1852)  # nm to meters, semi to full
            elif ellipse_units == "semi_majmin_m":
                major = major * 2  # semi to full
                minor = minor * 2  # semi to full
            # NB. assume "majmin_m" if any others

            # expel above CEP limit
            if major > search_meters or minor > search_meters:
                continue

            ellipse_render_mode = current_app.config["ELLIPSE_RENDER_MODE"]
            if ellipse_render_mode == "simple":
                angle_rad = angle * ((2.0 * pi) / 360.0)  # Convert degrees to radians
                Y, X = ellipse(
                    major / 2.0, minor / 2.0, angle_rad, y0, x0, num_points=16
                )
            elif ellipse_render_mode == "matrix":
                LAT, LON = generate_ellipse_points(
                    loc["lat"],
                    loc["lon"],
                    major / 2.0,
                    minor / 2.0,
                    tilt=angle,
                    n_points=current_app.config["NUM_ELLIPSE_POINTS"]
                )
                X, Y = lnglat_to_meters(LON, LAT)
            else:
                raise ValueError("invalid ellipse render mode %s", ellipse_render_mode)

            if category_field:
                if histogram_interval:
                    # Do quantization
                    raw = get_nested_field_from_hit(hit, category_field, 0.0)
                    if isinstance(raw, list):
                        C = []
                        for v in raw:
                            if category_type == "number" or type(v) in (int, float):                                
                                quantized = (
                                    math.floor(float(v) / histogram_interval) * histogram_interval
                                )
                                C.append( str(to_32bit_float(quantized)) )
                            else:
                                C.append( str(v) )
                    else:
                        if category_type == "number" or type(raw) in (int, float):                             
                            quantized = (
                                math.floor(raw / histogram_interval) * histogram_interval
                            )
                            C = [ str(to_32bit_float(quantized)) ]
                        else:
                            C = [ str(raw) ]
                else:
                    #If a number type, quantize it down to a 32-bit float so it matches what the legend will show
                    v = get_nested_field_from_hit(hit, category_field, "N/A")
                    if category_type == "number" or type(v) in (int, float):
                        if category_format:
                            C = [ pynumeral.format(to_32bit_float(v), category_format)]
                        else:
                            C = [ str(to_32bit_float(v)) ]
                    else:
                        # Just use the value
                        if not isinstance(v, (list, AttrList)):
                            C = [ str(v) ]
                        else:
                            C = [ str(vv) for vv in v ]
            else:
                C = [ "None" ]

            if len(C) > 100:
                current_app.logger.warning("truncating category list of size %s to first 100 categories", len(C))
                C = C[0:100]
            
            for c in C:
                for p in zip(X, Y):
                    yield {"x": p[0], "y": p[1], "c": c}
            yield NAN_LINE  # Break between ellipses
            metrics["locations"] += 1


def create_datashader_tracks_from_search(
    search,
    geopoint_fields,
    maximum_hits_per_tile,
    metrics=None,
    histogram_interval=None,
    category_format=None
):
    """

    :param search:
    :param geopoint_fields:
    :param maximum_hits_per_tile:
    :param metrics:
    :param histogram_interval:
    :return:
    """
    if metrics is None:
        metrics = {}
    metrics.update({"over_max": False, "hits": 0, "locations": 0})

    geopoint_center = geopoint_fields["geopoint_center"]
    category_field = geopoint_fields.get("category_field")
    track_connection = geopoint_fields.get("track_connection")
    category_type = geopoint_fields.get("category_type")

    category_set = set()

    _geopoint_center = split_fieldname_to_list(geopoint_center)

    if category_field:
        category_field = split_fieldname_to_list(category_field)
    if track_connection:
        track_connection = split_fieldname_to_list(track_connection)

    timeout_at = None
    if current_app.config["QUERY_TIMEOUT"]:
        timeout_at = time.time() + current_app.config["QUERY_TIMEOUT"]
        search = search.params(timeout="%ds" % current_app.config["QUERY_TIMEOUT"])

    for i, hit in enumerate(scan(search, use_scroll=current_app.config.get("USE_SCROLL", False))):
        if timeout_at and (time.time() > timeout_at):
            current_app.logger.warning("track generation hit query timeout")
            metrics["aborted"] = True
            break

        metrics["hits"] += 1
        # NB. this actually isn't maximum ellipses per tile, but rather
        # maximum number of records iterated.  We might want to keep this behavior
        # because if you ask for ellipses on a index where none of the records have ellipse
        # point fields you could end up iterating over the entire index
        if i >= maximum_hits_per_tile:
            metrics["over_max"] = True
            break

        # Get all the ellipse fields
        locs = get_nested_field_from_hit(hit, _geopoint_center, None)

        # Check that we have all the fields
        if locs is None:
            current_app.logger.debug("hit field %s has no values", geopoint_center)
            continue

        # If its a list determine if there are multiple geos or just a single geo in list format
        if isinstance(locs, list) or isinstance(locs, AttrList):
            if (
                len(locs) == 2
                and isinstance(locs[0], float)
                and isinstance(locs[1], float)
            ):
                locs = [locs]
        else:
            # All other cases are single ellipses
            locs = [locs]

        # process each ellipse
        for ii in range(len(locs)):
            loc = locs[ii]
            if isinstance(loc, str):
                if "," not in loc:
                    current_app.logger.warning(
                        "skipping loc with invalid str format %s", loc
                    )
                    continue
                lat, lon = loc.split(",", 1)
                loc = dict(lat=float(lat), lon=float(lon))
            elif isinstance(loc, list) or isinstance(loc, AttrList):
                if len(loc) != 2:
                    current_app.logger.warning(
                        "skipping loc with invalid list format %s", loc
                    )
                    continue
                lon, lat = loc
                loc = dict(lat=float(lat), lon=float(lon))
            elif not (isinstance(loc, dict) or isinstance(loc, AttrDict)):
                current_app.logger.warning(
                    "skipping loc with invalid format %s %s %s",
                    loc,
                    isinstance(loc, list),
                    type(loc),
                )
                continue

            # Handle deg->Meters conversion and everything else
            x0, y0 = lnglat_to_meters(loc["lon"], loc["lat"])

            if category_field:
                if histogram_interval:
                    # Do quantization
                    raw = get_nested_field_from_hit(hit, category_field, 0.0)
                    quantized = (
                        math.floor(raw / histogram_interval) * histogram_interval
                    )
                    C = [ str(to_32bit_float(quantized)) ]
                else:
                    #If a number type, quantize it down to a 32-bit float so it matches what the legend will show
                    v = get_nested_field_from_hit(hit, category_field, "N/A")
                    if category_type == "number" or type(v) in (int, float):
                        if category_format:
                            C = [ pynumeral.format(to_32bit_float(v), category_format)]
                        else:
                            C = [ str(to_32bit_float(v)) ]
                    else:
                        # Just use the value
                        if isinstance(v, list):
                            C = v
                        else:
                            C = [ v ]
            else:
                C = [ "None" ]
            if len(C) > 100:
                current_app.logger.warning("truncating category list of size %s to first 100 categories", len(C))
                C = C[0:100]

            #Handle tracking field
            if track_connection:
                v = get_nested_field_from_hit(hit, track_connection, "N/A")
                # Just use the value
                if isinstance(v, list):
                    T = v
                else:
                    T = [ v ]
            else:
                T = [ "None" ]

            category_set.update(C)
            for c in C:
                category_set.update(C)
                for t in T:
                    yield {"x": x0, "y": y0, "c": c, "t": t}
            #yield NAN_LINE  # Break between ellipses
            metrics["locations"] += 1

    #for c in category_set:
    #    yield {"x": None, "y": None, "c": c, "t": None}

def generate_nonaggregated_tile(
    idx, x, y, z, params, tile_height_px=256, tile_width_px=256
):
    # Handle legacy parameters
    geopoint_field = params["geopoint_field"]
    timestamp_field = params["timestamp_field"]
    start_time = params["start_time"]
    stop_time = params["stop_time"]
    category_field = params["category_field"]
    category_type = params["category_type"]
    category_format = params["category_format"]
    highlight = params["highlight"]
    cmap = params["cmap"]
    spread = params["spread"]
    span_range = params["span_range"]
    spread = params["spread"]
    lucene_query = params["lucene_query"]
    dsl_query = params["dsl_query"]
    dsl_filter = params["dsl_filter"]
    ellipse_major = params["ellipse_major"]
    ellipse_minor = params["ellipse_minor"]
    ellipse_tilt = params["ellipse_tilt"]
    ellipse_units = params["ellipse_units"]
    search_distance = params["search_distance"]
    filter_distance = params["filter_distance"]
    track_connection = params["track_connection"]
    max_batch = params["max_batch"]
    max_bins = params["max_bins"]
    max_ellipses_per_tile = params["max_ellipses_per_tile"]
    histogram_interval = params.get("generated_params", {}).get(
        "histogram_interval", None
    )
    global_doc_cnt = params.get("generated_params", {}).get("global_doc_cnt", None)
    global_bounds = params.get("generated_params", {}).get("global_bounds", None)
    field_max = params.get("generated_params", {}).get("field_max", None)
    field_min = params.get("generated_params", {}).get("field_min", None)
    render_mode = params["render_mode"]

    current_app.logger.info(
        "Generating non-aggegated (%s) tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s"
        % (
            render_mode,
            idx,
            z,
            x,
            y,
            geopoint_field,
            timestamp_field,
            category_field,
            start_time,
            stop_time,
        )
    )
    try:
        # Get the web mercador bounds for the tile
        xy_bounds = mu.xy_bounds(x, y, z)
        # Calculate the x/y range in meters
        x_range = xy_bounds[0], xy_bounds[2]
        y_range = xy_bounds[1], xy_bounds[3]
        # Swap the numbers so that [0] is always lowest
        if x_range[0] > x_range[1]:
            x_range = x_range[1], x_range[0]
        if y_range[0] > y_range[1]:
            y_range = y_range[1], y_range[0]

        # Expand this by search_distance value to get adjacent geos that overlap into our tile
        search_meters = search_distance * 1852
        if filter_distance is not None:
            filter_meters = filter_distance * 1852
        else:
            filter_meters = search_meters

        boundary_extension = search_meters*1.5 #Search slightly beyond to reduce literal corner cases

        # Get the top_left/bot_rght for the tile
        top_left = mu.lnglat(
            x_range[0] - boundary_extension, y_range[1] + boundary_extension
        )
        bot_rght = mu.lnglat(
            x_range[1] + boundary_extension, y_range[0] - boundary_extension
        )

        bb_dict = {
            "top_left": {
                "lat": min(90, max(-90, top_left[1])),
                "lon": min(180, max(-180, top_left[0])),
            },
            "bottom_right": {
                "lat": min(90, max(-90, bot_rght[1])),
                "lon": min(180, max(-180, bot_rght[0])),
            },
        }

        # Figure out how big the tile is in meters
        xwidth = x_range[1] - x_range[0]
        yheight = y_range[1] - y_range[0]
        # And now the area of the tile
        area = xwidth * yheight

        # Create base search
        base_s = get_search_base(current_app.config.get("ELASTIC"), params, idx).params(
            size=max_batch
        )

        # Add expanded bounding box
        count_s = copy.copy(base_s)
        count_s = count_s.filter("geo_bounding_box", **{geopoint_field: bb_dict})

        # trim category field postfixes
        if category_field:
            if category_field.endswith(".keyword"):
                category_field = category_field[: -len(".keyword")]
            elif category_field.endswith(".raw"):
                category_field = category_field[: -len(".raw")]

        # Process the hits (geos) into a list of points
        s1 = time.time()
        metrics = dict(over_max=False)
        if render_mode == "ellipses":
            geopoint_fields = {
                "geopoint_center": geopoint_field,
                "ellipse_major": ellipse_major,
                "ellipse_minor": ellipse_minor,
                "ellipse_tilt": ellipse_tilt,
                "ellipse_units": ellipse_units,
                "category_field": category_field,
            }
            includes_fields = list(
                filter(
                    lambda x: x is not None,
                    [
                        geopoint_field,
                        ellipse_major,
                        ellipse_minor,
                        ellipse_tilt,
                        category_field,
                    ],
                )
            )
            count_s = count_s.source(includes=includes_fields)
            df = pd.DataFrame.from_dict(
                create_datashader_ellipses_from_search(
                    count_s,
                    geopoint_fields,
                    max_ellipses_per_tile,
                    search_meters,
                    metrics,
                    histogram_interval,
                    category_format
                )
            )
            df_points = None
        else:
            geopoint_fields = {
                "geopoint_center": geopoint_field,
                "track_connection": track_connection,
                "category_field": category_field
            }
            includes_fields = list(
                filter(
                    lambda x: x is not None,
                    [
                        geopoint_field,
                        track_connection,
                        category_field,
                    ],
                )
            )
            df = pd.DataFrame.from_dict(
                create_datashader_tracks_from_search(
                    count_s,
                    geopoint_fields,
                    max_ellipses_per_tile,
                    metrics,
                    histogram_interval,
                    category_format
                )
            )

            #Sort by category (if used) and then tracking value
            if len(df) != 0:
                if category_field:
                    df.sort_values(["c","t"], inplace=True)
                else:
                    df.sort_values(["t"], inplace=True)

            #Now we need to iterate through the list so far and separate by different colors/distances
            split_dicts = []
            start_points_dicts = []
            current_track = []
            track_distance = 0.0
            blank_row = {"x": np.nan, "y": np.nan, "c": None, "t": None}
            old_row = blank_row
            for index, row in df.iterrows():
                if old_row.get("c") != row.get("c"):
                    #new category, so insert space in the tracks dicts and add to the start dicts
                    if track_distance > filter_meters:
                        split_dicts = split_dicts + current_track
                        split_dicts.append(blank_row)
                        start_points_dicts.append(old_row)
                    current_track = []
                    track_distance = 0
                elif not np.isnan(row.get("x")) and \
                        not np.isnan(row.get("y")) and \
                        not np.isnan(old_row.get("x")) and \
                        not np.isnan(old_row.get("y")) :
                    distance = np.sqrt(np.power(row.get("x")-old_row.get("x"), 2)+np.power(row.get("y")-old_row.get("y"), 2))
                    if distance > search_meters:
                        #These points are too far apart, split them as different tracks if total track length is acceptable
                        if track_distance > filter_meters:
                            split_dicts = split_dicts + current_track
                            split_dicts.append(blank_row)
                            start_points_dicts.append(old_row)
                        current_track = []
                        track_distance = 0
                    else:
                        track_distance += distance
                current_track.append(dict(row))
                old_row = row
            
            #last one is always an end-point if the track was long enough
            if track_distance > filter_meters:
                split_dicts = split_dicts + current_track
                split_dicts.append(blank_row)
                start_points_dicts.append(old_row)

            df = pd.DataFrame.from_dict(split_dicts)
            df_points = pd.DataFrame.from_dict(start_points_dicts)
        s2 = time.time()

        current_app.logger.debug(
            "ES took %s for locations: %s   hits: %s",
            (s2 - s1),
            metrics.get("locations", 0),
            metrics.get("hits", 0),
        )
        metrics["query_time"] = (s2 - s1)

        # Estimate the number of points per tile assuming uniform density
        estimated_points_per_tile = None
        if (span_range == "auto" or span_range is None):
            if global_bounds:
                num_tiles_at_level = mu.num_tiles(*global_bounds, z)
                estimated_points_per_tile = global_doc_cnt / num_tiles_at_level
                current_app.logger.debug(
                    "Doc Bounds %s %s %s %s",
                    global_bounds,
                    z,
                    num_tiles_at_level,
                    estimated_points_per_tile,
                )
            else:
                current_app.logger.warning(
                    "Cannot estimate points per tile because bounds are missing"
                )
                estimated_points_per_tile = 100000

        # If count is zero then return a null image
        if len(df) == 0:
            current_app.logger.debug("No points in bounding box")
            img = gen_empty(tile_width_px, tile_height_px)
            if metrics.get("over_max"):
                img = gen_overlay(img, color=(128, 128, 128, 128))
            elif metrics.get("aborted"):
                img = gen_overlay(img, color=(128, 128, 128, 128))
            if params.get("debug"):
                img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
        else:
            # Find number of pixels in required image
            pixels = tile_height_px * tile_width_px

            categories = [ x for x in df["c"].unique() if x != None ]
            metrics["categories"] = json.dumps(categories)

            # Generate the image
            df["c"] = df["c"].astype("category")
            color_key=create_color_key(
                categories,
                cmap=cmap,
                highlight=highlight,
                field_min=field_min,
                field_max=field_max,
                histogram_interval=histogram_interval
            )
            # prevent memory explosion in datashader _colorize
            _, color_key = simplify_categories(
                df,
                "c",
                color_key,
                inplace=True,
            )
            agg = ds.Canvas(
                plot_width=tile_width_px,
                plot_height=tile_height_px,
                x_range=x_range,
                y_range=y_range,
            ).line(df, "x", "y", agg=rd.count_cat("c"))

            #now for the points as well
            points_agg = None
            if df_points is not None:
                df_points["c"] = df_points["c"].astype("category")
                # prevent memory explosion in datashader _colorize
                _, points_color_key = simplify_categories(
                    df_points,
                    "c",
                    color_key,
                    inplace=True,
                )
                points_agg = ds.Canvas(
                    plot_width=tile_width_px,
                    plot_height=tile_height_px,
                    x_range=x_range,
                    y_range=y_range,
                ).points(df_points, "x", "y", agg=rd.count_cat("c"))

            span = None
            if span_range == "flat":
                min_alpha = 255
            elif span_range == "narrow":
                span = [0, math.log(1e3)]
                min_alpha = 200
            elif span_range == "normal":
                span = [0, math.log(1e6)]
                min_alpha = 100
            elif span_range == "wide":
                span = [0, math.log(1e9)]
                min_alpha = 50
            else:
                assert estimated_points_per_tile is not None
                span = [0, math.log(max(estimated_points_per_tile * 2, 2))]
                alpha_span = int(span[1]) * 25
                min_alpha = 255 - min(alpha_span, 225)

            img = tf.shade(
                agg,
                cmap=cc.palette[cmap],
                color_key=color_key,
                min_alpha=min_alpha,
                how="log",
                span=span,
            )
            # spread ellipse/tracks (i.e. make lines thicker)
            if (spread is not None) and (spread > 0):
                img = tf.spread(img, spread)

            if points_agg is not None:
                points_img = tf.shade(
                    points_agg,
                    cmap=cc.palette[cmap],
                    color_key=points_color_key,
                    min_alpha=min_alpha,
                    how="log",
                    span=span,
                )

                if (spread is not None) and (spread > 0):
                    #Spread squares x3
                    points_img = tf.spread(points_img, spread*3, shape='square')
                else:
                    points_img = tf.spread(points_img, 2, shape='square')

                #Stack end markers onto the tracks
                img = tf.stack(img, points_img)

            img = img.to_bytesio().read()
            if metrics.get("over_max"):
                # Put hashing on image to indicate that it is over maximum
                current_app.logger.info("Generating overlay for tile")
                img = gen_overlay(img, color=(128, 128, 128, 128))
            elif metrics.get("aborted"):
                img = gen_overlay(img, color=(128, 128, 128, 128))

        if params.get("debug"):
            img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
        # Set headers and return data
        return img, metrics
    except Exception:
        current_app.logger.exception(
            "An exception occured while attempting to generate a tile:"
        )
        raise

@lru_cache
def calculate_pixel_spread(geotile_precision: int) -> int:
    '''
    Pixel spread is the number of pixels to put around each
    data point.
    '''
    current_app.logger.debug('calculate_pixel_spread(%d)', geotile_precision)

    if geotile_precision >= 20:
        return geotile_precision // 4

    if geotile_precision >= 15:
        return 2

    if geotile_precision >= 12:
        return 1

    return 0

def apply_spread(img, spread):
    '''
    Applies the pixel spreading transform, if any.
    '''
    current_app.logger.debug('apply_spread(%d)', spread)

    if spread > 0:
        return tf.spread(img, spread)

    return img

def generate_tile(idx, x, y, z, params):
    '''
    idx: ElasticSearch index to search
    x, y: TMS tile coordinates
    z: Zoom level
    params: HTTP request parameters
    '''

    # Handle legacy keywords
    geopoint_field = params["geopoint_field"]
    timestamp_field = params["timestamp_field"]
    start_time = params["start_time"]
    stop_time = params["stop_time"]
    category_field = params["category_field"]
    category_type = params["category_type"]
    category_format = params["category_format"]
    highlight = params["highlight"]
    cmap = params["cmap"]
    spread = params["spread"]
    resolution = params["resolution"]
    span_range = params["span_range"]
    lucene_query = params["lucene_query"]
    dsl_query = params["dsl_query"]
    dsl_filter = params["dsl_filter"]
    max_bins = params["max_bins"]
    use_centroid = params["use_centroid"]
    histogram_interval = params.get("generated_params", {}).get("histogram_interval")
    histogram_cnt = params.get("generated_params", {}).get("histogram_cnt")
    global_doc_cnt = params.get("generated_params", {}).get("global_doc_cnt")
    global_bounds = params.get("generated_params", {}).get("global_bounds")
    field_max = params.get("generated_params", {}).get("field_max", None)
    field_min = params.get("generated_params", {}).get("field_min", None)

    metrics = dict()

    current_app.logger.debug(
        "Generating tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s"
        % (
            idx,
            z,
            x,
            y,
            geopoint_field,
            timestamp_field,
            category_field,
            start_time,
            stop_time,
        )
    )
    try:
        # Preconfigured tile size
        tile_height_px = 256
        tile_width_px = 256

        # Get the web mercador bounds for the tile
        xy_bounds = mu.xy_bounds(x, y, z)
        west, south, east, north = mu.bounds(x, y, z)
        # Calculate the x/y range in meters
        x_range = xy_bounds[0], xy_bounds[2]
        y_range = xy_bounds[1], xy_bounds[3]
        # Swap the numbers so that [0] is always lowest
        if x_range[0] > x_range[1]:
            x_range = x_range[1], x_range[0]
        if y_range[0] > y_range[1]:
            y_range = y_range[1], y_range[0]
        # Get the top_left/bot_rght for the tile
        west, south, east, north = mu.bounds(x, y, z)
        # Constrain exactly to map boundaries
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

        # Figure out how big the tile is in meters
        xwidth = x_range[1] - x_range[0]
        yheight = y_range[1] - y_range[0]
        # And now the area of the tile
        area = xwidth * yheight

        # Create base search
        base_s = get_search_base(current_app.config.get("ELASTIC"), params, idx)

        # Now find out how many documents
        count_s = copy.copy(base_s)
        count_s = count_s.filter("geo_bounding_box", **{geopoint_field: bb_dict})

        doc_cnt = count_s.count()
        current_app.logger.info("Document Count: %s", doc_cnt)
        metrics['doc_cnt'] = doc_cnt

        # If count is zero then return a null image
        if doc_cnt == 0:
            current_app.logger.debug("No points in bounding box")
            img = gen_empty(tile_width_px, tile_height_px)
            if metrics.get("aborted"):
                img = gen_overlay(img, color=(128, 128, 128, 128))
            if params.get("debug"):
                img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
            return img, metrics
        else:
            # Find number of pixels in required image
            total_tile_pixel_count = tile_height_px * tile_width_px

            current_zoom = z

            # Calculate the geo precision that ensure we have at most one bin per 'pixel'.
            # Every zoom level halves the number of pixels per bin assuming a square tile.
            max_agg_zooms = math.ceil(math.log(total_tile_pixel_count, 4))
            agg_zooms = max_agg_zooms

            # TODO consider adding 'grid resolution' coarse, fine, finest (pixel-lock)
            # In category-mode, zoom out if max_bins has not been increased

            if category_field and max_bins < total_tile_pixel_count:
                agg_zooms -= 1

            if resolution == "coarse":
                agg_zooms -= 2
            elif resolution == "fine":
                agg_zooms -= 1
            elif resolution == "finest":
                if category_field:
                    if doc_cnt > 5e3:
                        agg_zooms -= 2
                    elif doc_cnt > 1e6:
                        agg_zooms -= 3
                    elif doc_cnt > 5e6:
                        agg_zooms -= 4
            else:
                raise ValueError("invalid resolution value")

            # don't allow geotile precision to be any worse than current zoom
            geotile_precision = max(current_zoom, current_zoom + agg_zooms)
            
            tile_bbox = {
                "top_left": {
                    "lat": bb_dict["top_left"]["lat"],
                    "lon": bb_dict["top_left"]["lon"]
                },
                "bottom_right": {
                    "lat": bb_dict["bottom_right"]["lat"],
                    "lon": bb_dict["bottom_right"]["lon"],
                },
            }

            tile_s = copy.copy(base_s)
            tile_s = tile_s.params(size=0, track_total_hits=False)
            tile_s = tile_s.filter(
                "geo_bounding_box", **{geopoint_field: tile_bbox}
            )

            s1 = time.time()

            inner_aggs = {}
            # TODO if we are pixel locked, calcuating a centriod seems unnecessary
            category_filters = None
            inner_agg_size = None
            if category_field and histogram_interval == None: # Category Mode
                # We calculate the categories to show based on the mapZoom (which is usually
                # a lower number then the requested tile)
                category_tile = mercantile.Tile(x, y, z)
                if category_tile.z > int(params.get("mapZoom")):
                    category_tile = mercantile.parent(category_tile, zoom=int(params["mapZoom"]))
                    
                category_filters, category_legend = get_tile_categories(
                    base_s,
                    category_tile.x,
                    category_tile.y,
                    category_tile.z,
                    geopoint_field,
                    category_field,
                    int(current_app.config["MAX_LEGEND_ITEMS_PER_TILE"]),
                )

                if len(category_filters) >= int(current_app.config["MAX_LEGEND_ITEMS_PER_TILE"]):
                    agg_zooms -= 1

                # to avoid max bucket errors we need space for two
                # additional buckets (one for Other and one for something else
                # internal to Elastic)
                inner_agg_size = len(category_filters) + 2
                inner_agg = A(
                    "filters",
                    filters=category_filters,
                    other_bucket_key="Other"
                )
                if use_centroid:
                    inner_agg = inner_agg.metric(
                        "centroid",
                        "geo_centroid",
                        field=geopoint_field
                    )
                inner_aggs = { "categories": inner_agg }
            elif category_field and histogram_interval != None: # Histogram Mode
                inner_agg_size = histogram_cnt

                inner_agg = A(
                    "histogram",
                    field=category_field,
                    interval=histogram_interval,
                    min_doc_count=1
                )

                if use_centroid:
                    inner_agg = inner_agg.metric(
                        "centroid",
                        "geo_centroid",
                        field=geopoint_field
                    )
                inner_aggs = { "categories": inner_agg }
            else:
                inner_agg_size = 1
                if use_centroid:
                    inner_aggs = {
                        "centroid": A(
                            "geo_centroid",
                            field=geopoint_field
                        )
                    }

            # the composite needs one bin for 'after_key'
            composite_agg_size = int(max_bins / inner_agg_size) - 1

            resp = ScanAggs(
                tile_s,
                {"grids": A("geotile_grid", field=geopoint_field, precision=geotile_precision)},
                inner_aggs,
                size=composite_agg_size,
                timeout=current_app.config["QUERY_TIMEOUT"]
            )

            partial_data = False # TODO can we get partial data?
            df = pd.DataFrame(
                convert_composite(
                    resp.execute(),
                    (category_field != None),
                    bool(category_filters),
                    histogram_interval,
                    category_type,
                    category_format
                )
            )
            
            s2 = time.time()
            current_app.logger.info("ES took %s (%s) for %s with %s searches" % ((s2 - s1), resp.total_took, len(df), resp.num_searches))
            metrics["query_time"] = (s2 - s1)
            metrics["query_took"] = resp.total_took
            metrics["num_searches"] = resp.num_searches
            metrics["aborted"] = resp.aborted
            metrics["shards_total"] = resp.total_shards
            metrics["shards_skipped"] = resp.total_skipped
            metrics["shards_successful"] = resp.total_successful
            metrics["shards_failed"] = resp.total_failed
            current_app.logger.info("%s", metrics)

            # Estimate the number of points per tile assuming uniform density
            estimated_points_per_tile = None
            if (span_range == "auto" or span_range is None):
                if global_bounds:
                    num_tiles_at_level = mu.num_tiles(*global_bounds, z)
                    estimated_points_per_tile = global_doc_cnt / num_tiles_at_level
                    current_app.logger.debug(
                        "Doc Bounds %s %s %s %s",
                        global_bounds,
                        z,
                        num_tiles_at_level,
                        estimated_points_per_tile,
                    )
                else:
                    current_app.logger.warning(
                        "Cannot estimate poins per tile because bounds ar missing"
                    )
                    estimated_points_per_tile = 100000

            if len(df.index) == 0:
                img = gen_empty(tile_width_px, tile_height_px)
                if metrics.get("aborted"):
                    img = gen_overlay(img, color=(128, 128, 128, 128))
                if params.get("debug"):
                    img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
                return img, metrics
            else:
                ###############################################################
                # Category Mode
                if category_field:
                    # TODO it would be nice if datashader honored the category orders
                    # in z-order, then we could make "Other" drawn underneath the less
                    # promenent colors
                    categories = list( df["t"].unique() )
                    metrics["categories"] = json.dumps(categories)
                    try:
                        categories.insert(0, categories.pop(categories.index("Other")))
                    except ValueError:
                        pass
                    cat_dtype = pd.api.types.CategoricalDtype(categories=categories, ordered=True)
                    df["t"] = df["t"].astype(cat_dtype)

                    """
                    # When the number of categories exceeds the number of colors, we can simply
                    # replaced the category name with the desired color (i.e. use the color as the category)
                    # field.  This is especially important in highly categorical data where the number of
                    # categories can be very large and thus cause huge memory allocations in `_colorize`
                    _, color_key = simplify_categories(
                        df,
                        "t",
                        create_color_key(df["t"].cat.categories, cmap=cmap, highlight=highlight),
                        inplace=True,
                    )
                    """
                    color_key=create_color_key(
                        df["t"].cat.categories,
                        cmap=cmap,
                        highlight=highlight,
                        field_min=field_min,
                        field_max=field_max,
                        histogram_interval=histogram_interval
                    )

                    agg = ds.Canvas(
                        plot_width=tile_width_px,
                        plot_height=tile_height_px,
                        x_range=x_range,
                        y_range=y_range,
                    ).points(df, "x", "y", agg=ds.by("t", ds.sum("c")))

                    span = None
                    if span_range == "flat":
                        min_alpha = 255
                    elif span_range == "narrow":
                        span = [0, math.log(1e3)]
                        min_alpha = 200
                    elif span_range == "normal":
                        span = [0, math.log(1e6)]
                        min_alpha = 100
                    elif span_range == "wide":
                        span = [0, math.log(1e9)]
                        min_alpha = 50
                    else:
                        assert estimated_points_per_tile is not None
                        span = [0, math.log(max(estimated_points_per_tile * 2, 2))]
                        alpha_span = int(span[1]) * 25
                        min_alpha = 255 - min(alpha_span, 225)

                    current_app.logger.debug("MinAlpha:%s Span:%s", min_alpha, span)
                    img = tf.shade(
                        agg,
                        cmap=cc.palette[cmap],
                        color_key=color_key,
                        min_alpha=min_alpha,
                        how="log",
                        span=span,
                    )

                ###############################################################
                # Heat Mode
                else:  # Heat Mode
                    agg = ds.Canvas(
                        plot_width=tile_width_px,
                        plot_height=tile_height_px,
                        x_range=x_range,
                        y_range=y_range,
                    ).points(df, "x", "y", agg=ds.sum("c"))

                    # Handle span range, the span applies the color map across
                    # the span range, so for example, if span is narrow, any
                    # bins that have 1000 or more items will be colored full
                    # scale
                    span = None
                    if span_range == "flat":
                        span = [0, 0]
                    elif span_range == "narrow":
                        span = [0, math.log(1e3)]
                    elif span_range == "normal":
                        span = [0, math.log(1e6)]
                    elif span_range == "wide":
                        span = [0, math.log(1e9)]
                    else:
                        assert estimated_points_per_tile != None
                        span = [0, math.log(max(estimated_points_per_tile * 2, 2))]

                    current_app.logger.debug("Span %s %s", span, span_range)
                    img = tf.shade(agg, cmap=cc.palette[cmap], how="log", span=span)

                ###############################################################
                # Common
                img = apply_spread(img, spread or calculate_pixel_spread(geotile_precision))
                img = img.to_bytesio().read()

                if partial_data:
                    current_app.logger.info(
                        "Generating overlay for tile due to partial category data"
                    )
                    img = gen_overlay(img, color=(128, 128, 128, 128))
                elif metrics.get("aborted"):
                    img = gen_overlay(img, color=(128, 128, 128, 128))

        if params.get("debug"):
            img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
        elif metrics.get("aborted"):
            img = gen_overlay(img, color=(128, 128, 128, 128))

        # Set headers and return data
        return img, metrics
    except Exception:
        current_app.logger.exception(
            "An exception occured while attempting to generate a tile:"
        )
        raise
