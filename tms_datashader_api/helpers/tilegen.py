#!/usr/bin/env python3
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

import tms_datashader_api.helpers.mercantile_util as mu
from tms_datashader_api.helpers.drawing import (
    ellipse,
    gen_empty,
    gen_overlay,
    create_color_key,
    gen_debug_overlay,
    generate_ellipse_points
)
from tms_datashader_api.helpers.elastic import (
    get_search_base,
    convert,
    convert_composite,
    split_fieldname_to_list,
    get_nested_field_from_hit,
    to_32bit_float,
    ScanAggs,
    get_tile_categories
)
from tms_datashader_api.helpers.pandas_util import simplify_categories

NAN_LINE = {"x": None, "y": None, "c": "None"}


class GeotileGrid(Bucket):
    name = "geotile_grid"


def create_datashader_ellipses_from_search(
    search,
    geopoint_fields,
    maximum_ellipses_per_tile,
    extend_meters,
    metrics=None,
    histogram_interval=None,
    category_format=None
):
    """

    :param search:
    :param geopoint_fields:
    :param maximum_ellipses_per_tile:
    :param extend_meters:
    :param metrics:
    :param histogram_interval:
    :return:
    """
    if metrics is None:
        metrics = {}
    metrics.update({"over_max": False, "hits": 0, "ellipses": 0})

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

    for i, hit in enumerate(search.scan()):
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
            if major > extend_meters or minor > extend_meters:
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
                        c = str(v)
            else:
                C = [ "None" ]

            if len(C) > 100:
                current_app.logger.warning("truncating category list of size %s to first 100 categories", len(C))
                C = C[0:100]
            
            for c in C:
                for p in zip(X, Y, len(X) * [c]):
                    yield {"x": p[0], "y": p[1], "c": p[2]}
            yield NAN_LINE  # Break between ellipses
            metrics["ellipses"] += 1


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
    ellipse_max_cep = params["ellipse_max_cep"]
    max_batch = params["max_batch"]
    max_bins = params["max_bins"]
    max_ellipses_per_tile = params["max_ellipses_per_tile"]
    histogram_interval = params.get("generated_params", {}).get(
        "histogram_interval", None
    )
    global_doc_cnt = params.get("generated_params", {}).get("global_doc_cnt", None)
    global_bounds = params.get("generated_params", {}).get("global_bounds", None)

    current_app.logger.info(
        "Generating ellipse tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s"
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

        # Expand this by maximum CEP value to get adjacent geos that overlap into our tile
        extend_meters = ellipse_max_cep * 1852

        # Get the top_left/bot_rght for the tile
        top_left = mu.lnglat(
            x_range[0] - extend_meters, y_range[1] + extend_meters
        )
        bot_rght = mu.lnglat(
            x_range[1] + extend_meters, y_range[0] - extend_meters
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

        # Per ES documentation, sorting by _doc improves scroll speed
        count_s.sort("_doc")

        # trim category field postfixes
        if category_field:
            if category_field.endswith(".keyword"):
                category_field = category_field[: -len(".keyword")]
            elif category_field.endswith(".raw"):
                category_field = category_field[: -len(".raw")]

        # Handle the limiting to only the fields required for processing
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

        # Process the hits (geos) into a list of points
        s1 = time.time()
        metrics = dict(over_max=False)
        df = pd.DataFrame.from_dict(
            create_datashader_ellipses_from_search(
                count_s,
                geopoint_fields,
                max_ellipses_per_tile,
                extend_meters,
                metrics,
                histogram_interval,
                category_format
            )
        )
        s2 = time.time()

        current_app.logger.debug(
            "ES took %s for ellipses: %s   hits: %s",
            (s2 - s1),
            metrics.get("ellipses", 0),
            metrics.get("hits", 0),
        )
        metrics["query_time"] = (s2 - s1)

        # Estimate the number of points per tile assuming uniform density
        estimated_points_per_tile = None
        if span_range == "auto" or span_range is None:
            num_tiles_at_level = mu.num_tiles(*global_bounds, z)
            estimated_points_per_tile = global_doc_cnt / num_tiles_at_level
            current_app.logger.debug(
                "Doc Bounds %s %s %s %s",
                global_bounds,
                z,
                num_tiles_at_level,
                estimated_points_per_tile,
            )

        # If count is zero then return a null image
        if len(df) == 0:
            current_app.logger.debug("No points in bounding box")
            img = gen_empty(tile_width_px, tile_height_px)
            if metrics.get("over_max"):
                img = gen_overlay(img)
            if current_app.config.get("DEBUG_TILES"):
                img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
        else:
            categories = list( df["c"].unique() )
            metrics["categories"] = json.dumps(categories)

            # Generate the image
            df["c"] = df["c"].astype("category")
            # prevent memory explosion in datashader _colorize
            _, color_key = simplify_categories(
                df,
                "c",
                create_color_key(df["c"].cat.categories, cmap=cmap),
                inplace=True,
            )

            # Find number of pixels in required image
            pixels = tile_height_px * tile_width_px

            if len(df.index) == 0:
                img = gen_empty(tile_width_px, tile_height_px)
                if current_app.config.get("DEBUG_TILES"):
                    img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
            else:
                agg = ds.Canvas(
                    plot_width=tile_width_px,
                    plot_height=tile_height_px,
                    x_range=x_range,
                    y_range=y_range,
                ).line(df, "x", "y", agg=rd.count_cat("c"))

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

                if (spread is not None) and (spread > 0):
                    img = tf.spread(img, spread)

                img = img.to_bytesio().read()
                if metrics.get("over_max"):
                    # Put hashing on image to indicate that it is over maximum
                    current_app.logger.info("Generating overlay for tile")
                    img = gen_overlay(img)

        if current_app.config.get("DEBUG_TILES"):
            img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
        # Set headers and return data
        return img, metrics
    except Exception:
        current_app.logger.exception(
            "An exception occured while attempting to generate a tile:"
        )
        raise


def generate_tile(idx, x, y, z, params):

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
        # Calculate the x/y range in meters
        x_range = xy_bounds[0], xy_bounds[2]
        y_range = xy_bounds[1], xy_bounds[3]
        # Swap the numbers so that [0] is always lowest
        if x_range[0] > x_range[1]:
            x_range = x_range[1], x_range[0]
        if y_range[0] > y_range[1]:
            y_range = y_range[1], y_range[0]
        # Get the top_left/bot_rght for the tile
        top_left = mu.lnglat(x_range[0], y_range[1])
        bot_rght = mu.lnglat(x_range[1], y_range[0])
        # Constrain exactly to map boundaries
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
            if current_app.config.get("DEBUG_TILES"):
                img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))
            return img, metrics
        else:
            # Find number of pixels in required image
            pixels = tile_height_px * tile_width_px

            current_zoom = z

            # calculate the geo precision that ensure we have at most one bin per 'pixel'
            # every zoom level halves the number of pixels per bin
            # assuming a square tile
            max_agg_zooms = math.ceil(math.log(pixels, 4))
            agg_zooms = max_agg_zooms

            # TODO consider adding 'grid resolution' coarse, fine, finest (pixel-lock)
            # In category-mode, zoom out if max_bins has not been increased
            min_auto_spread = 0  # by default we don't need to spread
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
                if category_field:
                    if doc_cnt > 5e3:
                        agg_zooms -= 2
                        min_auto_spread += 1
                    elif doc_cnt > 1e6:
                        agg_zooms -= 3
                        min_auto_spread += 2
                    elif doc_cnt > 5e6:
                        agg_zooms -= 4
                        min_auto_spread += 3
            else:
                raise ValueError("invalid resolution value")

            # don't allow geotile precision to be anyworse than current zoom
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
            # TOOD if we are pixel locked, calcuating a centriod seems unnecessary
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
                    min_auto_spread += 1

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
                size=composite_agg_size
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

            # Estimate the number of points per tile assuming uniform density
            estimated_points_per_tile = None
            if span_range == "auto" or span_range is None:
                num_tiles_at_level = mu.num_tiles(*global_bounds, z)
                estimated_points_per_tile = global_doc_cnt / num_tiles_at_level
                current_app.logger.debug(
                    "Doc Bounds %s %s %s %s",
                    global_bounds,
                    z,
                    num_tiles_at_level,
                    estimated_points_per_tile,
                )

            if len(df.index) == 0:
                img = gen_empty(tile_width_px, tile_height_px)
                if current_app.config.get("DEBUG_TILES"):
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
                    color_key=create_color_key(df["t"].cat.categories, cmap=cmap, highlight=highlight)

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

                # Below zoom threshold spread to make individual dots large enough
                if spread is None or spread < 0:
                    spread_threshold = 11
                    # Always spread at least min_auto_spread
                    spread = min_auto_spread
                    if z >= spread_threshold:
                        # Increase spread at high zoom levels, with a min spread of 2
                        spread = math.floor(
                            min_auto_spread + (z - (spread_threshold - 1)) * 0.5
                        )
                        spread = max(spread, 1)
                    current_app.logger.info(
                        "Calculated auto-spread %s (min %s)", spread, min_auto_spread
                    )
                else:
                    current_app.logger.info("Spreading by fixed %s", spread)

                if spread > 0:
                    img = tf.spread(img, spread)

                img = img.to_bytesio().read()

                if partial_data:
                    current_app.logger.info(
                        "Generating overlay for tile due to partial category data"
                    )
                    img = gen_overlay(img)

        if current_app.config.get("DEBUG_TILES"):
            img = gen_debug_overlay(img, "%s/%s/%s" % (z, x, y))

        # Set headers and return data
        return img, metrics
    except Exception:
        current_app.logger.exception(
            "An exception occured while attempting to generate a tile:"
        )
        raise
