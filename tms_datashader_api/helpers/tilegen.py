#!/usr/bin/env python3
import copy
import math
import time

import colorcet as cc
import datashader as ds
import pandas as pd
from datashader import reductions as rd, transfer_functions as tf
from elasticsearch_dsl import AttrList, AttrDict
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
)
from tms_datashader_api.helpers.elastic import (
    get_search_base,
    convert,
    split_fieldname_to_list,
    get_nested_field_from_hit,
    to_32bit_float,
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
            angle = angle * ((2.0 * pi) / 360.0)  # Convert degrees to radians
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

            X, Y = ellipse(
                minor / 2.0, major / 2.0, angle, x0, y0, num_points=16
            )  # Points per ellipse, NB. this takes semi-maj/min
            if category_field:
                if histogram_interval:
                    # Do quantization
                    raw = get_nested_field_from_hit(hit, category_field, 0.0)
                    quantized = (
                        math.floor(raw / histogram_interval) * histogram_interval
                    )
                    c = str(to_32bit_float(quantized))
                else:
                    #If a number type, quantize it down to a 32-bit float so it matches what the legend will show
                    v = get_nested_field_from_hit(hit, category_field, "None")
                    if category_type == "number" or type(v) in (int, float):
                        c = "%0.1f" % to_32bit_float(v)
                    else:
                        # Just use the value
                        c = str(v)
            else:
                c = "None"

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
    cmap = params["cmap"]
    spread = params["spread"]
    span_range = params["span_range"]
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
            )
        )
        s2 = time.time()

        current_app.logger.debug(
            "ES took %s for ellipses: %s   hits: %s",
            (s2 - s1),
            metrics.get("ellipses", 0),
            metrics.get("hits", 0),
        )

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
        else:
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

                # NB. No spread on ellipses, could be added here if visibility is an issue

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
    highlight = params["highlight"]
    cmap = params["cmap"]
    spread = params["spread"]
    resolution = params["resolution"]
    span_range = params["span_range"]
    lucene_query = params["lucene_query"]
    dsl_query = params["dsl_query"]
    dsl_filter = params["dsl_filter"]
    max_bins = params["max_bins"]
    histogram_interval = params.get("generated_params", {}).get("histogram_interval")
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

        category_cnt = 0
        if category_field:
            # Also need to calculate the number of categories
            count_s = count_s.params(size=0)
            count_s.aggs.metric(
                "term_count", "cardinality", field=category_field
            ).metric("point_count", "value_count", field=geopoint_field)
            resp = count_s.execute()
            assert len(resp.hits) == 0
            if resp._shards.failed != 0:
                current_app.logger.warning("term_count response had shard failures")
            if hasattr(resp.aggregations, "term_count"):
                category_cnt = resp.aggregations.term_count.value
                if category_cnt <= 0:
                    category_cnt = 1
            if hasattr(resp.aggregations, "point_count"):
                doc_cnt = resp.aggregations.point_count.value

            # circuit breaker, if someone wants to color by category and there are
            # more than 1000, they will only get the first 1000 cateogries
            category_cnt = min(category_cnt, 1000)
            current_app.logger.info(
                "Document Count: %s, Category Count: %s", doc_cnt, category_cnt
            )
            metrics['doc_cnt'] = doc_cnt
            metrics['category_cnt'] = category_cnt
        else:
            category_cnt = 1  # Heat mode effectively has one category
            doc_cnt = count_s.count()
            current_app.logger.info("Document Count: %s", doc_cnt)
            metrics['doc_cnt'] = doc_cnt

        # If count is zero then return a null image
        if doc_cnt == 0:
            current_app.logger.debug("No points in bounding box")
            return gen_empty(tile_width_px, tile_height_px), metrics
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
                pass  # finest needs to do nothing
            else:
                raise ValueError("invalid resolution value")

            # don't allow geotile precision to be anyworse than current zoom
            geotile_precision = max(current_zoom, current_zoom + agg_zooms)

            # calculate how many sub_frames are required to avoid more than max_bins per
            # sub frame.  The number of bins in a sub-frame is 4**Z_delta so we need
            # to move up the zoom-level no further than that
            sub_frame_backout = int(math.log(max_bins, 4))

            # adding more categories limits how big a sub_frame can be
            if category_cnt <= max_bins:
                max_sub_frame_backout = math.floor(math.log(max_bins / category_cnt, 4))
            else:
                # if there are more categories than max_bins, the situation is hopeless
                max_sub_frame_backout = 0

            sub_frame_backout = min(sub_frame_backout, max_sub_frame_backout)
            sub_frame_level = max(current_zoom, geotile_precision - sub_frame_backout)

            geo_bins_per_subframe = 4 ** sub_frame_backout

            current_app.logger.debug(
                "GeoTile Zoom Info: pixels %s, current %s, agg %s, backout %s, sub frame level %s, precision %s, bins %s",
                pixels,
                current_zoom,
                agg_zooms,
                sub_frame_backout,
                sub_frame_level,
                geotile_precision,
                geo_bins_per_subframe,
            )

            metrics["sub_frame_level"] = sub_frame_level

            # generate n subframe bounding boxes
            subframes = mu.tiles_bounds(
                bb_dict["top_left"]["lon"],  # west
                bb_dict["bottom_right"]["lat"],  # south
                bb_dict["bottom_right"]["lon"],  # east
                bb_dict["top_left"]["lat"],  # north
                sub_frame_level,
            )

            partial_data = False
            df = pd.DataFrame()
            s1 = time.time()
            for subframe_bounds in subframes:
                subframe_bbox = {
                    "top_left": {
                        "lat": subframe_bounds[3],
                        "lon": subframe_bounds[0],
                    },
                    "bottom_right": {
                        "lat": subframe_bounds[1],
                        "lon": subframe_bounds[2],
                    },
                }

                subframe_s = copy.copy(base_s)
                subframe_s = subframe_s.params(size=0)
                subframe_s = subframe_s.filter(
                    "geo_bounding_box", **{geopoint_field: subframe_bbox}
                )

                # Set up the aggregations and the dataframe extraction
                if category_field and histogram_interval == None:  # Category Mode
                    assert (category_cnt * geo_bins_per_subframe) <= max_bins
                    subframe_s.aggs.bucket(
                        "categories", "terms", field=category_field, size=category_cnt
                    ).bucket(
                        "grids",
                        "geotile_grid",
                        field=geopoint_field,
                        precision=geotile_precision,
                        size=geo_bins_per_subframe,
                    ).metric(
                        "centroid", "geo_centroid", field=geopoint_field
                    )
                elif category_field and histogram_interval != None:  # Histogram Mode
                    assert histogram_interval != None
                    assert (category_cnt * geo_bins_per_subframe) <= max_bins
                    subframe_s.aggs.bucket(
                        "categories",
                        "histogram",
                        field=category_field,
                        interval=histogram_interval,
                        min_doc_count=1,
                    ).bucket(
                        "grids",
                        "geotile_grid",
                        field=geopoint_field,
                        precision=geotile_precision,
                        size=geo_bins_per_subframe,
                    ).metric(
                        "centroid", "geo_centroid", field=geopoint_field
                    )
                else:  # Heat Mode
                    assert geo_bins_per_subframe <= max_bins
                    subframe_s.aggs.bucket(
                        "grids",
                        "geotile_grid",
                        field=geopoint_field,
                        precision=geotile_precision,
                        size=geo_bins_per_subframe,
                    ).metric("centroid", "geo_centroid", field=geopoint_field)

                try:
                    resp = subframe_s.execute()
                except:
                    current_app.logger.exception(
                        "failed to generate subframe %s categories %s %s %s %s",
                        subframe_bounds,
                        category_cnt,
                        current_zoom,
                        sub_frame_level,
                        request,
                    )
                    raise

                assert len(resp.hits) == 0

                if hasattr(resp.aggregations, "categories") and hasattr(
                    resp.aggregations.categories, "sum_other_doc_count"
                ):
                    partial_data = resp.aggregations.categories.sum_other_doc_count > 0

                df = df.append(pd.DataFrame(convert(resp)), sort=False)

            s2 = time.time()
            current_app.logger.debug("ES took %s for %s" % ((s2 - s1), len(df)))
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

            if len(df.index) == 0:
                return gen_empty(tile_width_px, tile_height_px), metrics
            else:
                ###############################################################
                # Category Mode
                if category_field:
                    df["t"] = df["t"].astype("category")

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
