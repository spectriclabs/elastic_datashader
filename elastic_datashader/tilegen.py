from dataclasses import asdict, dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import copy
import math
import time
import json

from datashader import reductions as rd
from datashader import transfer_functions as tf
from datashader.utils import lnglat_to_meters
from elasticsearch_dsl import AttrList, AttrDict, A
from georgio import wm_tile_expanded_bbox  # pylint: disable=E0611

import colorcet as cc
import datashader as ds
import mercantile
import numpy as np
import pynumeral
import pandas as pd

from . import mercantile_util as mu

from .config import config
from .drawing import (
    create_color_key,
    ellipse_planar_points,
    ellipse_spheroid_points,
    gen_debug_overlay,
    gen_empty,
    gen_overlay,
)
from .elastic import (
    get_field_type,
    get_search_base,
    convert_composite,
    split_fieldname_to_list,
    get_nested_field_from_hit,
    to_32bit_float,
    Scan,
    get_tile_categories,
    scan
)
from .logger import logger
from .pandas_util import simplify_categories

NAN_LINE = {"x": None, "y": None, "c": "None"}
TILE_HEIGHT_PX = 256
TILE_WIDTH_PX = 256

@dataclass
class EllipseFieldNames:
    geopoint_center: List[str]
    ellipse_major: List[str]
    ellipse_minor: List[str]
    ellipse_tilt: List[str]
    category_field: Optional[List[str]]

def get_ellipse_field_names(params: Dict[str, Any]) -> EllipseFieldNames:
    category_field = params.get('category_field')

    if category_field is not None:
        category_field = split_fieldname_to_list(category_field)

    return EllipseFieldNames(
        geopoint_center=split_fieldname_to_list(params['geopoint_field']),
        ellipse_major=split_fieldname_to_list(params['ellipse_major']),
        ellipse_minor=split_fieldname_to_list(params['ellipse_minor']),
        ellipse_tilt=split_fieldname_to_list(params['ellipse_tilt']),
        category_field=category_field,
    )

@dataclass
class TrackFieldNames:
    geopoint_center: List[str]
    category_field: Optional[List[str]]
    track_connection: Optional[List[str]]

def get_track_field_names(params: Dict[str, Any]) -> TrackFieldNames:
    category_field = params.get('category_field')
    track_connection = params.get('track_connection')

    if category_field is not None:
        category_field = split_fieldname_to_list(category_field)

    if track_connection is not None:
        track_connection = split_fieldname_to_list(track_connection)

    return TrackFieldNames(
        geopoint_center=split_fieldname_to_list(params['geopoint_field']),
        category_field=category_field,
        track_connection=track_connection,
    )

def populated_field_names(field_names: Union[EllipseFieldNames,TrackFieldNames]) -> List[str]:
    field_names_as_lists = (v for v in asdict(field_names).values() if v is not None)
    return [".".join(field_name_as_list) for field_name_as_list in field_names_as_lists]

def all_ellipse_fields_have_values(locs, majors, minors, angles, field_names: EllipseFieldNames) -> bool:
    if locs is None:
        logger.debug("hit field %s has no values", field_names.geopoint_center)
        return False

    if majors is None:
        logger.debug("hit field %s has no values", field_names.ellipse_major)
        return False

    if minors is None:
        logger.debug("hit field %s has no values", field_names.ellipse_minor)
        return False

    if angles is None:
        logger.debug("hit field %s has no values", field_names.ellipse_tilt)
        return False

    return True

def normalize_ellipses_to_list(locs, majors, minors, angles):
    '''
    If its a list determine if there are multiple ellipses or just a single ellipse in list format
    '''
    if isinstance(locs, (AttrList, list)):
        if (
            len(locs) == 2
            and isinstance(locs[0], float)
            and isinstance(locs[1], float)
        ):
            locs = [locs]  # eg. [[-73.986,40.7485]]
            majors = [majors]
            minors = [minors]
            angles = [angles]
    else:
        # All other cases are single ellipses
        locs = [locs]
        majors = [majors]
        minors = [minors]
        angles = [angles]

    return locs, majors, minors, angles

def normalize_locations_to_list(locs):
    if isinstance(locs, (AttrList, list)):
        if (
            len(locs) == 2
            and isinstance(locs[0], float)
            and isinstance(locs[1], float)
        ):
            locs = [locs]  # eg. [[-73.986,40.7485]]
    else:
        # All other cases are single location
        locs = [locs]

    return locs

@dataclass
class Location:
    lat: float
    lon: float

@dataclass
class Ellipse:
    location: Location
    major_meters: float
    minor_meters: float
    angle_degrees: float

def normalize_location(location) -> Optional[Location]:
    # sometimes the location is a comma-separated lat,lon
    if isinstance(location, str):
        if "," not in location:
            logger.warning("skipping location with invalid str format %s", location)
            return None

        lat, lon = location.split(",", 1)
        return Location(lat=float(lat), lon=float(lon))

    # sometimes the location is a two-element [lon,lat] list
    if isinstance(location, (AttrList, list)):
        if len(location) != 2:
            logger.warning("skipping location with invalid list format %s", location)
            return None

        lon, lat = location
        return Location(lat=float(lat), lon=float(lon))

    # if none of the above, then the location better be a dictionary
    if not isinstance(location, (AttrDict, dict)):
        logger.warning(
            "skipping location with invalid format %s %s %s",
            location,
            isinstance(location, list),
            type(location),
        )
        return None

    return Location(lat=location["lat"], lon=location["lon"])

def normalize_to_float(value, field_name) -> Optional[float]:
    try:
        return float(value)
    except ValueError:
        logger.warning(
            "skipping invalid %s with value %s and type %s",
            field_name,
            value,
            type(value),
        )

    return None

def convert_to_full_axis_meters(distance: float, units: str) -> float:
    if units == "majmin_nm":
        return distance * 1852  # full-axis nautical miles to full-axis meters

    if units == "semi_majmin_nm":
        return distance * 2 * 1852  # semi-axis nautical miles to full-axis meters

    if units == "semi_majmin_m":
        return distance * 2  # semi-axis meters to full-axis meters

    # NB. assume "majmin_m" if any others
    return distance

def ellipse_points(ellipse: Ellipse) -> Tuple[np.array, np.array]:
    if config.ellipse_render_mode == "simple":
        x0, y0 = lnglat_to_meters(ellipse.location.lon, ellipse.location.lat)
        y_points, x_points = ellipse_planar_points(
            ellipse.major_meters/2,
            ellipse.minor_meters/2,
            np.radians(ellipse.angle_degrees),
            y0,
            x0,
            num_points=16,
        )
        return x_points, y_points

    if config.ellipse_render_mode == "matrix":
        lats, lons = ellipse_spheroid_points(
            ellipse.location.lat,
            ellipse.location.lon,
            ellipse.major_meters/2,
            ellipse.minor_meters/2,
            tilt=ellipse.angle_degrees,
            n_points=config.num_ellipse_points,
        )
        x_points, y_points = lnglat_to_meters(lons, lats)
        return x_points, y_points

    raise ValueError(f"Invalid ellipse render mode {config.ellipse_render_mode}")

def ellipse_generator(hit, field_names: EllipseFieldNames, ellipse_units: str) -> Iterable[Optional[Ellipse]]:
    # Get all the ellipse fields
    locations = get_nested_field_from_hit(hit, field_names.geopoint_center, None)
    majors = get_nested_field_from_hit(hit, field_names.ellipse_major, None)
    minors = get_nested_field_from_hit(hit, field_names.ellipse_minor, None)
    angles = get_nested_field_from_hit(hit, field_names.ellipse_tilt, None)

    if not all_ellipse_fields_have_values(locations, majors, minors, angles, field_names):
        return

    locations, majors, minors, angles = normalize_ellipses_to_list(locations, majors, minors, angles)

    # verify same length
    if not len(locations) == len(majors) == len(minors) == len(angles):
        logger.warning(
            "ellipse parameters and length are not consistent"
        )
        return

    for location, major, minor, angle in zip(locations, majors, minors, angles):
        location = normalize_location(location)

        if location is None:
            continue

        major = normalize_to_float(major, 'major')

        if major is None:
            continue

        minor = normalize_to_float(minor, 'minor')

        if minor is None:
            continue

        angle = normalize_to_float(angle, 'angle')

        if angle is None:
            continue

        yield Ellipse(
            location=location,
            major_meters=convert_to_full_axis_meters(major, ellipse_units),
            minor_meters=convert_to_full_axis_meters(minor, ellipse_units),
            angle_degrees=angle,
        )

def limit_list_length(input_list: List[Any], max_length: int) -> List[Any]:
    if len(input_list) > max_length:
        return input_list[:max_length+1]

    return input_list

def get_quantized_category_list(hit, category_field: str, category_type: Optional[str], histogram_interval) -> List[str]:
    raw = get_nested_field_from_hit(hit, category_field, 0.0)

    if isinstance(raw, list):
        category_list = []

        for v in raw:
            if category_type == "number" or type(v) in (int, float):
                quantized = math.floor(float(v) / histogram_interval) * histogram_interval
                category_list.append(str(to_32bit_float(quantized)))
            else:
                category_list.append(str(v))

    else:
        if category_type == "number" or isinstance(raw, (int, float)):
            quantized = math.floor(raw / histogram_interval) * histogram_interval
            category_list = [str(to_32bit_float(quantized))]
        else:
            category_list = [str(raw)]

    return category_list

def get_float_quantized_category_list(hit, category_field: str, category_type, category_format) -> List[str]:
    '''
    If a number type, quantize it down to a 32-bit float so it matches what the legend will show
    '''
    raw = get_nested_field_from_hit(hit, category_field, "N/A")

    if category_type == "number" or isinstance(raw, (int, float)):
        if category_format:
            category_list = [pynumeral.format(to_32bit_float(raw), category_format)]
        else:
            category_list = [str(to_32bit_float(raw))]
    else:
        # Just use the value
        if not isinstance(raw, (list, AttrList)):
            category_list = [str(raw)]
        else:
            category_list = [str(v) for v in raw]

    return category_list

def get_category_list(hit, category_field: Optional[str], category_type: Optional[str], category_format, histogram_interval) -> List[str]:
    if category_field is None:
        return ["None"]

    if histogram_interval:
        return limit_list_length(
            get_quantized_category_list(hit, category_field, category_type, histogram_interval),
            100,
        )

    return limit_list_length(
        get_float_quantized_category_list(hit, category_field, category_type, category_format),
        100,
    )

def create_datashader_ellipses_from_search(
    search,
    field_names: EllipseFieldNames,
    ellipse_units: str,
    maximum_ellipses_per_tile,
    search_meters,
    histogram_interval,
    category_type,
    category_format,
    metrics: Dict[str, Any],
) -> Dict[str, Any]:
    metrics.update({"over_max": False, "hits": 0, "locations": 0})

    timeout_at = None

    if config.query_timeout_seconds:
        timeout_at = time.time() + config.query_timeout_seconds
        search = search.params(timeout=f"{config.query_timeout_seconds}s")

    for i, hit in enumerate(scan(search, use_scroll=config.use_scroll)):
        if timeout_at and (time.time() > timeout_at):
            logger.warning("ellipse generation hit query timeout")
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

        category_list = get_category_list(
            hit,
            field_names.category_field,
            category_type,
            category_format,
            histogram_interval,
        )

        for ellipse in ellipse_generator(hit, field_names, ellipse_units):
            # expel above CEP limit
            if ellipse.major_meters > search_meters or ellipse.minor_meters > search_meters:
                continue

            x_points, y_points = ellipse_points(ellipse)

            for category in category_list:
                for point in zip(x_points, y_points):
                    yield {"x": point[0], "y": point[1], "c": category}

            yield NAN_LINE  # Break between ellipses
            metrics["locations"] += 1

def location_generator(hit, geopoint_center_field_name: str) -> Iterable[Optional[Location]]:
    locations = get_nested_field_from_hit(hit, geopoint_center_field_name, None)

    if locations is None:
        logger.debug("hit field %s has no values", geopoint_center_field_name)
        return

    locations = normalize_locations_to_list(locations)

    for location in locations:
        location = normalize_location(location)

        if location is not None:
            yield location

def get_track_list(hit, track_connection: List[str]) -> List[str]:
    # Handle tracking field
    if track_connection:
        v = get_nested_field_from_hit(hit, track_connection, "N/A")

        # Just use the value
        if isinstance(v, list):
            return v

        return [v]

    return ["None"]

def create_datashader_tracks_from_search(
    search,
    field_names: TrackFieldNames,
    maximum_hits_per_tile,
    histogram_interval,
    category_type,
    category_format,
    metrics: Dict[str, Any],
) -> Dict[str, Any]:
    metrics.update({"over_max": False, "hits": 0, "locations": 0})
    category_set = set()
    timeout_at = None

    if config.query_timeout_seconds:
        timeout_at = time.time() + config.query_timeout_seconds
        search = search.params(timeout=f"{config.query_timeout_seconds}s")

    for i, hit in enumerate(scan(search, use_scroll=config.use_scroll)):
        if timeout_at and (time.time() > timeout_at):
            logger.warning("track generation hit query timeout")
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

        category_set.update(
            get_category_list(
                hit,
                field_names.category_field,
                category_type,
                category_format,
                histogram_interval,
            )
        )

        track_list = get_track_list(hit, field_names.track_connection)

        for location in location_generator(hit, field_names.geopoint_center):
            # Handle deg->Meters conversion and everything else
            x0, y0 = lnglat_to_meters(location.lon, location.lat)

            for category in category_set:
                for track in track_list:
                    yield {"x": x0, "y": y0, "c": category, "t": track}

            metrics["locations"] += 1

def get_estimated_points_per_tile(span_range: Optional[str], global_bounds, zoom_level: int, global_doc_count: int) -> int:
    '''
    Estimate the number of points per tile assuming uniform density
    '''
    estimated_points_per_tile = None

    if span_range == "auto" or span_range is None:
        if global_bounds:
            num_tiles_at_level = mu.num_tiles(*global_bounds, zoom_level)
            estimated_points_per_tile = global_doc_count / num_tiles_at_level
            logger.debug(
                "Doc Bounds %s %s %s %s",
                global_bounds,
                zoom_level,
                num_tiles_at_level,
                estimated_points_per_tile,
            )
        else:
            logger.warning("Cannot estimate points per tile because bounds are missing")
            estimated_points_per_tile = 100000

    return estimated_points_per_tile

def get_span_upper_bound(span_range: str, estimated_points_per_tile: Optional[int]) -> Optional[float]:
    if span_range == "flat":
        return None

    if span_range == "narrow":
        return math.log(1e3)

    if span_range == "normal":
        return math.log(1e6)

    if span_range == "wide":
        return math.log(1e9)

    if span_range == "ultrawide":
        return math.log(1e308)

    assert estimated_points_per_tile is not None
    return math.log(max(math.pow(estimated_points_per_tile,2), 2))

def get_span_none(span_upper_bound: Optional[float]) -> Optional[List[float]]:
    if span_upper_bound is None:
        return None

    return [0, span_upper_bound]

def get_span_zero(span_upper_bound: Optional[float]) -> Optional[List[float]]:
    if span_upper_bound is None:
        return [0, 0]

    return [0, span_upper_bound]

def get_min_alpha(span_range: str, span_upper_bound: Optional[float]) -> int:
    if span_range == "flat":
        return 255

    if span_range == "narrow":
        return 200

    if span_range == "normal":
        return 100

    if span_range == "wide":
        return 50

    assert span_upper_bound is not None
    alpha_span = int(span_upper_bound) * 25
    return 255 - min(alpha_span, 225)

def generate_nonaggregated_tile(
    idx, x, y, z, headers, params, tile_height_px=256, tile_width_px=256
):
    # Handle legacy parameters
    geopoint_field = params["geopoint_field"]
    timestamp_field = params["timestamp_field"]
    start_time = params["start_time"]
    stop_time = params["stop_time"]
    category_field = params["category_field"]
    highlight = params["highlight"]
    cmap = params["cmap"]
    spread = params["spread"]
    span_range = params["span_range"]
    spread = params["spread"]
    filter_distance = params["filter_distance"]
    max_batch = params["max_batch"]
    histogram_interval = params.get("generated_params", {}).get(
        "histogram_interval", None
    )
    global_doc_cnt = params.get("generated_params", {}).get("global_doc_cnt", None)
    global_bounds = params.get("generated_params", {}).get("global_bounds", None)
    field_max = params.get("generated_params", {}).get("field_max", None)
    field_min = params.get("generated_params", {}).get("field_min", None)
    render_mode = params["render_mode"]

    logger.info(
        "Generating non-aggegated (%s) tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s",
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
    try:

        # Expand this by search_nautical_miles value to get adjacent geos that overlap into our tile
        search_meters = params["search_nautical_miles"] * 1852

        if filter_distance is not None:
            filter_meters = filter_distance * 1852
        else:
            filter_meters = search_meters

        bb_dict = create_bounding_box_for_ellipses(x, y, z, search_meters)

        # Create base search
        base_s = get_search_base(config.elastic_hosts, headers, params, idx).params(
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
        metrics = {"over_max": False}

        if render_mode == "ellipses":
            field_names = get_ellipse_field_names(params)
            count_s = count_s.source(includes=populated_field_names(field_names))
            df = pd.DataFrame.from_dict(
                create_datashader_ellipses_from_search(
                    count_s,
                    field_names,
                    params["ellipse_units"],
                    params["max_ellipses_per_tile"],
                    search_meters,
                    histogram_interval,
                    params["category_type"],
                    params["category_format"],
                    metrics,
                )
            )
            df_points = None

        else:
            field_names = get_track_field_names(params)
            count_s = count_s.source(includes=populated_field_names(field_names))
            df = pd.DataFrame.from_dict(
                create_datashader_tracks_from_search(
                    count_s,
                    field_names,
                    params["max_ellipses_per_tile"],
                    histogram_interval,
                    params["category_type"],
                    params["category_format"],
                    metrics,
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
            for _, row in df.iterrows():
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

        logger.debug(
            "ES took %s for locations: %s   hits: %s",
            (s2 - s1),
            metrics.get("locations", 0),
            metrics.get("hits", 0),
        )
        metrics["query_time"] = (s2 - s1)

        estimated_points_per_tile = get_estimated_points_per_tile(span_range, global_bounds, z, global_doc_cnt)

        # If count is zero then return a null image
        if len(df) == 0:
            logger.debug("No points in bounding box")
            img = gen_empty(tile_width_px, tile_height_px)

            if metrics.get("over_max"):
                img = gen_overlay(img, color=(128, 128, 128, 128))
            elif metrics.get("aborted"):
                img = gen_overlay(img, color=(128, 128, 128, 128))

            if params.get("debug"):
                img = gen_debug_overlay(img, f"{z}/{x}/{y}")
        else:
            categories = [x for x in df["c"].unique() if x is not None]
            metrics["categories"] = json.dumps(categories)

            # Generate the image
            df["c"] = df["c"].astype("category")
            color_key = create_color_key(
                categories,
                cmap=cmap,
                highlight=highlight,
                field_min=field_min,
                field_max=field_max,
                histogram_interval=histogram_interval,
            )

            # prevent memory explosion in datashader _colorize
            _, color_key = simplify_categories(
                df,
                "c",
                color_key,
                inplace=True,
            )

            x_range, y_range = xy_ranges(x, y, z)
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

            span_upper_bound = get_span_upper_bound(span_range, estimated_points_per_tile)
            span = get_span_none(span_upper_bound)
            min_alpha = get_min_alpha(span_range, span_upper_bound)

            img = tf.shade(
                agg,
                cmap=cc.palette[cmap],
                color_key=color_key,
                min_alpha=min_alpha,
                how="log",
                span=span,
            )

            # spread ellipse/tracks (i.e. make lines thicker)
            if spread is not None and spread > 0:
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
                logger.info("Generating overlay for tile")
                img = gen_overlay(img, color=(128, 128, 128, 128))
            elif metrics.get("aborted"):
                img = gen_overlay(img, color=(128, 128, 128, 128))

        if params.get("debug"):
            img = gen_debug_overlay(img, f"{z}/{x}/{y}")
        # Set headers and return data
        return img, metrics
    except Exception:
        logger.exception(
            "An exception occured while attempting to generate a tile:"
        )
        raise

@lru_cache
def calculate_pixel_spread(geotile_precision: int) -> int:
    '''
    Pixel spread is the number of pixels to put around each
    data point.
    '''
    logger.debug('calculate_pixel_spread(%d)', geotile_precision)

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
    logger.debug('apply_spread(%d)', spread)

    if spread > 0:
        return tf.spread(img, spread)

    return img

@lru_cache
def xy_ranges(x: int, y: int, z: int) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    xy_bounds = mu.xy_bounds(x, y, z)  # bounds with coordinates as meters

    # make sure element 0 is always lowest
    x_range = sorted((xy_bounds[0], xy_bounds[2]))
    y_range = sorted((xy_bounds[1], xy_bounds[3]))

    return tuple(x_range), tuple(y_range)

@lru_cache
def create_bounding_box_for_tile(x: int, y: int, z: int) -> Dict[str, Dict[str, float]]:
    '''
    Creates the lat/lon bounding box dictionary used to query
    ElasticSearch for a particular x, y, z Web Mercator tile.
    '''
    west, south, east, north = mu.bounds(x, y, z) # bounds with coordinate as degrees

    # Constrain exactly to map boundaries
    return {
        "top_left": {
            "lat": min(90, max(-90, north)),
            "lon": min(180, max(-180, west)),
        },
        "bottom_right": {
            "lat": min(90, max(-90, south)),
            "lon": min(180, max(-180, east)),
        },
    }


@lru_cache
def create_bounding_box_for_ellipses(x: int, y: int, z: int, search_meters: float) -> Dict[str, Dict[str, float]]:
    '''
    Creates the lat/lon bounding box dictionary used to query
    ElasticSearch.  It takes the boundaries for an x, y, z  Web
    Mercator tile and expands them to include ellipses whose
    center points are outside the tile, but with major/minor
    axes that could extend onto the tile.
    '''
    west, south, east, north = wm_tile_expanded_bbox(x, y, z, search_meters)

    return {
        "top_left": {
            "lon": min(180, max(-180, west)),
            "lat": min(90, max(-90, north)),
        },
        "bottom_right": {
            "lon": min(180, max(-180, east)),
            "lat": min(90, max(-90, south)),
        },
    }

def generate_tile(idx, x, y, z, headers, params, tile_width_px=256, tile_height_px=256):
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
    max_bins = params["max_bins"]
    use_centroid = params["use_centroid"]
    histogram_interval = params.get("generated_params", {}).get("histogram_interval")
    histogram_cnt = params.get("generated_params", {}).get("histogram_cnt")
    global_doc_cnt = params.get("generated_params", {}).get("global_doc_cnt")
    global_bounds = params.get("generated_params", {}).get("global_bounds")
    field_max = params.get("generated_params", {}).get("field_max", None)
    field_min = params.get("generated_params", {}).get("field_min", None)

    metrics = {}

    logger.debug(
        "Generating tile for: %s - %s/%s/%s.png, geopoint:%s timestamp:%s category:%s start:%s stop:%s",
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
    try:
        bb_dict = create_bounding_box_for_tile(x, y, z)

        # Create base search
        base_s = get_search_base(config.elastic_hosts, headers, params, idx)
        base_s = base_s[0:0]
        # Now find out how many documents
        count_s = copy.copy(base_s)[0:0] #slice of array sets from/size since we are aggregating the data we don't need the hits
        count_s = count_s.filter("geo_bounding_box", **{geopoint_field: bb_dict})

        doc_cnt = count_s.count()
        logger.info("Document Count: %s", doc_cnt)
        metrics['doc_cnt'] = doc_cnt

        # If count is zero then return a null image
        if doc_cnt == 0:
            logger.debug("No points in bounding box")
            img = gen_empty(tile_width_px, tile_height_px)
            if metrics.get("aborted"):
                img = gen_overlay(img, color=(128, 128, 128, 128))
            if params.get("debug"):
                img = gen_debug_overlay(img, f"{z}/{x}/{y}")
            return img, metrics

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

        tile_s = copy.copy(base_s)
        tile_s = tile_s.params(size=0, track_total_hits=False)
        tile_s = tile_s.filter(
            "geo_bounding_box", **{geopoint_field: bb_dict}
        )

        s1 = time.time()

        inner_aggs = {}
        # TODO if we are pixel locked, calcuating a centriod seems unnecessary
        category_filters = None
        inner_agg_size = None
        if category_field and histogram_interval is None: # Category Mode
            # We calculate the categories to show based on the mapZoom (which is usually
            # a lower number then the requested tile)
            category_tile = mercantile.Tile(x, y, z)
            if category_tile.z > int(params.get("mapZoom")):
                category_tile = mercantile.parent(category_tile, zoom=int(params["mapZoom"]))

            category_filters, _category_legend = get_tile_categories(
                base_s,
                category_tile.x,
                category_tile.y,
                category_tile.z,
                geopoint_field,
                category_field,
                config.max_legend_items_per_tile,
            )

            if len(category_filters) >= config.max_legend_items_per_tile:
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
        elif category_field and histogram_interval is not None: # Histogram Mode
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
        field_type = get_field_type(config.elastic_hosts, headers, params,geopoint_field, idx)
        partial_data = False # TODO can we get partial data?
        span = None
        if field_type == "geo_point":
            geo_tile_grid = A("geotile_grid", field=geopoint_field, precision=geotile_precision)
            logger.info(params)
            estimated_points_per_tile = get_estimated_points_per_tile(span_range, global_bounds, z, global_doc_cnt)
            if params['bucket_min']>0 or params['bucket_max']<1:
                if estimated_points_per_tile is None:
                    #this isn't good we need a real number so lets query the max aggregation ammount
                    max_value_s = copy.copy(base_s)
                    bucket = max_value_s.aggs.bucket("comp", "geotile_grid", field=geopoint_field,precision=geotile_precision,size=1)
                    if category_field:
                        bucket.metric("sum","sum",field=category_field,missing=0)
                    resp = max_value_s.execute()
                    if category_field:
                        estimated_points_per_tile = resp.aggregations.comp.buckets[0].sum['value']
                    else:
                        estimated_points_per_tile = resp.aggregations.comp.buckets[0].doc_count
                min_bucket = estimated_points_per_tile*params['bucket_min']
                max_bucket = estimated_points_per_tile*params['bucket_max']
                geo_tile_grid.pipeline("selector","bucket_selector",buckets_path={"doc_count":"_count"},script=f"params.doc_count > {min_bucket} && params.doc_count < {max_bucket}")
                logger.info(geo_tile_grid.to_dict())
            if inner_aggs is not None:
                for agg_name, agg in inner_aggs.items():
                    geo_tile_grid.aggs[agg_name] = agg
            tile_s.aggs["comp"] = geo_tile_grid
            resp = Scan([tile_s],timeout=config.query_timeout_seconds)
            # resp = ScanAggs(
            #     tile_s,
            #     {"grids": geo_tile_grid},
            #     inner_aggs,
            #     size=composite_agg_size,
            #     timeout=config.query_timeout_seconds
            # ) #Dont use composite aggregator because you cannot use a bucket selector


            df = pd.DataFrame(
                convert_composite(
                    resp.execute(),
                    (category_field is not None),
                    bool(category_filters),
                    histogram_interval,
                    category_type,
                    category_format
                )
            )

        elif field_type == "geo_shape":
            zoom = 0
            if resolution == "coarse":
                zoom = 5
                spread = 7
            elif resolution == "fine":
                zoom = 6
                spread = 3
            elif resolution == "finest":
                zoom = 7
                spread = 1
            geotile_precision = current_zoom+zoom
            searches = []
            if category_field:
                max_value_s = copy.copy(base_s)
                bucket = max_value_s.aggs.bucket("comp", "geotile_grid", field=geopoint_field,precision=geotile_precision,size=1)
                bucket.metric("sum","sum",field=category_field,missing=0)
                resp = max_value_s.execute()
                estimated_points_per_tile = resp.aggregations.comp.buckets[0].sum['value']
                span = [0,estimated_points_per_tile]
            else:
                max_value_s = copy.copy(base_s)
                max_value_s.aggs.bucket("comp", "geotile_grid", field=geopoint_field,precision=geotile_precision,size=1)
                resp = max_value_s.execute()
                estimated_points_per_tile = resp.aggregations.comp.buckets[0].doc_count
                span = [0,estimated_points_per_tile]
            logger.info("EST Points: %s",estimated_points_per_tile)

            searches = []
            composite_agg_size = 65536#max agg bucket size
            subtile_bb_dict = create_bounding_box_for_tile(x, y, z)
            subtile_s = copy.copy(base_s)
            subtile_s = subtile_s[0:0]
            subtile_s = subtile_s.filter("geo_bounding_box", **{geopoint_field: subtile_bb_dict})
            bucket = subtile_s.aggs.bucket("comp", "geotile_grid", field=geopoint_field,precision=geotile_precision,size=composite_agg_size,bounds=subtile_bb_dict)
            if category_field:
                bucket.metric("sum","sum",field=category_field,missing=0)
            searches.append(subtile_s)
            cmap = "bmy" #todo have front end pass the cmap for none categorical

            # def calc_aggregation(bucket,search):
            #     #get bounds from bucket.key
            #     #do search for sum of values on category_field
            #     z, x, y = [ int(x) for x in bucket.key.split("/") ]
            #     bucket_bb_dict = create_bounding_box_for_tile(x, y, z)
            #     subtile_s = copy.copy(base_s)
            #     subtile_s.aggs.bucket("sum","avg",field=category_field,missing=0)
            #     subtile_s = subtile_s[0:0]
            #     subtile_s = subtile_s.filter("geo_bounding_box", **{geopoint_field: bucket_bb_dict})
            #     response = subtile_s.execute()
            #     search.num_searches += 1
            #     search.total_took += response.took
            #     search.total_shards += response._shards.total  # pylint: disable=W0212
            #     search.total_skipped += response._shards.skipped  # pylint: disable=W0212
            #     search.total_successful += response._shards.successful  # pylint: disable=W0212
            #     search.total_failed += response._shards.failed  # pylint: disable=W0212
            #     bucket.doc_count = response.aggregations.sum['value'] #replace with sum of category_field
            #     return bucket

            def remap_bucket(bucket,search):
                # pylint: disable=unused-argument
                #get bounds from bucket.key
                #remap sub aggregation for sum of values to the doc count
                bucket.doc_count = bucket.sum['value']
                return bucket
            bucket_callback = None
            if category_field:
                #bucket_callback = calc_aggregation #don't run a sub query. sub aggregation worked But we might want to leave this in for cross index searches
                bucket_callback = remap_bucket
            resp = Scan(searches,timeout=config.query_timeout_seconds,bucket_callback=bucket_callback)
            df = pd.DataFrame(
                convert_composite(
                    resp.execute(),
                    False,#we don't need categorical, because ES doesn't support composite buckets for geo_shapes we calculate that with a secondary search in the bucket_callback
                    False,#we dont need filter_buckets, because ES doesn't support composite buckets for geo_shapes we calculate that with a secondary search in the bucket_callback
                    histogram_interval,
                    category_type,
                    category_format
                )
            )
            if len(df)/resp.num_searches == composite_agg_size:
                logger.warning("clipping on tile %s",[x,y,z])

        s2 = time.time()
        logger.info("ES took %s (%s) for %s with %s searches", (s2 - s1), resp.total_took, len(df), resp.num_searches)
        metrics["query_time"] = (s2 - s1)
        metrics["query_took"] = resp.total_took
        metrics["num_searches"] = resp.num_searches
        metrics["aborted"] = resp.aborted
        metrics["shards_total"] = resp.total_shards
        metrics["shards_skipped"] = resp.total_skipped
        metrics["shards_successful"] = resp.total_successful
        metrics["shards_failed"] = resp.total_failed
        logger.info("%s", metrics)


        if len(df.index) == 0:
            img = gen_empty(tile_width_px, tile_height_px)
            if metrics.get("aborted"):
                img = gen_overlay(img, color=(128, 128, 128, 128))
            if params.get("debug"):
                img = gen_debug_overlay(img, f"{z}/{x}/{y}")
            return img, metrics

        ###############################################################
        # Category Mode
        if category_field and field_type != "geo_shape":
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

            # When the number of categories exceeds the number of colors, we can simply
            # replaced the category name with the desired color (i.e. use the color as the category)
            # field.  This is especially important in highly categorical data where the number of
            # categories can be very large and thus cause huge memory allocations in `_colorize`
            #   _, color_key = simplify_categories(
            #       df,
            #       "t",
            #       create_color_key(df["t"].cat.categories, cmap=cmap, highlight=highlight),
            #       inplace=True,
            #   )

            color_key=create_color_key(
                df["t"].cat.categories,
                cmap=cmap,
                highlight=highlight,
                field_min=field_min,
                field_max=field_max,
                histogram_interval=histogram_interval
            )

            x_range, y_range = xy_ranges(x, y, z)
            agg = ds.Canvas(
                plot_width=tile_width_px,
                plot_height=tile_height_px,
                x_range=x_range,
                y_range=y_range,
            ).points(df, "x", "y", agg=ds.by("t", ds.sum("c")))

            span_upper_bound = get_span_upper_bound(span_range, estimated_points_per_tile)
            span = get_span_none(span_upper_bound)
            min_alpha = get_min_alpha(span_range, span_upper_bound)

            logger.debug("MinAlpha:%s Span:%s", min_alpha, span)
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
        else:
            x_range, y_range = xy_ranges(x, y, z)
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
            span_upper_bound = get_span_upper_bound(span_range, estimated_points_per_tile)
            if span is None:
                span = get_span_zero(span_upper_bound)
            logger.info("Span %s %s", span, span_range)
            logger.info("aggs min:%s max:%s",float(agg.min()),float(agg.max()))
            img = tf.shade(agg, cmap=cc.palette[cmap], how="log", span=span)

        ###############################################################
        # Common
        img = apply_spread(img, spread or calculate_pixel_spread(geotile_precision))
        img = img.to_bytesio().read()

        if partial_data:
            logger.info(
                "Generating overlay for tile due to partial category data"
            )
            img = gen_overlay(img, color=(128, 128, 128, 128))
        elif metrics.get("aborted"):
            img = gen_overlay(img, color=(128, 128, 128, 128))

        if params.get("debug"):
            img = gen_debug_overlay(img, f"{z}/{x}/{y}")
        elif metrics.get("aborted"):
            img = gen_overlay(img, color=(128, 128, 128, 128))

        # Set headers and return data
        return img, metrics

    except Exception:
        logger.exception(
            "An exception occured while attempting to generate a tile:"
        )
        raise
