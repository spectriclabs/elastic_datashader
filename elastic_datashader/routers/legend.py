from copy import copy
from json import dumps, loads
from typing import Optional

from fastapi import APIRouter, Request, Response

import mercantile
import pynumeral

from ..config import config
from ..drawing import create_color_key
from ..elastic import (
    get_search_base,
    get_tile_categories,
    make_label,
    to_32bit_float,
)
from ..logger import logger
from ..parameters import extract_parameters, merge_generated_parameters

router = APIRouter()

def legend_response(data: str, error: Optional[Exception]=None, **kwargs) -> Response:
    headers={
        "Cache-Control": "max-age=60",
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }

    if kwargs.get("parameter_hash"):
        headers["Datashader-Parameter-Hash"] = kwargs["parameter_hash"]

    if kwargs.get("params"):
        headers["Datashader-RunAs-User"] = kwargs["params"].get("user", "")

    if error is not None:
        headers["Error"] = str(error)

    return Response(data, status_code=200, headers=headers)

@router.get("/{idx}/{field_name}/legend.json")
async def provide_legend(idx: str, field_name: str, request: Request):  # pylint: disable=W0613
    # Extract out special extent parameter that is independent from hash
    extent = None
    params = request.query_params.get("params")

    if params and params != "{params}":
        params = loads(params)
        extent = params.get("extent")

    zoom = params.get("zoom")

    if zoom is None and extent:
        zoom = mercantile.bounding_tile(
            max(-180.0, extent["minLon"]),
            max(-90.0, extent["minLat"]),
            max(180.0, extent["maxLon"]),
            max(90.0, extent["maxLat"]),
        ).z - 1

    elif zoom is None and not extent:
        return legend_response("[]", "no zoom")

    zoom = int(zoom)

    # Get hash and parameters
    try:
        parameter_hash, params = extract_parameters(request.headers, request.query_params)
    except Exception as e:  # pylint: disable=W0703
        logger.exception("Error while extracting parameters")
        return legend_response("[]", e)

    params = merge_generated_parameters(request.headers, params, idx, parameter_hash)

    # Assign param value to legacy keyword values
    geopoint_field = params["geopoint_field"]
    category_type = params["category_type"]
    category_histogram = params["category_histogram"]
    category_format = params["category_format"]
    cmap = params["cmap"]
    histogram_interval = params.get("generated_params", {}).get(
        "histogram_interval", None
    )
    field_min = params.get("generated_params", {}).get(
        "field_min", None
    )
    field_max = params.get("generated_params", {}).get(
        "field_max", None
    )

    # If not in category mode, just return nothing
    if params["category_field"] is None:
        return legend_response("[]", parameter_hash=parameter_hash, params=params)

    cmap = params["cmap"]
    category_field = params["category_field"]
    geopoint_field = params["geopoint_field"]

    base_s = get_search_base(config.elastic_hosts, request.headers, params, idx)

    if extent:
        legend_bbox = {
            "top_left": {
                "lat": min(90.0, extent["maxLat"]),
                "lon": max(-180.0, extent["minLon"]),
            },
            "bottom_right": {
                "lat": max(-90.0, extent["minLat"]),
                "lon": min(180.0, extent["maxLon"]),
            },
        }
        logger.info("legend_bbox: %s", legend_bbox)
        base_s = base_s.filter("geo_bounding_box", **{geopoint_field: legend_bbox})

    legend_s = copy(base_s)
    legend_s = legend_s.params(size=0)

    legend = {}

    if histogram_interval is not None and category_histogram in (True, None):
        # Put in the histogram search
        legend_s.aggs.bucket(
            "categories",
            "histogram",
            field=category_field,
            interval=histogram_interval,
            min_doc_count=1,
        )

        # Perform the execution
        response = legend_s.execute()
        # If no categories then return blank list
        if not hasattr(response.aggregations, "categories"):
            return legend_response("[]", parameter_hash=parameter_hash, params=params)

        # Generate the legend list
        for category in response.aggregations.categories:
            # Bin the data
            label = make_label(float(category.key), histogram_interval, category_format)
            legend[label] = category.doc_count

    elif category_field:
        tiles_iter = mercantile.tiles(
            max(-180.0, extent["minLon"]),
            max(-90.0, extent["minLat"]),
            min(180.0, extent["maxLon"]),
            min(90.0, extent["maxLat"]),
            zoom
        )

        for tile in tiles_iter:
            #Query the database to get the categories for this tile
            _, tile_legend = get_tile_categories(
                base_s,
                tile.x,
                tile.y,
                tile.z,
                geopoint_field,
                category_field,
                int(config.max_legend_items_per_tile),
            )

            for k, v in tile_legend.items():
                if category_type == "number":
                    try:
                        k = pynumeral.format(to_32bit_float(k), category_format)
                    except ValueError:
                        k = str(k)
                else:
                    k = str(k)
                legend[k] = legend.get(k, 0) + v

    color_key_legend = []

    if not legend:
        return legend_response("[]", parameter_hash=parameter_hash, params=params)

    # Extract other to put it at the end
    other = legend.pop("Other", None)
    for k, count in sorted(legend.items(), key=lambda x: x[1], reverse=True):
        c = create_color_key([k], cmap=cmap, field_min=field_min, field_max=field_max, histogram_interval=histogram_interval).get(
            str(k), "#000000"
        )
        color_key_legend.append({"key": k, "color": c, "count": count})

    # Add Other to the end
    if other:
        k = "Other"
        count = other

        if not params.get("ellipses"):
            c = create_color_key([k], cmap=cmap, field_min=field_min, field_max=field_max, histogram_interval=histogram_interval).get(
                str(k), "#000000"
            )
        else:
            # In ellipse mode everything gets it's own color
            # so there is never a color for Other
            c = None

        color_key_legend.append({"key": k, "color": c, "count": count})

    return legend_response(dumps(color_key_legend), parameter_hash=parameter_hash, params=params)
