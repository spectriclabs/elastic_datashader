import io
from functools import lru_cache
from hashlib import md5
from typing import Dict, Iterable, Tuple

import numpy as np
from PIL import Image, ImageDraw
from colorcet import palette
from numba import njit
import colorcet as cc

def force_within_range(val: int, lower_inclusive: int, upper_exclusive: int) -> int:
    return max(lower_inclusive, min(val, upper_exclusive-1))

def get_categorical_color_index(category: str, num_colors: int) -> int:
    """
    If the field doesn't have a min/max range we simply take the hash
    and then map it to a color-index.  This ensures colors are consistent
    across independent tile generations, but can result in situations where
    colors are reused before exhausting the color palette.
    """
    category_hash = md5(bytes(category, encoding="utf-8")).hexdigest()
    number = int(category_hash[0:2], 16)
    return number % num_colors

def get_ramp_color_index(category: str, field_min: float, field_max: float, num_colors: int) -> int:
    """
    For ramp cmaps, we map the field range across the palette
    """
    number_str = category.replace(",", "")  # remove commas from large number
    lower_val = float(number_str)
    color_index = int(((lower_val - field_min) / (field_max - field_min)) * num_colors)
    return force_within_range(color_index, 0, num_colors)

def get_histogram_color_index(category: str, field_min: float, field_max: float, num_colors: int):
    number_str, _ = category.rsplit("-", 1)
    number_str = number_str.replace(",", "")  # remove comma from large number
    lower_val = float(number_str)
    color_index = int(((lower_val - field_min) / (field_max - field_min)) * num_colors)
    return force_within_range(color_index, 0, num_colors)

def create_color_key(
    categories: Iterable,
    cmap: str = "glasbey_category10",
    highlight: str = None,
    field_min: float = None,
    field_max: float = None,
    histogram_interval: float = None,
) -> Dict[str, str]:
    """Create a mapping from category to color

    :param categories: Categories to encode as different colors
    :param cmap: Colorcet color-map name (defaults to "glasbey_category10")
    :param highlight: Only colorize this category and make all others a default grey color
    :param field_min: The minimum numerical value for that field
    :param field_max: The maximum numerical value for that field
    :param histogram_interval:
    :return: Dictionary containing each category and their respective color

    :Example:
    >>> create_color_key(["foo", "bar", "baz"])
    {'foo': '#9a0390', 'bar': '#8a9500', 'baz': '#870062'}
    """
    mapping = {}

    for category in categories:
        if category == "Other":
            mapping[category] = '#AAAAAA' # Light Grey
        elif category == "N/A":
            mapping[category] = '#666666' # Dark Grey
        else:
            color_index = None

            if field_min is None or field_max is None:
                color_index = get_categorical_color_index(category, len(palette[cmap]))
            else:
                if field_max - field_min <= 0.0:
                    # If there is a range but it's zero or less, simply map to the last color
                    color_index = len(palette[cmap]) - 1
                else:
                    try:
                        if histogram_interval is None:
                            if is_categorical_cmap(cmap):
                                # for categorical color maps, we want nearby colors to not map to the same index
                                color_index = get_categorical_color_index(category, len(palette[cmap]))
                            else:
                                # For ramp cmaps, we map the field range across the palette
                                color_index = get_ramp_color_index(category, field_min, field_max, len(palette[cmap]))
                        else:
                            # If there is a histogram interval, map the color based off the histogram bin
                            color_index = get_histogram_color_index(category, field_min, field_max, len(palette[cmap]))

                    except ValueError:
                        color_index = get_categorical_color_index(category, len(palette[cmap]))

            if highlight:
                if category == highlight:
                    mapping[category] = palette[cmap][color_index]
                else:
                    mapping[category] = '#D3D3D3' # Light Grey
            elif color_index is not None:
                mapping[category] = palette[cmap][color_index]
            else:
                mapping[category] = '#D3D3D3' # Light Grey

    return mapping


@lru_cache(10)
def gen_overlay_img(width: int, height: int, thickness: int, color: tuple = (255, 0, 0, 64)) -> Image:
    """Create an overlay hash image, using an lru_cache since the same
    overlay can be generated once and then reused indefinitely

    :param width: Width of overlay image
    :param height: Height of overlay image
    :param thickness: Thickness of border
    :return: Image object
    """
    overlay = Image.new("RGBA", (width, height))
    draw = ImageDraw.Draw(overlay)
    for s in range(0, max(height, width), thickness * 2):
        draw.line([(s - width, s + height), (s + width, s - height)], color, thickness)
    return overlay


@lru_cache(10)
def gen_debug_img(width: int, height: int, text: str, thickness: int = 2) -> Image:
    """Create an overlay hash image, using an lru_cache since the same
    overlay can be generated once and then reused indefinitely

    :param width: Width of debug image
    :param height: Height of debug image
    :param text: Text that will be on image
    :param thickness: Thickness of border
    :return: Image object
    """
    overlay = Image.new("RGBA", (width, height))
    draw = ImageDraw.Draw(overlay)
    color = (0, 0, 0, 127)
    draw.rectangle([0, 0, width, height], outline=color, width=thickness)
    draw.text([10, 10], text, fill=color)
    return overlay


def gen_overlay(img, thickness: int = 8, color: tuple = (255, 0, 0, 64)) -> bytes:
    """Generate and overlay to image

    :param img: Image over which to add an overlay
    :param thickness: Thickness of border
    :return: Image bytes
    """
    base = Image.open(io.BytesIO(img))
    overlay = gen_overlay_img(*base.size, thickness=thickness, color=color)
    out = Image.alpha_composite(base, overlay)
    with io.BytesIO() as output:
        out.save(output, format="PNG")
        return output.getvalue()


def gen_debug_overlay(img: bytes, text: str) -> bytes:
    """Generate debug overlay (with text) for image

    :param img: Image to overlay debug
    :param text: Text to put on image
    :return: Debug overlay on image
    """
    base = Image.open(io.BytesIO(img))
    overlay = gen_debug_img(base.size[0], base.size[1], text)
    out = Image.alpha_composite(base, overlay)
    with io.BytesIO() as output:
        out.save(output, format="PNG")
        return output.getvalue()


@lru_cache(10)
def gen_error(width: int, height: int, thickness: int = 8, color: tuple = (255, 0, 0, 255)) -> bytes:
    """Generate error image

    :param width: Width of image
    :param height: Height of image
    :param thickness: Thickness of border
    :return: Error image
    """
    overlay = Image.new("RGBA", (width, height))
    draw = ImageDraw.Draw(overlay)

    # Draw a red border
    draw.line([(0, 0), (width, height)], color, thickness)
    draw.line([(width, 0), (0, height)], color, thickness)

    with io.BytesIO() as output:
        overlay.save(output, format="PNG")
        return output.getvalue()


@lru_cache(10)
def gen_empty(width: int, height: int) -> bytes:
    """Generate empty image

    :param width: Width of image
    :param height: Height of image
    :return: Empty image
    """
    overlay = Image.new("RGBA", (width, height))
    with io.BytesIO() as output:
        overlay.save(output, format="PNG")
        return output.getvalue()


@njit
def ellipse(
    radm: float,
    radn: float,
    tilt: float,
    ypos: float,
    xpos: float,
    num_points: int = 16,
) -> Tuple[np.ndarray, np.ndarray]:
    """Accelerated helper function for generating ellipses from point data

    :param radm: Semimajor axis
    :param radn: Semiminor axis
    :param tilt: Ellipse tilt in radians (0 deg is North)
    :param ypos: Cartesian coordinate y position
    :param xpos: Cartesian coordinate x position
    :param num_points: Number of points with which to draw ellipse
    :return: Tuple containing X points and corresponding Y points for ellipse
    """
    co = np.cos(tilt)
    si = np.sin(tilt)
    the = np.linspace(0, 2 * np.pi, num_points+1)
    yarr = radm * np.cos(the) * co - si * radn * np.sin(the) + ypos
    xarr = radm * np.cos(the) * si + co * radn * np.sin(the) + xpos
    return yarr, xarr

@njit(fastmath=True)
def generate_ellipse_points(
    lat: float,
    lon: float,
    smaj: float,
    smin: float,
    tilt: float = 0,
    n_points: float = 12,
    box_ended: bool = False,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Generate an ellipse polygon with ``n_points``

    Should you need to optimize the performance of a large KML, 8 points per
    ellipse actually looks much better than you would think.

    Parameters
    ----------
    lat : float
        Latitude in decimal degrees

    lon : float
        Longitude in decimal degrees

    smaj : float
        Semi-major axis in meters

    smin : float
        Semi-minor axis in meters

    tilt : float (defaults to 0)
        Tilt angle in degrees, clockwise positive from North = 0

    n_points : int (defaults to 12)
        Number of points for the ellipse

    box_ended : bool (defaults to False)
        Whether or not to have the semi-major axis terminate in
        a flat-nosed or box shape

    Returns
    -------
    [lat], [lon]

    Example
    -------
    >>> lst = generate_ellipse_points(30, 40, 5, 5, n_points=5)
    >>> lst
    array([[30.00004966, 40.        ,  0.        ],
           [30.00001535, 39.99994547,  0.        ],
           [29.99995983, 39.9999663 ,  0.        ],
           [29.99995983, 40.0000337 ,  0.        ],
           [30.00001535, 40.00005453,  0.        ],
           [30.00004966, 40.        ,  0.        ]])
    """
    # earth equatorial radius in meters
    earth_equatorial_radius = 6378137.0

    # measure the topocentric measures of lat and lon at the placemark
    meters_per_deg_lat = 2.0 * np.pi * earth_equatorial_radius / 360.0
    meters_per_deg_lon = meters_per_deg_lat * np.cos(np.radians(lat))

    # the tilt angle is in degrees, clockwise-positive. Convert to cartesian.
    theta = np.radians(90 - tilt)

    # generate a set of points equispaced about a unit circle (this produces
    # placement of vertices along the ellipse at uniform samples of the
    # tangent slopes along the curve, smoothly defining both circles and
    # eccentric ellipses)
    if box_ended:
        # have the semi-major axis terminate in a flat-nosed or box shape
        offset_angle = np.pi / n_points
    else:
        # have the semi-major axis terminate in a pointy shape
        offset_angle = 0

    angles = offset_angle + np.arange(1 + n_points) * np.pi * 2.0 / n_points
    points = np.vstack((np.cos(angles), np.sin(angles)))

    # generate a scaling so that the rendered edges connecting the vertices
    # weave in and out of the circle, yielding a better fit from
    # piecewise-linear to elliptical (rather than fully-inscribing the ellipse
    # or being fully-inscribed by it, this produces symmetric overlap)
    if n_points < 36:
        render_scale = 2.0 / (1.0 + np.cos(np.pi / n_points))
    else:
        # at 10 degrees per point, it's so close to rounded we won't need this
        render_scale = 1.0

    # generate a 2x2 matrix that scales a unit circle to our eccentric ellipse
    eccen_scale = np.array(((smaj, 0.0), (0.0, smin)))

    # generate a 2x2 matrix that rotates points to the correct angle
    cee = np.cos(theta)
    ess = np.sin(theta)
    rotator = np.array(((cee, -ess), (ess, cee)))

    # generate a 2x2 matrix that scales from meters to deg lat and lon
    latlon_scale = np.array(
        ((1.0 / meters_per_deg_lon, 0), (0, 1.0 / meters_per_deg_lat))
    )

    # build an aggregate transformation
    xform = render_scale * np.dot(latlon_scale, np.dot(rotator, eccen_scale))

    # apply the transformation
    points = np.dot(xform, points)

    # add the center lat and lon
    points[0] += lon
    points[1] += lat

    return points[1], points[0]

def initialize_custom_color_maps():
    cc.palette['kibana5'] = [
        '#6eadc1',
        '#57c17b',
        '#6f87d8',
        '#663db8',
        '#bc52bc',
        '#9e3533',
        '#daa05d'
    ]

    cc.palette['hv'] = cc.palette['glasbey_hv'][0:10]
    cc.palette['category10'] = cc.palette['glasbey_category10'][0:10]

CATEGORICAL_CMAPS = set((
  'glasbey_light',
  'glasbey_bw',
  'glasbey',
  'glasbey_cool',
  'glasbey_warm',
  'glasbey_dark',
  'glasbey_category10',
  'glasbey_hv',
  'hv',
  'category10',
  'kibana5'
))

def is_categorical_cmap(cmap):
    return cmap in CATEGORICAL_CMAPS
