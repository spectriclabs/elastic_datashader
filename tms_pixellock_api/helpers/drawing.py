#!/usr/bin/env python3
import hashlib
import io
from functools import lru_cache
from typing import Dict, Tuple

import colorcet as cc
from PIL import Image, ImageDraw
from numba import njit
import numpy as np


def create_color_key(categories, cmap: str = "glasbey_category10") -> Dict[str, str]:
    """

    :param categories:
    :param cmap:
    :return:
    """
    color_key = {}
    for k in categories:
        color_key[k] = cc.palette[cmap][
            int(hashlib.md5(k.encode("utf-8")).hexdigest()[0:2], 16)
        ]
    return color_key


@lru_cache(10)
def gen_overlay_img(width: int, height: int, thickness: int) -> Image:
    """Create an overlay hash image, using an lru_cache since the same
    overlay can be generated once and then reused indefinitely

    :param width:
    :param height:
    :param thickness:
    :return:
    """
    overlay = Image.new("RGBA", (width, height))
    draw = ImageDraw.Draw(overlay)
    color = (255, 0, 0, 64)
    for s in range(0, max(height, width), thickness * 2):
        draw.line([(s - width, s + height), (s + width, s - height)], color, thickness)
    return overlay


@lru_cache(10)
def gen_debug_img(width: int, height: int, text: str, thickness: int = 2) -> Image:
    """Create an overlay hash image, using an lru_cache since the same
    overlay can be generated once and then reused indefinitely

    :param width:
    :param height:
    :param text:
    :param thickness:
    :return:
    """
    overlay = Image.new("RGBA", (width, height))
    draw = ImageDraw.Draw(overlay)
    color = (0, 0, 0, 127)
    draw.rectangle([0, 0, width, height], outline=color, width=thickness)
    draw.text([10, 10], text, fill=color)
    return overlay


def gen_overlay(img, thickness: int = 8) -> bytes:
    """

    :param img:
    :param thickness:
    :return:
    """
    base = Image.open(io.BytesIO(img))
    overlay = gen_overlay_img(*base.size, thickness=thickness)
    out = Image.alpha_composite(base, overlay)
    with io.BytesIO() as output:
        out.save(output, format="PNG")
        return output.getvalue()


def gen_debug_overlay(img: bytes, text: str) -> bytes:
    """Generate debug overlay

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
def gen_error(width: int, height: int, thickness: int = 8) -> bytes:
    """Generate error image

    :param width: Width of image
    :param height: Height of image
    :param thickness: Thickness of border
    :return: Error image
    """
    overlay = Image.new("RGBA", (width, height))
    draw = ImageDraw.Draw(overlay)

    # Draw a red border
    color = (255, 0, 0, 255)
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
    xpos: float,
    ypos: float,
    num_points: int = 16,
) -> Tuple[np.ndarray, np.ndarray]:
    """Accelerated helper function for generating ellipses from point data

    :param radm:
    :param radn:
    :param tilt:
    :param xpos:
    :param ypos:
    :param num_points:
    :return:
    """
    co = np.cos(tilt)
    si = np.sin(tilt)
    the = np.linspace(0, 2 * np.pi, num_points)
    xarr = radm * np.cos(the) * co - si * radn * np.sin(the) + xpos
    yarr = radm * np.cos(the) * si + co * radn * np.sin(the) + ypos
    return xarr, yarr
