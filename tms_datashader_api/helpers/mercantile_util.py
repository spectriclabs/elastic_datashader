#!/usr/bin/env python
"""
mercantile_util.py contains modified versions of
many of the mercantile functions, just with numba acceleration
and catered specifically to our use-case
"""
import numba
import numpy as np


__all__ = [
    "lnglat",
    "ul",
    "xy_bounds",
    "bounds",
    "_xy",
    "tile",
    "num_tiles",
    "tiles_bounds",
]


R2D = 57.29577951308232  # 180 / PI
RE = 6378137.0
HALF_CE = np.pi * RE
CE = 2 * HALF_CE
INV_RE = 1.567855942887398e-07
EPSILON = 1e-14
LL_EPSILON = 1e-11
INV_PI = 1 / np.pi
INV_360 = 1 / 360.0


@numba.njit(fastmath=True)
def lnglat(x, y):
    return (
        x * R2D * INV_RE,
        ((np.pi * 0.5) - 2.0 * np.arctan(np.exp(-y * INV_RE))) * R2D,
    )


@numba.njit(fastmath=True)
def ul(xtile, ytile, zoom):
    inv_z2 = 1 / 2 ** zoom
    return (
        # lon_deg
        xtile * inv_z2 * 360.0 - 180.0,
        # lat_deg
        np.degrees(np.arctan(np.sinh(np.pi * (1 - 2 * ytile * inv_z2)))),
    )


@numba.njit(fastmath=True)
def xy_bounds(xtile, ytile, zoom):
    tile_size = CE / 2 ** zoom

    left = xtile * tile_size - HALF_CE
    right = left + tile_size

    top = HALF_CE - ytile * tile_size
    bottom = top - tile_size

    return left, bottom, right, top


@numba.njit(fastmath=True)
def bounds(xtile, ytile, zoom):
    a = ul(xtile, ytile, zoom)
    b = ul(xtile + 1, ytile + 1, zoom)
    return a[0], b[1], b[0], a[1]  # west, south, east, north

@numba.njit(fastmath=True)
def center(xtile, ytile, zoom):
    a = ul(xtile, ytile, zoom)
    b = ul(xtile + 1, ytile + 1, zoom)
    return ((a[0] + b[0]) / 2), ((b[1] + a[1]) / 2)  # lon, lat

@numba.njit(fastmath=True)
def _xy(lng, lat):
    x = lng * INV_360 + 0.5
    sinlat = np.sin(np.radians(lat))

    denom = 1.0 - sinlat
    if denom == 0:
        raise ValueError("Y can not be computed due to lat value")

    log_expr = (1.0 + sinlat) / denom
    y = 0.5 - 0.25 * np.log(log_expr) * INV_PI
    return x, y


@numba.njit(fastmath=True)
def tile(lng, lat, zoom):
    x, y = _xy(lng, lat)
    z2 = 2 ** zoom

    if x <= 0:
        xtile = 0
    elif x >= 1:
        xtile = int(z2 - 1)
    else:
        # Heuristic to find points straddling tiles.
        xtile_a = np.floor((x - EPSILON) * z2)
        xtile_b = np.floor((x + EPSILON) * z2)
        if xtile_a != xtile_b:
            xtile = int(xtile_b)
        else:
            xtile = int(xtile_a)

    if y <= 0:
        ytile = 0
    elif y >= 1:
        ytile = int(z2 - 1)
    else:
        # Heuristic to find points straddling tiles.
        ytile_a = np.floor((y + EPSILON) * z2)
        ytile_b = np.floor((y - EPSILON) * z2)
        if ytile_a != ytile_b:
            ytile = int(ytile_a)
        else:
            ytile = int(ytile_b)

    return xtile, ytile, zoom


@numba.njit(fastmath=True)
def _num_tiles_in_bbox(w, s, e, n, zoom):
    w = max(-180.0, w)
    s = max(-85.051129, s)
    e = min(180.0, e)
    n = min(85.051129, n)
    ul_tile = tile(w, n, zoom)
    lr_tile = tile(e - LL_EPSILON, s + LL_EPSILON, zoom)
    return (lr_tile[0] + 1 - ul_tile[0]) * (lr_tile[1] + 1 - ul_tile[1])


@numba.njit(fastmath=True)
def num_tiles(west, south, east, north, zoom):
    if west > east:
        return (
            # bbox_west
            _num_tiles_in_bbox(-180.0, south, east, north, zoom)
            +
            # bbox_east
            _num_tiles_in_bbox(west, south, 180.0, north, zoom)
        )
    else:
        return _num_tiles_in_bbox(west, south, east, north, zoom)


@numba.njit(fastmath=True, parallel=True, nogil=True)
def _tiles_in_bbox(w, s, e, n, z):
    w = max(-180.0, w)
    s = max(-85.051129, s)
    e = min(180.0, e)
    n = min(85.051129, n)

    ul_tile = tile(w, n, z)
    lr_tile = tile(e - LL_EPSILON, s + LL_EPSILON, z)

    num_i = lr_tile[0] + 1 - ul_tile[0]
    num_j = lr_tile[1] + 1 - ul_tile[1]
    num_tiles = num_i * num_j

    result = np.zeros((num_tiles, 4), dtype=np.float64)

    # Because numba doesn't support np.tile or np.resize yet
    current_tile = 0
    for i in range(ul_tile[0], lr_tile[0] + 1):
        for j in range(ul_tile[1], lr_tile[1] + 1):
            result[current_tile] = bounds(i, j, z)
            current_tile += 1
    return result


@numba.njit(fastmath=True)
def tiles_bounds(west, south, east, north, zoom):
    if west > east:
        tiles_west = _tiles_in_bbox(-180.0, south, east, north, zoom)
        tiles_east = _tiles_in_bbox(west, south, 180.0, north, zoom)
        return np.concatenate((tiles_west, tiles_east))
    else:
        return _tiles_in_bbox(west, south, east, north, zoom)
