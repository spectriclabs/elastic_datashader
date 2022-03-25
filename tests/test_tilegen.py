import pytest

from geopy.distance import distance
from mercantile import tile

from elastic_datashader import tilegen

@pytest.mark.parametrize(
    "lon, lat, zoom, search_meters",
    (
        (0.0, 0.0, 10, 1852),
        (-73.986, 40.7485, 10, 1852),
    )
)
def test_create_bounding_box_for_ellipses(lon, lat, zoom, search_meters):
    x, y, z = tile(lon, lat, zoom)
    bb_dict = tilegen.create_bounding_box_for_ellipses(x, y, z, search_meters)

    assert "top_left" in bb_dict
    assert "bottom_right" in bb_dict

    assert "lat" in bb_dict["top_left"]
    assert "lon" in bb_dict["top_left"]
    assert "lat" in bb_dict["bottom_right"]
    assert "lon" in bb_dict["bottom_right"]

    bb_top_left = (bb_dict["top_left"]["lat"], bb_dict["top_left"]["lon"])
    bb_top_right = (bb_dict["top_left"]["lat"], bb_dict["bottom_right"]["lon"])
    actual_bb_top_meters = distance(bb_top_left, bb_top_right).m
    assert actual_bb_top_meters > 2 * search_meters
