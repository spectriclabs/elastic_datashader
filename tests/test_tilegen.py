import pytest

from geopy.distance import distance
from mercantile import tile

from elastic_datashader import tilegen

@pytest.mark.parametrize(
    "lon, lat, zoom, search_meters",
    (
        (0.0, 0.0, 18, 1852),
        (-73.986, 40.7485, 18, 1852),
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

    top_left = (bb_dict["top_left"]["lat"], bb_dict["top_left"]["lon"])
    top_right = (bb_dict["top_left"]["lat"], bb_dict["bottom_right"]["lon"])
    assert distance(top_left, top_right).m / (search_meters * 1.5 * 2) == pytest.approx(1.0, 0.1)
