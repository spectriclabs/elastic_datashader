#!/usr/bin/env pytest
import numpy as np
from PIL import Image
import pytest
from tms_datashader_api.helpers import drawing


@pytest.mark.parametrize(
    "categories,expected",
    (
        (("foo", "bar", "baz"), {"bar": "#8a9500", "baz": "#870062", "foo": "#9a0390"}),
        ([], {}),
    ),
)
def test_create_color_key(categories, expected):
    assert drawing.create_color_key(categories) == expected


def test_gen_overlay_img():
    width = 256
    height = 256
    thickness = 2
    expected = Image.open("./tests/dat/gen_overlay_img.png")
    img = drawing.gen_overlay_img(width, height, thickness)

    np.testing.assert_equal(np.array(expected), np.array(img))


def test_gen_debug_img():
    width = 256
    height = 256
    text = "Hello, world!"
    thickness = 5
    expected = Image.open("./tests/dat/gen_debug_img.png")
    img = drawing.gen_debug_img(width, height, text, thickness)

    np.testing.assert_equal(np.array(expected), np.array(img))


def test_gen_overlay():
    img = drawing.gen_empty(256, 256)
    with open("./tests/dat/gen_overlay.txt", "rb") as expected_file:
        expected = expected_file.read()
    actual = drawing.gen_overlay(img)
    assert expected == actual


def test_gen_debug_overlay():
    img = drawing.gen_empty(256, 256)
    with open("./tests/dat/gen_debug_overlay.txt", "rb") as expected_file:
        expected = expected_file.read()
    actual = drawing.gen_debug_overlay(img, "hello, world!")
    assert expected == actual


def test_gen_error():
    with open("./tests/dat/gen_error.txt", "rb") as expected_file:
        expected = expected_file.read()
    actual = drawing.gen_error(256, 256, 5)
    assert expected == actual


def test_gen_empty():
    with open("./tests/dat/gen_empty.txt", "rb") as expected_file:
        expected = expected_file.read()
    actual = drawing.gen_empty(256, 256)
    assert expected == actual


def test_ellipse():
    # Verify tilt of 0 means North
    radm = 100
    radn = 50
    tilt = 0
    xpos = 0
    ypos = 0
    actual_y, actual_x = drawing.ellipse(radm, radn, tilt, xpos, ypos)
    np.testing.assert_almost_equal(actual_y[0], 100)
    np.testing.assert_almost_equal(actual_y[4], 0)
    np.testing.assert_almost_equal(actual_y[8], -100)
    np.testing.assert_almost_equal(actual_y[12], 0)
    np.testing.assert_almost_equal(actual_x[0], 0)
    np.testing.assert_almost_equal(actual_x[4], 50)
    np.testing.assert_almost_equal(actual_x[8], 0)
    np.testing.assert_almost_equal(actual_x[12], -50)

    

    radm = 100
    radn = 30
    tilt = 10
    xpos = 0
    ypos = 5
    actual_y, actual_x = drawing.ellipse(radm, radn, tilt, xpos, ypos)
    print(actual_y)
    print(actual_x)
    expected_x = np.array(
        [
            -49.40211109,
            -54.89396014,
            -51.26749671,
            -39.07481696,
            -20.17214587,
              2.56275624,
             25.66870662,
             45.62803378,
             59.40211109,
             64.89396014,
             61.26749671,
             49.07481696,
             30.17214587,
              7.43724376,
            -15.66870662,
            -35.62803378,
            -49.40211109
        ]
    )
    expected_y = np.array(
        [
            -83.90715291,
            -71.27446522,
            -47.79088631,
            -17.03157819,
             16.32063333,
             47.18817636,
             70.87174731,
             83.76573718,
             83.90715291,
             71.27446522,
             47.79088631,
             17.03157819,
            -16.32063333,
            -47.18817636,
            -70.87174731,
            -83.76573718,
            -83.90715291
        ]
    )

    np.testing.assert_allclose(expected_x, actual_x)
    np.testing.assert_allclose(expected_y, actual_y)
