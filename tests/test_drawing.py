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
    radm = 100
    radn = 30
    tilt = 10
    xpos = 0
    ypos = 5
    actual_x, actual_y = drawing.ellipse(radm, radn, tilt, xpos, ypos)
    expected_x = np.array(
        [
            -83.90715291,
            -70.01479879,
            -44.0162499,
            -10.40689152,
            25.00191294,
            56.08765952,
            77.47534023,
            85.46683077,
            78.68032983,
            58.28928507,
            27.81949339,
            -7.46054144,
            -41.45058087,
            -68.27343831,
            -83.29119801,
            -83.90715291,
        ]
    )
    expected_y = np.array(
        [
            -49.40211109,
            -54.93723558,
            -50.10866753,
            -35.75131022,
            -14.34768115,
            10.40133775,
            34.21641629,
            52.97971103,
            63.44687785,
            63.80804851,
            54.00077334,
            35.72081929,
            12.1289565,
            -12.69556763,
            -34.46036736,
            -49.40211109,
        ]
    )

    np.testing.assert_allclose(expected_x, actual_x)
    np.testing.assert_allclose(expected_y, actual_y)
