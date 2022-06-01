import pytest

from starlette.datastructures import URL

from elastic_datashader.routers.tms import get_next_wait, make_next_wait_url

def test_get_next_wait():
    assert get_next_wait(0) == 2
    assert get_next_wait(1) == 5
    assert get_next_wait(2) == 5
    assert get_next_wait(5) == 10
    assert get_next_wait(10) == 15
    assert get_next_wait(-1) == 2

@pytest.mark.parametrize(
    "idx, x, y, z, first_wait, next_wait, expected_url",
    (
        (
            "someindex", 10, 15, 5, True, 2,
            "../../../2/someindex/5/10/15.png"
        ),
        (
            "someindex", 10, 15, 5, False, 15,
            "../../../../15/someindex/5/10/15.png"
        ),
    )
)
def test_make_next_wait_url(idx, x, y, z, first_wait, next_wait, expected_url):
    assert make_next_wait_url(idx, x, y, z, first_wait, next_wait) == expected_url
