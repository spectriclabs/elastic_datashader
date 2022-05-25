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
    "url, next_wait, expected_url",
    (
        (
            URL("https://foo:bar@baz.com/tms/someindex/5/10/15.png?c1=spectric&c2=labs"),
            2,
            "https://foo:bar@baz.com/tms/2/someindex/5/10/15.png?c1=spectric&c2=labs"
        ),
        (
            URL("https://foo:bar@baz.com/tms/10/someindex/5/10/15.png?c1=spectric&c2=labs"),
            15,
            "https://foo:bar@baz.com/tms/15/someindex/5/10/15.png?c1=spectric&c2=labs"
        ),
    )
)
def test_make_next_wait_url(url, next_wait, expected_url):
    assert make_next_wait_url(url, next_wait) == expected_url
