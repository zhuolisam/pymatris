from unittest.mock import patch
from pymatris import Downloader


def throwerror(*args, **kwargs):
    raise ValueError("Out of Cheese.")


@patch("pymatris.downloader.default_name", throwerror)
def test_raises_other_exception(httpserver, tmp_path):
    httpserver.serve_content("Hello World")
    dm = Downloader()

    dm.enqueue_file(httpserver.url, path=tmp_path)
    res = dm.download()
    assert isinstance(res.errors[0].exception, ValueError)
