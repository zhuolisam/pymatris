import pytest
from pymatris import Downloader
from .conftest import validate_test_file


def test_http_download(httpserver, tmp_path):
    httpserver.serve_content(
        "HIRE ME! I'M A TEST FILE!",
        headers={"Content-Disposition": "attachment; filename=testfile.txt"},
    )
    dm = Downloader()
    dm.enqueue_file(httpserver.url, path=tmp_path, max_splits=None)

    assert dm.queued_downloads == 1

    f = dm.download()
    f.urls == [httpserver.url]
    validate_test_file(
        f[0], "e74f4c92ee3794ed642d88e9a470d9d582e7e946aa5ffdb3b6f7060b9856046e"
    )  # Generated from echo -n -e "HIRE ME\! I'M A TEST FILE\!" | shasum -a 256


@pytest.mark.parametrize("max_tries,expected", [(1, 1), (2, 2), (3, 3)])
def test_http_download_fails(singlepartserverfail, tmp_path, max_tries, expected):
    dm = Downloader(
        max_tries=max_tries,
    )
    dm.enqueue_file(
        singlepartserverfail.url,
        path=tmp_path,
    )

    assert dm.queued_downloads == 1

    f = dm.download()

    assert len(f.errors) == 1
    assert len(singlepartserverfail.requests) == expected

    assert not any(tmp_path.iterdir())  # make sure tmp_path is empty


def test_invalid_url(tmp_path):
    dm = Downloader()
    dm.enqueue_file(
        "http://nosuchurl.com/",
        path=tmp_path,
    )

    f = dm.download()

    assert len(f.errors) == 1
    assert len(f.urls) == 0
