from pymatris import Downloader
from tests.conftest import validate_test_file_content
from .localserver import crash_handler, fail_between_handler, intermittent_fail_handler
from functools import partial
import pytest
from pathlib import Path


def test_multipartserver(multipartserver, tmp_path):
    dm = Downloader()
    dm.enqueue_file(multipartserver.url, path=tmp_path)
    f = dm.download()

    multipartserver.request_number == 6
    assert len([*tmp_path.iterdir()]) == 1
    validate_test_file_content(f[0], "multipart" * 100)


def test_multipartserver_ranged_http(multipartserver, tmp_path):
    dm = Downloader()
    max_splits = 10
    dm.enqueue_file(multipartserver.url, path=tmp_path, max_splits=max_splits)
    f = dm.download()

    assert multipartserver.request_number == 11  # 1 head requests + 10 splits
    assert len(f.urls) == 1
    assert len(f.errors) == 0
    assert Path(f[0]).name == "multipartfile.txt"
    assert len([*tmp_path.iterdir()]) == 1
    validate_test_file_content(f[0], "multipart" * 100)


def test_multipartserver_default_max_tries(multipartserver, tmp_path):
    # server will fail on only 3rd request
    multipartserver.override = partial(intermittent_fail_handler, 3)

    dm = Downloader()
    dm.enqueue_file(multipartserver.url, path=tmp_path)
    f = dm.download()

    assert multipartserver.request_number == 7  # 1 head request + 5 splits + 1 retry
    assert len(f.urls) == 1
    assert len(f.errors) == 0
    assert Path(f[0]).name == "multipartfile.txt"
    assert len([*tmp_path.iterdir()]) == 1
    validate_test_file_content(f[0], "multipart" * 100)


def test_multipartserver_custom_max_tries(multipartserver, tmp_path):
    multipartserver.override = partial(
        fail_between_handler, 3, 7
    )  # server will fail from 3rd to 7th request
    max_tries = 6
    dm = Downloader()
    dm.enqueue_file(multipartserver.url, path=tmp_path, max_tries=max_tries)
    f = dm.download()

    assert len(f.urls) == 1
    assert len(f.errors) == 0
    assert (
        multipartserver.request_number == 11
    )  # 1 head request + 1 splits + 6 retries(2nd split) + 3 splits
    assert len([*tmp_path.iterdir()]) == 1
    validate_test_file_content(f[0], "multipart" * 100)


@pytest.mark.parametrize("max_tries,expected", [(1, 6), (2, 10), (3, 14)])
def test_multipartserver_exceeds_max_tries(
    multipartserver, tmp_path, max_tries, expected
):
    multipartserver.override = partial(
        crash_handler, 3
    )  # server will fail from 3rd request

    dm = Downloader()
    max_tries = max_tries
    dm.enqueue_file(multipartserver.url, path=tmp_path, max_tries=max_tries)
    f = dm.download()

    assert len(f.urls) == 0
    assert len(f.errors) == 1
    assert (
        multipartserver.request_number == expected
    )  # 1 head request + 1 splits + (4 * max_tries)
    assert not any(tmp_path.iterdir())
