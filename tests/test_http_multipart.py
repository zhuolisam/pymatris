from pymatris import Downloader
from .localserver import crash_handler, fail_between_handler, intermittent_fail_handler
from functools import partial
import pytest


def test_multipartserver(multipartserver, tmp_path):
    dm = Downloader()
    dm.enqueue_file(multipartserver.url, path=tmp_path)

    f = dm.download()

    # Verify contents are transferred
    with open(f[0], "rb") as dl_file:
        assert dl_file.read() == b"multipart" * 100

    multipartserver.request_number == 6
    assert len([*tmp_path.iterdir()]) == 1


def test_multipartserver_ranged_http(multipartserver, tmp_path):
    dm = Downloader()
    max_splits = 10
    dm.enqueue_file(multipartserver.url, path=tmp_path, max_splits=max_splits)

    f = dm.download()

    # Verify contents are transferred
    with open(f[0], "rb") as dl_file:
        assert dl_file.read() == b"multipart" * 100

    assert multipartserver.request_number == 11  # 1 head requests + 10 splits
    assert len([*tmp_path.iterdir()]) == 1


def test_multipartserver_default_max_tries(multipartserver, tmp_path):
    multipartserver.override = partial(intermittent_fail_handler, 3)
    # server will fail on only 3rd request
    dm = Downloader()
    dm.enqueue_file(multipartserver.url, path=tmp_path)

    f = dm.download()

    assert len(f.errors) == 0
    assert multipartserver.request_number == 7  # 1 head request + 5 splits + 1 retry
    assert len([*tmp_path.iterdir()]) == 1


def test_multipartserver_custom_max_tries(multipartserver, tmp_path):
    multipartserver.override = partial(
        fail_between_handler, 3, 7
    )  # server will fail from 3rd to 7th request
    dm = Downloader()
    dm.enqueue_file(multipartserver.url, path=tmp_path, max_tries=6)

    f = dm.download()

    assert len(f.errors) == 0
    assert (
        multipartserver.request_number == 11
    )  # 1 head request + 1 splits + 6 retry + 3 remaining splits
    assert len([*tmp_path.iterdir()]) == 1


@pytest.mark.parametrize("max_tries, expected", [(1, 3), (2, 4), (3, 5)])
def multipartserver_exceeds_max_tries(multipartserver, tmp_path, max_tries, expected):
    multipartserver.override = partial(
        crash_handler, 3
    )  # server will fail from 3rd request
    dm = Downloader()
    max_tries = max_tries
    dm.enqueue_file(multipartserver.url, path=tmp_path, max_tries=max_tries)

    f = dm.download()

    assert len(f.errors) == 0
    assert (
        multipartserver.request_number == expected
    )  # 1 head request + 1 splits + 5 retry
    assert not any(tmp_path.iterdir())
