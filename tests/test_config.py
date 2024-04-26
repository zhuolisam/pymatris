from pymatris import Downloader, SessionConfig


def test_setup():
    dl = Downloader()
    assert dl.queued_downloads == 0
    assert dl.config.max_conn == 5
    assert dl.config.max_splits == 5


def test_modifyconfig():
    dl = Downloader(max_conn=10, max_splits=10, config=SessionConfig(max_tries=6))
    assert dl.queued_downloads == 0
    assert dl.config.max_conn == 10
    assert dl.config.max_splits == 10
    assert dl.config.max_tries == 6
