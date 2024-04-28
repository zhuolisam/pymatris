from pymatris import Downloader, SessionConfig


def test_setup():
    dl = Downloader()
    assert dl.queued_downloads == 0
    assert dl.config.max_parallel == 5
    assert dl.config.max_splits == 5


def test_modifyconfig():
    dl = Downloader(
        max_parallel=10,
        max_splits=10,
        max_tries=6,
        session_config=SessionConfig(chunksize=2048, timeouts=600, file_progress=False),
    )
    assert dl.queued_downloads == 0
    assert dl.config.max_parallel == 10
    assert dl.config.max_splits == 10
    assert dl.config.max_tries == 6
    assert dl.config.chunksize == 2048
    assert dl.config.timeouts == 600
    assert dl.config.file_progress is False
