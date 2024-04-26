from pathlib import Path
import os
from pymatris.downloader import Downloader


THIS_DIR = Path(__file__).parent

test_file_dir = THIS_DIR / "static/ftp_testfile.txt"


def compare_content(path1, path2):
    with open(path1) as original, open(path2) as downloaded:
        assert original.read() == downloaded.read()


def test_ftp_download(ftpserver, tmp_path):
    ftp_url = ftpserver.put_files(
        {"src": str(test_file_dir), "dest": "ftp_testfile.txt"},
        style="url",
        anon=False,
        overwrite=True,
    )

    dm = Downloader()

    dm.enqueue_file(ftp_url[0], path=tmp_path)
    f = dm.download()

    ftp_filepath = os.path.join(ftpserver.server_home, "ftp_testfile.txt")
    assert len([*tmp_path.iterdir()]) == 1
    compare_content(ftp_filepath, f[0])
