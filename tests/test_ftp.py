from pathlib import Path

from pymatris import Downloader
import pytest

THIS_DIR = Path(__file__).parent

test_file_dir = THIS_DIR / "static/ftp_testfile.txt"


# def test_ftp_download(ftpserver, tmp_path):
#     ftp_url = ftpserver.put_files(
#         {"src": str(test_file_dir), "dest": "ftp_testfile.txt"},
#         style="url",
#         anon=False,
#         overwrite=True,
#     )

#     dm = Downloader()

#     dm.enqueue_file(ftp_url[0], path=tmp_path)
#     f = dm.download()

#     ftp_filepath = os.path.join(ftpserver.server_home, "ftp_testfile.txt")
#     assert len([*tmp_path.iterdir()]) == 1
#     compare_two_files(ftp_filepath, f[0])


# @pytest.mark.allow_hosts(True)
# def test_ftp_custom(tmp_path):
#     dm = Downloader()

#     dm.enqueue_file("ftp://ftp.swpc.noaa.gov/pub/_SRS.tar.gz", path=tmp_path)
#     f = dm.download()

#     assert len(f.errors) == 1
