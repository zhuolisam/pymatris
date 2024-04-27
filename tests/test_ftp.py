# from pathlib import Path
# import os
# from pymatris import Downloader

# from .conftest import compare_two_files


# THIS_DIR = Path(__file__).parent

# test_file_dir = THIS_DIR / "static/ftp_testfile.txt"


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


# def test_sftp_nosuchfile(ftpserver, tmp_path):
#     # ftp://fakeusername:qweqwe@localhost:8888
#     ftp_host = ftpserver.get_login_data(style="url", anon=False)

#     dm = Downloader()
#     dm.enqueue_file(f"{ftp_host}/nonexistentfile.txt", path=tmp_path)
#     f = dm.download()

#     assert len(f.urls) == 0
#     assert len(f.errors) == 1
#     assert not any(tmp_path.iterdir())  # Make sure tmp_path is empty
