# from pymatris import Downloader

# from .conftest import validate_test_file_content


# def test_ftp_server(ftp_server, tmp_path):
#     pathlists = list(ftp_server.get_file_contents("testfile.txt", style="url"))

#     ftpfile = pathlists[0]

#     dm = Downloader()
#     dm.enqueue_file(ftpfile["path"], path=tmp_path)
#     f = dm.download()

#     assert len([*tmp_path.iterdir()]) == 1
#     validate_test_file_content(f[0], ftpfile["content"])


# def test_ftp_nosuchfile(ftp_server, tmp_path):
#     # ftp://fakeusername:qweqwe@localhost:8888
#     ftp_host = ftp_server.get_login_data(style="url", anon=False)

#     dm = Downloader()
#     dm.enqueue_file(f"{ftp_host}/nonexistentfile.txt", path=tmp_path)
#     f = dm.download()

#     assert len(f.urls) == 0
#     assert len(f.errors) == 1
#     assert not any(tmp_path.iterdir())  # Make sure tmp_path is empty
