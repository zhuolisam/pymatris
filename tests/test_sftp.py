from pymatris import Downloader
from .conftest import validate_test_file_content
from pathlib import Path


def test_sftp_download(sftp_server, tmp_path):
    dm = Downloader()
    dm.enqueue_file(f"{sftp_server.url}/testfile.txt", path=tmp_path)

    f = dm.download()
    assert len(f.urls) == 1
    assert Path(f[0]).name == "testfile.txt"
    validate_test_file_content(f[0], "Hello World From SFTP")


def test_sftp_nosuchfile(sftp_server, tmp_path):
    dm = Downloader()
    dm.enqueue_file(f"{sftp_server.url}/nonexistentfile.txt", path=tmp_path)
    f = dm.download()

    assert len(f.urls) == 0
    assert len(f.errors) == 1
    assert not any(tmp_path.iterdir())  # Make sure tmp_path is empty


# pytest_sftpserver does not provide credentials mock
# def test_sftp_wrongpassword(sftp_server, tmp_path):
#     dm = Downloader()

#     build_url = f"sftp://blablabla:fakepasswordd@{sftp_server.host}:{sftp_server.port}/test_folder/testfile.txt"

#     dm.enqueue_file(build_url, path=tmp_path)
#     f = dm.download()
#     assert len(f.urls) == 0
#     assert len(f.errors) == 1
#     assert not any(tmp_path.iterdir())  # Make sure tmp_path is empty
