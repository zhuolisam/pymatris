from .localserver import MultiPartServer, SimpleSFTPServer

import pytest
from pathlib import Path
from pymatris.utils import sha256sum

THIS_DIR = Path(__file__).parent

ftp_testfile = THIS_DIR / "static/ftp_testfile.txt"


@pytest.fixture(scope="function")
def singlepartserver(httpserver):
    httpserver.serve_content(
        b"Hello World!",
        200,
        headers={"Content-Disposition": "attachment; filename=testfile.txt"},
    )

    return httpserver


@pytest.fixture(scope="function")
def singlepartserverfail(httpserver):
    httpserver.serve_content("File not found!", 404)
    return httpserver


@pytest.fixture(scope="function")
def multipartserver():
    server = MultiPartServer()

    server.start_server()
    yield server
    server.stop_server()


@pytest.fixture(scope="function")
def sftp_server():
    server = SimpleSFTPServer(contents={"testfile.txt": "Hello World From SFTP"}).server
    server.start()
    yield server
    if server.is_alive():
        server.shutdown()


# @pytest.fixture(scope="function")
# def ftp_server(ftpserver):
#     ftpserver.put_files(
#         {"src": str(ftp_testfile), "dest": "testfile.txt"},
#         style="url",
#         anon=False,
#         overwrite=True,
#     )

#     return ftpserver


def validate_test_file(f, shasum):
    assert sha256sum(Path(f)) == shasum


def validate_test_file_content(f, content):
    with open(f) as downloaded:
        assert downloaded.read() == content


def compare_two_files(path1, path2):
    with open(path1) as original, open(path2) as downloaded:
        assert original.read() == downloaded.read()
