from .localserver import MultiPartServer, SimpleSFTPServer

import pytest
from pathlib import Path
from pymatris.utils import sha256sum


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


# @pytest.fixture
# def ftpserver(ftpserver):
#     ftpserver.put_files()

#     return ftpserver


@pytest.fixture(scope="function")
def sftp_server():
    server = SimpleSFTPServer(
        contents={"test_folder": {"testfile.txt": "Hello World From SFTP"}}
    ).server
    server.start()
    yield server
    server.shutdown()


def validate_test_file(f, shasum):
    assert sha256sum(Path(f)) == shasum


def validate_test_file_content(f, content):
    with open(f) as downloaded:
        assert downloaded.read() == content


def compare_two_files(path1, path2):
    with open(path1) as original, open(path2) as downloaded:
        assert original.read() == downloaded.read()
