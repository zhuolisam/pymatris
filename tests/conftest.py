from .localserver import MultiPartServer

import pytest
from pathlib import Path
from pymatris.utils import sha256sum


@pytest.fixture
def singlepartserver(httpserver):
    httpserver.serve_content(
        b"Hello World!",
        200,
        headers={"Content-Disposition": "attachment; filename=testfile.txt"},
    )

    return httpserver


@pytest.fixture
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


def validate_test_file(f, shasum):
    assert Path(f).name == "testfile.txt"
    assert sha256sum(Path(f)) == shasum
