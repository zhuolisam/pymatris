from typing import Callable, Optional
from pytest_localserver.http import WSGIServer
from pytest_sftpserver.sftp.server import SFTPServer


class MultiPartServer:
    def __init__(self, override: Optional[Callable] = None):
        self.requests = []
        self.app = WSGIServer(application=self.request_handler)
        self.request_number = 0
        self.override = override

    def override_handler(self, environ, start_response):
        if self.override:
            return self.override(self.request_number, environ, start_response)

    def request_handler(self, environ, start_response):
        self.requests.append(environ)
        self.request_number += 1
        callback_return = self.override_handler(environ, start_response)
        if callback_return:
            return callback_return

        content = b"multipart" * 100

        byte_start = 0
        byte_end = content_length = len(content)
        http_range = environ.get("HTTP_RANGE", None)

        # Default Handler
        if http_range:
            http_bytes = http_range.split("bytes=")[1]
            byte_start = int(http_bytes.split("-")[0])
            byte_end = http_bytes.split("-")[1]

            # if empty, then take content_length
            if not byte_end:
                byte_end = content_length
            else:
                byte_end = int(byte_end)

        content_length = byte_end - byte_start

        response_headers = [
            ("Content-type", "text/plain"),
            ("Content-Length", content_length),
            ("Accept-Ranges", "bytes"),
            ("Content-Disposition", "attachment; filename=multipartfile.txt"),
        ]
        status = "200 OK"
        start_response(status, response_headers)
        return [content[byte_start:byte_end]]

    def start_server(self):
        self.app.start()

    def stop_server(self):
        self.app.stop()

    @property
    def url(self):
        return self.app.url


def intermittent_fail_handler(i, cur, environ, start_response):
    if i == cur:
        status = "404 Not Found"
        response_headers = [("Content-type", "text/plain")]
        start_response(status, response_headers)
        return [b""]


def crash_handler(i, cur, environ, start_response):
    if i <= cur:
        status = "404 Not Found"
        response_headers = [("Content-type", "text/plain")]
        start_response(status, response_headers)
        return [b""]


def fail_between_handler(start, end, cur, environ, start_response):
    if start <= cur and cur <= end:
        status = "404 Not Found"
        response_headers = [("Content-type", "text/plain")]
        start_response(status, response_headers)
        return [b""]


class SimpleSFTPServer:
    def __init__(self, contents):
        self.server = SFTPServer(content_object=contents)
