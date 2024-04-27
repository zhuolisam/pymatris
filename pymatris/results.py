from collections import UserList, namedtuple
from .exceptions import FailedDownload, MultiPartDownloadError
import aiohttp

__all__ = ["Results"]


class Error(namedtuple("error", ("filepath_partial", "url", "exception"))):
    def __str__(self):
        filepath_partial = ""
        if isinstance(self.filepath_partial, str):
            filepath_partial = f"{self.filepath_partial},\n"
        return filepath_partial + f"{self.url},\n{repr(self.exception)}"

    def __repr__(self):
        return self.__str__()


class Success(namedtuple("success", ("path", "url"))):
    def __str__(self) -> str:
        return f"{self.path} downloaded from {self.url}\n"

    def __repr__(self):
        return f"{self.path} {self.url}\n"


class Results(UserList):
    def __init__(self, *args, errors=None, urls=None, success=None):
        super().__init__(*args)
        self._errors = errors or list()
        self._urls = urls or list()
        self._success = success or list()

    def _get_nice_resp_repr(self, response):
        # This is a modified version of aiohttp.ClientResponse.__repr__
        if isinstance(response, aiohttp.ClientResponse):
            ascii_encodable_url = str(response.url)
            if response.reason:
                ascii_encodable_reason = response.reason.encode(
                    "ascii", "backslashreplace"
                ).decode("ascii")
            else:
                ascii_encodable_reason = response.reason
            return f"<ClientResponse({ascii_encodable_url}) [{response.status} {ascii_encodable_reason}]>"
        else:
            return repr(response)

    def __str__(self):
        out = ""

        if self.success:
            out = "\nSuccess:\n"
            for suc in self.success:
                out += f"{repr(suc)}"

        if self.errors:
            out += "\nErrors:\n"
            for error in self.errors:
                if isinstance(error.exception, aiohttp.ClientResponse):
                    resp = self._get_nice_resp_repr(error.exception)
                    out += f"(url={error.url}, response={resp})\n"
                else:
                    out += f"({repr(error)})"
        return out

    def __repr__(self):
        return str(self)

    def append(self, path, url):
        super().append(path)
        self._urls.append(url)
        self._success.append(Success(path, url))

    def add_error(self, filename, url, exception):
        self._errors.append(Error(filename, url, exception))

    @property
    def errors(self):
        return self._errors

    @property
    def success(self):
        return self._success

    @property
    def urls(self):
        return self._urls
