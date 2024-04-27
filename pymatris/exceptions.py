import pathlib
import aiohttp


class FailedDownload(Exception):
    def __init__(
        self, filepath_partial: pathlib.Path, url: str, exception: Exception
    ) -> None:
        self.filepath_partial = filepath_partial
        self.url = url
        self.exception = exception
        super().__init__()

    def __repr__(self) -> str:
        out = super().__repr__()
        out += f"\n {self.url} {self.exception}"
        return out

    def __str__(self) -> str:
        return f"Download Failed: {self.url} with error {str(self.exception)}"


class MultiPartDownloadError(Exception):
    def __init__(
        self,
        response: aiohttp.ClientResponse,
        retry_count: int = None,
        max_retries: int = None,
    ):
        self.response = response
        self.retry_count = retry_count
        self.max_retries = max_retries
        super().__init__(str(response))

    def __str__(self) -> str:
        if self.retry_count and self.max_retries:
            return "{} with network response {} on ({}/{}) retry \n".format(
                self.__class__,
                str(self.response),
                str(self.retry_count),
                str(self.max_retries),
            )
        else:
            return "{} with network response {} on (NIL) retry retry \n".format(
                self.__class__,
                str(self.response),
            )


class FailedHTTPRequestError(Exception):
    def __init__(
        self,
        response: aiohttp.ClientResponse = None,
        retry_count: int = None,
        max_retries: int = None,
    ):
        self.response = response
        self.retry_count = retry_count
        self.max_retries = max_retries
        super().__init__(str(response))

    def __str__(self) -> str:
        if self.retry_count and self.max_retries:
            return "{} with network response {} on ({}/{}) retry \n".format(
                self.__class__,
                str(self.response),
                str(self.retry_count),
                str(self.max_retries),
            )
        else:
            return "{} with network response {} on (NIL) retry retry \n".format(
                self.__class__,
                str(self.response),
            )
