import os
import pathlib
import aiohttp
import aioftp
from typing import Generator, Tuple, Dict, Union
from itertools import count
import asyncio
import warnings
import hashlib
from tqdm import tqdm as tqdm_std
import socket
from concurrent.futures import ThreadPoolExecutor


def default_name(
    path: os.PathLike, resp: aiohttp.ClientResponse, url: str
) -> os.PathLike:
    url_filename = url.split("/")[-1]
    if resp:
        cdheader = resp.headers.get("Content-Disposition", None)
        if cdheader:
            value, params = parse_header(cdheader)
            name = params.get("filename", url_filename)
        else:
            name = url_filename
    else:
        name = url_filename
    return pathlib.Path(path) / name


def parse_header(line: str) -> Tuple[str, Dict[str, str]]:
    parts = _parseparam(";" + line)
    key = parts.__next__()
    pdict = {}
    for p in parts:
        i = p.find("=")
        if i >= 0:
            name = p[:i].strip().lower()
            value = p[i + 1 :].strip()
            if len(value) >= 2 and value[0] == value[-1] == '"':
                value = value[1:-1]
                value = value.replace("\\\\", "\\").replace('\\"', '"')
            pdict[name] = value
    return key, pdict


# Copied out of CPython under PSF Licence 2
def _parseparam(s: str) -> Generator[str, None, None]:
    while s[:1] == ";":
        s = s[1:]
        end = s.find(";")
        while end > 0 and (s.count('"', 0, end) - s.count('\\"', 0, end)) % 2:
            end = s.find(";", end + 1)
        if end < 0:
            end = len(s)
        f = s[:end]
        yield f.strip()
        s = s[end:]


def replacement_filename(path: os.PathLike) -> pathlib.Path:  # type: ignore[return]
    path = pathlib.Path(path)

    if not path.exists():
        return path

    suffix = "".join(path.suffixes)
    for c in count(start=1):
        if suffix:
            name, _ = path.name.split(suffix)
        else:
            name = path.name
        new_name = f"{name}.{c}{suffix}"
        new_path = path.parent / new_name
        if not new_path.exists():
            return new_path


def get_filepath(
    filepath: os.PathLike, overwrite: bool
) -> Tuple[Union[pathlib.Path, str], bool]:
    """
    Get the filepath to download to and ensure dir exists.

    Returns
    -------
    `pathlib.Path`, `bool`
    """
    filepath = pathlib.Path(filepath)
    if filepath.exists():
        if not overwrite:  # if not overwrite, then create a replacement filename
            filepath = replacement_filename(filepath)

    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True)

    return filepath


def get_http_size(resp: aiohttp.ClientResponse) -> Union[int, None]:
    size = resp.headers.get("content-length", None)
    return int(size) if size else size


async def get_ftp_size(client: "aioftp.Client", filepath: os.PathLike) -> int:
    try:
        size = await client.stat(filepath)
        size = size.get("size", None)
    except Exception:
        tqdm_std.write("Failed to get size of FTP file")
        size = None
    return int(size) if size else size


class FailedDownload(Exception):
    def __init__(
        self, filepath_partial: pathlib.Path, url: str, exception: BaseException
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
    def __init__(self, response: aiohttp.ClientResponse) -> None:
        self.response = response


async def cancel_task(task: asyncio.Task) -> bool:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return True
    return task.cancelled()


def remove_file(filepath: os.PathLike) -> None:
    filepath = pathlib.Path(filepath)
    try:
        filepath.unlink(missing_ok=True)
    except Exception as remove_exception:
        warnings.warn(
            f"Failed to delete possibly incomplete file {filepath} {remove_exception}"
        )


def sha256sum(filename: str) -> str:
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


async def retry2(max_tries: int = 5):
    def retry_max(coro_func):
        async def wrapper(self, *args, **kwargs):
            tried = 0
            while True:
                tried += 1
                try:
                    return await coro_func(self, *args, **kwargs)
                except (FailedDownload, aiohttp.ClientError, socket.gaierror) as exc:
                    if tried <= max_tries:
                        sec = tried / 2
                        tqdm_std.write(
                            "%s() failed: retry in %.1f seconds (%d/%d)"
                            % (coro_func.__name__, sec, tried, max_tries)
                        )
                        await asyncio.sleep(sec)
                    else:
                        tqdm_std.write(
                            "%s() failed after %d tries: "
                            % (coro_func.__name__, max_tries)
                        )
                        raise exc
                except asyncio.TimeoutError:
                    # Usually server has a fixed TCP timeout to clean dead
                    # connections, so you can see a lot of timeouts appear
                    # at the same time. I don't think this is an error,
                    # So retry it without checking the max retries.
                    tqdm_std.write(
                        "%s() timeout, retry in 1 second" % coro_func.__name__
                    )
                    await asyncio.sleep(1)

        return wrapper

    return retry_max


def retry(coro_func):
    async def wrapper(self, *args, **kwargs):
        max_tries = kwargs.pop(
            "max_tries"
        )  # Have to use this workaround to get the max_tries without using parameterized decorator
        cur_url = args[2]  # Get URL
        tried = 0
        while True:
            tried += 1
            try:
                return await coro_func(self, *args, **kwargs)
            except (FailedDownload, aiohttp.ClientError, socket.gaierror) as exc:
                if tried < max_tries:
                    # Exponential backoff
                    sec = tried / 2
                    tqdm_std.write(
                        "%s(%s) failed: retry in %.1f seconds (%d/%d)"
                        % (coro_func.__name__, cur_url, sec, tried, max_tries)
                    )
                    await asyncio.sleep(sec)
                else:
                    tqdm_std.write(
                        "%s(%s) failed after %d tries: "
                        % (coro_func.__name__, cur_url, max_tries)
                    )
                    raise exc
            except asyncio.TimeoutError:
                # Usually server has a fixed TCP timeout to clean dead
                # connections, so you can see a lot of timeouts appear
                # at the same time. I don't think this is an error,
                # So retry it without checking the max retries.
                tqdm_std.write("%s() timeout, retry in 1 second" % coro_func.__name__)
                await asyncio.sleep(1)

    return wrapper


def run_task_in_thread(loop: asyncio.BaseEventLoop, coro: asyncio.Task):
    """
    This function returns the asyncio Future after running the loop in a
    thread.

    This makes the return value of this function the same as the return
    of ``loop.run_until_complete``.
    """
    with ThreadPoolExecutor(max_workers=1) as aio_pool:
        try:
            future = aio_pool.submit(loop.run_until_complete, coro)
        except KeyboardInterrupt:
            future.cancel()
    return future.result()
