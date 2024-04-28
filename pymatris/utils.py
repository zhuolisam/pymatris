import os
import pathlib
import asyncio
import aiohttp
import aioftp
import asyncssh
from typing import Generator, Tuple, Dict, Union, TypeVar, List
from itertools import count
import warnings
import hashlib
from tqdm import tqdm as tqdm_std
import socket
from concurrent.futures import ThreadPoolExecutor
from .exceptions import FailedHTTPRequestError, MultiPartDownloadError
import pymatris

_T = TypeVar("_T")


class _QueueList(List[_T]):
    def __init__(self):
        pass

    def generate_queue(self, maxsize: int = 0) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        for item in self:
            queue.put_nowait(item)
        self.clear()
        return queue

    def queued_urls(self):
        queue_urls = []
        for item in self:
            queue_urls.append


class Token:
    def __init__(self, n: int) -> None:
        self.n = n

    def __repr__(self) -> str:
        return super().__repr__() + f"n = {self.n}"

    def __str__(self) -> str:
        return f"Token {self.n}"


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
    """Create replacement for a filepath

    Args:
        path (os.PathLike): original filepath

    Returns:
        pathlib.Path: replacement filepath
    """
    path = pathlib.Path(path)

    suffix = "".join(path.suffixes)
    for c in count(start=1):
        if suffix:
            name, _ = path.name.split(suffix)
        else:
            name = path.name
        new_name = f"{name}.{c}{suffix}"
        new_path = path.parent / new_name
        tmp_path = new_path.parent / (new_path.name + ".matris")
        if not new_path.exists() and not tmp_path.exists():
            return new_path


def allocate_tempfile(path: os.PathLike) -> pathlib.Path:
    """Takes in original filepath, allocate the tempfile

    Args:
        path (os.PathLike): tempfile's path

    Returns:
        pathlib.Path: originalfile's path
    """
    path = pathlib.Path(path)

    if not path:
        pymatris.log.warning("No path given, cannot allocate tempfile ")
        return path

    tempfile_suffix = ".matris"
    tempfile = path.parent / (path.name + tempfile_suffix)
    open(str(tempfile), "a").close()  # occupy the tempfile
    return tempfile


def replace_tempfile(path: os.PathLike) -> pathlib.Path:
    """Takes in the tempfile, try to remove the tempfile, then replace

    Args:
        path (os.PathLike): tempfile's path

    Returns:
        pathlib.Path: originalfile's path
    """
    if not path:
        return None

    tempfile_path = pathlib.Path(path)
    originalfile_path = tempfile_path.parent / tempfile_path.stem

    if not tempfile_path.exists():
        return originalfile_path

    if originalfile_path.exists():
        remove_file(str(originalfile_path))

    return replace_file(tempfile_path, originalfile_path)


def remove_file(filepath: os.PathLike) -> None:
    filepath = pathlib.Path(filepath)
    try:
        filepath.unlink(missing_ok=True)
    except Exception as remove_exception:
        warnings.warn(f"Failed to delete {filepath} {remove_exception}")
        pymatris.log.warning(f"Failed to delete {filepath} {remove_exception}")


def replace_file(old_path: pathlib.Path, new_path: pathlib.Path) -> pathlib.Path:
    try:
        old_path.replace(new_path)
        return new_path
    except Exception as rename_exception:
        warnings.warn(f"Failed to replace {old_path} {rename_exception}")
        pymatris.log.warning(f"Failed to replace {old_path} {rename_exception}")


def get_filepath(
    filepath: os.PathLike, overwrite: bool
) -> Tuple[Union[pathlib.Path, str], bool]:
    """
    Get the filepath to download to and ensure dir exists.

    Returns
    -------
    `pathlib.Path`, `pathlib.Path`
    """
    filepath = pathlib.Path(filepath)
    tempfile_path = filepath.parent / (filepath.name + ".matris")  # xxxx.txt.matris
    finalpath = None

    if not filepath.exists() and not tempfile_path.exists():
        finalpath = filepath

    # if tempfile exists, the tempfile already allocated
    elif tempfile_path.exists():
        finalpath = replacement_filename(str(filepath))

    # if file exists but no tempfile allocated, can choose to overwrite
    elif filepath.exists():
        if not overwrite:
            finalpath = replacement_filename(str(filepath))
        elif overwrite:
            finalpath = filepath

    if not finalpath.exists():
        finalpath.parent.mkdir(parents=True, exist_ok=True)

    return finalpath


def get_http_size(resp: aiohttp.ClientResponse) -> Union[int, None]:
    size = resp.headers.get("content-length", None)
    return int(size) if size else size


async def get_ftp_size(
    client: Union["aioftp.Client", asyncssh.SFTPClient], filepath: os.PathLike
) -> int:
    try:
        attr = await client.stat(filepath)
        if isinstance(client, aioftp.Client):
            size = attr.get("size", None)
        else:
            size = attr.size
    except Exception as e:
        raise e
    return int(size) if size else size


async def cancel_task(task: asyncio.Task) -> bool:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return True
    return task.cancelled()


def sha256sum(filename: str) -> str:
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def retry_http(coro_func):
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
            except asyncio.TimeoutError:
                # From https://github.com/cshuaimin/aiodl
                # Usually server has a fixed TCP timeout to clean dead
                # connections, might have a lot of timeouts appear
                # So retry it without checking the max retries.
                tqdm_std.write("%s() timeout, retry in 1 second" % coro_func.__name__)
                await asyncio.sleep(1)
            except (
                MultiPartDownloadError,
                FailedHTTPRequestError,
                aiohttp.ClientError,
                socket.gaierror,
            ) as exc:
                if tried < max_tries:
                    # Exponential backoff
                    sec = tried / 2
                    message = "(%s) failed: retry in %.1f seconds (%d/%d)" % (
                        cur_url,
                        sec,
                        tried,
                        max_tries,
                    )
                    tqdm_std.write(message)
                    pymatris.log.debug(message)
                    await asyncio.sleep(sec)
                else:
                    message = "(%s) failed after %d tries: " % (
                        cur_url,
                        max_tries,
                    )
                    tqdm_std.write(message)
                    pymatris.log.debug(message)
                    if isinstance(
                        exec, (MultiPartDownloadError, FailedHTTPRequestError)
                    ):
                        exec.retry = tried
                        exec.max_retries = max_tries
                    raise exc

    return wrapper


def retry_ftp(coro_func):
    async def wrapper(self, *args, **kwargs):
        max_tries = kwargs.pop(
            "max_tries"
        )  # Have to use this workaround to get the max_tries without using parameterized decorator
        cur_url = kwargs.pop("url")  # Get URL
        tried = 0
        while True:
            tried += 1
            try:
                return await coro_func(self, *args, **kwargs)
            except asyncio.TimeoutError:
                tqdm_std.write("%s() timeout, retry in 1 second" % coro_func.__name__)
                await asyncio.sleep(1)
            except (
                asyncssh.SFTPError,
                aioftp.AIOFTPException,
                socket.gaierror,
                Exception,
            ) as exc:
                if tried < max_tries:
                    # Exponential backoff
                    sec = tried / 2
                    message = "(%s) failed: retry in %.1f seconds (%d/%d)" % (
                        cur_url,
                        sec,
                        tried,
                        max_tries,
                    )
                    tqdm_std.write(message)
                    pymatris.log.debug(message)
                    await asyncio.sleep(sec)
                else:
                    message = "(%s) failed after %d tries: " % (
                        cur_url,
                        max_tries,
                    )
                    tqdm_std.write(message)
                    pymatris.log.debug(message)
                    raise exc

    return wrapper


def run_task_in_thread(loop: asyncio.BaseEventLoop, coro: asyncio.Task):
    with ThreadPoolExecutor(max_workers=1) as aio_pool:
        try:
            future = aio_pool.submit(loop.run_until_complete, coro)
        except KeyboardInterrupt:
            future.cancel()
    return future.result()


def generate_range(content_length, max_splits):
    if not max_splits or max_splits < 1:
        max_splits = 1
        tqdm_std.write("Max Splits cannot be smaller than 1")

    split_length = max(1, content_length // max_splits)
    ranges = [
        [start, start + split_length]
        for start in range(0, content_length, split_length)
    ]
    ranges[-1][1] = ""

    return ranges
