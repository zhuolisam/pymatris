from pymatris.utils import (
    FailedDownload,
    MultiPartDownloadError,
    get_filepath,
    get_http_size,
    get_ftp_size,
    cancel_task,
    remove_file,
    retry,
)
import asyncio
from pymatris.write_worker import async_write_worker
from abc import ABC, abstractmethod
from typing import Callable, Optional
import aiohttp
import urllib
import aioftp
import asyncssh
from pymatris.config import DownloaderConfig


class ProtocolHandler(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    async def run_download(
        self,
        config: DownloaderConfig,
        session: Optional[aiohttp.ClientSession],
        url: str,
        filepath_partial: Callable[[aiohttp.ClientResponse, str], str],
        overwrite: bool,
        token: asyncio.Event,
        file_pb: Optional[callable] = None,
        chunksize: Optional[int] = None,
        max_splits: Optional[int] = None,
        max_tries: Optional[int] = None,
        pb_callback: Optional[Callable] = None,
        **kwargs,
    ):
        raise NotImplementedError("run_download() must be implemented")

    @classmethod
    def get_handler(cls, scheme: str) -> "ProtocolHandler":
        if scheme in ["http", "https"]:
            return HTTPHandler()
        elif scheme == "ftp":
            return FTPHandler()
        else:
            raise ValueError(f"No handler available for scheme: {scheme}")

    @classmethod
    def supported_protocols(self):
        return ["http", "https", "ftp", "sftp"]


class HTTPHandler(ProtocolHandler):
    async def run_download(
        self,
        config,
        session,
        url,
        filepath_partial,
        overwrite,
        token,
        file_pb=None,
        chunksize=None,
        max_splits=None,
        max_tries=None,
        pb_callback=None,
        **kwargs,
    ):
        if chunksize is None:
            chunksize = config.chunksize
        if max_splits is None:
            max_splits = config.max_splits
        kwargs["max_tries"] = max_tries if max_tries else config.max_tries

        filepath = writer = None
        tasks = []
        try:
            resp, url = await self._get_download_info(config, session, url, **kwargs)
            filepath = get_filepath(filepath_partial(resp, url), overwrite)

            if callable(file_pb):
                file_pb = file_pb(
                    position=token.n,
                    unit="B",
                    unit_scale=True,
                    desc=filepath.name,
                    leave=False,
                    total=get_http_size(resp),
                )
            else:
                file_pb = None

            downloaded_chunk_queue = asyncio.Queue()

            writer = asyncio.create_task(
                async_write_worker(downloaded_chunk_queue, file_pb, filepath)
            )

            if (
                max_splits
                and resp.headers.get("Accept-Ranges", None) == "bytes"
                and "Content-length" in resp.headers
            ):
                content_length = int(resp.headers["Content-length"])
                split_length = max(1, content_length // max_splits)
                ranges = [
                    [start, start + split_length]
                    for start in range(0, content_length, split_length)
                ]
                ranges[-1][1] = ""
                for _range in ranges:
                    tasks.append(
                        asyncio.create_task(
                            self._download_worker(
                                config,
                                session,
                                url,
                                chunksize,
                                _range,
                                downloaded_chunk_queue,
                                **kwargs,
                            )
                        )
                    )
            else:
                tasks.append(
                    asyncio.create_task(
                        self._download_worker(
                            config,
                            session,
                            url,
                            chunksize,
                            None,
                            downloaded_chunk_queue,
                            **kwargs,
                        )
                    )
                )

            await asyncio.gather(*tasks)
            await downloaded_chunk_queue.join()
            return url, str(filepath)

        except (Exception, asyncio.CancelledError) as e:
            for task in tasks:
                task.cancel()
            if writer is not None:
                await cancel_task(writer)
                writer = None
            if filepath is not None:
                remove_file(filepath)

            # Wrap the leaf exception in a FailedDownload exception, all handlers will raise this should anything go wrong. So it will be like FailedDownload(filepath_partial, url, FailedDownload(None, url, response)) or FailedDownload(filepath_partial, url, MultipartError(None, url, response))
            # It can also be FailedDownload(filepath_partial, url, aiohttp.ClientError)
            raise FailedDownload(filepath_partial, url, e)
        finally:
            if writer is not None:
                writer.cancel()
            pb_callback(file_pb)

    @retry
    async def _download_worker(
        self, config, session, url, chunksize, http_range, queue, **kwargs
    ):
        additionl_headers = kwargs.pop("headers", {})
        headers = {**config.headers, **additionl_headers}
        if http_range:
            headers["Range"] = "bytes={}-{}".format(*http_range)
            # make the offset from file
            offset, _ = http_range
        else:
            offset = 0

        async with session.get(url, timeout=config.timeouts, headers=headers) as resp:
            if resp.status < 200 or resp.status >= 300:
                raise FailedDownload(None, url, MultiPartDownloadError(resp))

            while True:
                chunk = await resp.content.read(chunksize)
                if not chunk:
                    break
                await queue.put((offset, chunk))
                offset += len(chunk)

    @retry
    async def _get_download_info(self, config, session, url, **kwargs):
        additional_headers = kwargs.pop("headers", {})
        headers = {**config.headers, **additional_headers}
        # Might get no response at all, which is likely a client error, including ssl, proxy, auth, etc.
        # But they are handled in retry decorator.
        async with session.head(
            url,
            timeout=config.timeouts,
            headers=headers,
            allow_redirects=True,
            **kwargs,
        ) as resp:
            if resp.status < 200 or resp.status >= 400:
                raise FailedDownload(None, url, resp)
            redirectUrl = resp.headers.get("Location", url)
            return resp, redirectUrl


class FTPHandler(ProtocolHandler):
    async def run_download(
        self,
        config,
        session,
        url,
        filepath_partial,
        overwrite,
        token,
        file_pb=None,
        pb_callback=None,
        **kwargs,
    ):
        pass
        filepath = writer = None
        parse = urllib.parse.urlparse(url)
        print("What is the url??", url)
        print(parse)
        try:
            async with aioftp.Client.context(parse.hostname, **kwargs) as client:
                await client.login(parse.username, parse.password)

                # Get the filepath
                filepath = get_filepath(filepath_partial(None, url), overwrite)

                if callable(file_pb):
                    total_size = await get_ftp_size(client, parse.path)
                    file_pb = file_pb(
                        position=token.n,
                        unit="B",
                        unit_scale=True,
                        desc=filepath.name,
                        leave=False,
                        total=total_size,
                    )
                else:
                    file_pb = None

                async with client.download_stream(parse.path) as stream:
                    downloaded_chunks_queue = asyncio.Queue()
                    download_workers = []
                    writer = asyncio.create_task(
                        async_write_worker(downloaded_chunks_queue, file_pb, filepath)
                    )

                    download_workers.append(
                        asyncio.create_task(
                            self._download_worker(stream, downloaded_chunks_queue)
                        )
                    )

                    await asyncio.gather(*download_workers)
                    await downloaded_chunks_queue.join()

                    return url, str(filepath)

        except (Exception, asyncio.CancelledError) as e:
            if writer is not None:
                await cancel_task(writer)
                writer = None
            # If filepath is None then the exception occurred before the request
            # computed the filepath, so we have no file to cleanup
            if filepath is not None:
                remove_file(filepath)
                filepath = None

            raise FailedDownload(filepath_partial, url, e) from e

        finally:
            if writer is not None:
                writer.cancel()
            pb_callback(file_pb)

    async def _download_worker(self, stream, queue):
        offset = 0
        async for chunk in stream.iter_by_block():
            # Write this chunk to the output file.
            await queue.put((offset, chunk))
            offset += len(chunk)


class SFTPHandler(ProtocolHandler):
    async def run_download(
        self,
        config,
        *,
        url,
        filepath_partial,
        overwrite,
        token,
        chunksize=None,
        file_pb=None,
        pb_callback=None,
        **kwargs,
    ):
        filepath = writer = None
        chunksize = chunksize or config.chunksize
        parse = urllib.parse.urlparse(url)
        try:
            async with asyncssh.connect(
                parse.hostname,
                username=parse.username,
                password=parse.password,
                **kwargs,
            ) as conn:
                async with conn.start_sftp_client() as sftp:
                    # Ensure we get the correct path and handle overwriting
                    filepath = get_filepath(filepath_partial(None, url), overwrite)

                    # Optionally handle progress bar setup
                    if callable(file_pb):
                        file_attrs = await sftp.stat(parse.path)
                        total_size = file_attrs.size
                        file_pb = file_pb(
                            position=token.n,
                            unit="B",
                            unit_scale=True,
                            desc=filepath.name,
                            leave=False,
                            total=total_size,
                        )
                    else:
                        file_pb = None

                    # Setting up the writer and the queue
                    downloaded_chunks_queue = asyncio.Queue()
                    writer = asyncio.create_task(
                        async_write_worker(downloaded_chunks_queue, file_pb, filepath)
                    )

                    # Read the file in chunks
                    async with sftp.open(parse.path, "rb") as file:
                        while True:
                            chunk = await file.read(chunksize or 4096)
                            if not chunk:
                                break
                            await downloaded_chunks_queue.put(chunk)

                    await (
                        downloaded_chunks_queue.join()
                    )  # Ensure all chunks are written

                    for callback in self.config.done_callbacks:
                        callback(filepath, url, None)

                    return url, str(filepath)

        except (Exception, asyncio.CancelledError) as e:
            if writer is not None:
                await cancel_task(writer)
                writer = None
            if filepath is not None:
                remove_file(filepath)
                filepath = None

            raise FailedDownload(filepath_partial, url, e)

        finally:
            if writer is not None:
                writer.cancel()
            pb_callback()
