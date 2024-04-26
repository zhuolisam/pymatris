from pymatris.utils import (
    FailedDownload,
    FailedHTTPRequestError,
    MultiPartDownloadError,
    get_filepath,
    get_http_size,
    get_ftp_size,
    cancel_task,
    remove_file,
    retry,
    retry_sftp,
    generate_range,
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
        elif scheme == "sftp":
            return SFTPHandler()
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
            parse = urllib.parse.urlparse(url)
            filepath = get_filepath(filepath_partial(resp, parse.path), overwrite)

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
                ranges = generate_range(
                    content_length=content_length, max_splits=max_splits
                )
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

            # Wrap the leaf exception in a FailedDownload exception, all handlers will raise this should anything go wrong. So it will be like FailedDownload(filepath_partial, url, FailedDownload(None, url, response)) or FailedDownload(filepath_partial, url)
            # It can also be FailedDownload(filepath_partial, url, aiohttp.ClientError)
            raise FailedDownload(filepath_partial, url, e) from e
        finally:
            # Cancel idle writer
            if writer is not None:
                writer.cancel()
            pb_callback(file_pb)

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
                raise FailedHTTPRequestError(resp)
            redirectUrl = resp.headers.get("Location", url)
            return resp, redirectUrl

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
                raise MultiPartDownloadError(resp)

            while True:
                chunk = await resp.content.read(chunksize)
                if not chunk:
                    break
                await queue.put((offset, chunk))
                offset += len(chunk)


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
        chunksize=None,
        max_splits=None,
        max_tries=None,
        pb_callback=None,
        **kwargs,
    ):
        filepath = writer = None
        chunksize = chunksize or config.chunksize
        parse = urllib.parse.urlparse(url)
        try:
            async with aioftp.Client.context(parse.hostname, parse.port) as client:
                if parse.username and parse.password:
                    await client.login(user=parse.username, password=parse.password)

                filepath = get_filepath(filepath_partial(None, url), overwrite)

                total_size = await get_ftp_size(client, parse.path)

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
                            self._download_worker(
                                stream, chunksize, downloaded_chunks_queue
                            )
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

    async def _download_worker(self, stream, chunksize, queue):
        offset = 0
        async for chunk in stream.iter_by_block(chunksize):
            # Write this chunk to the output file.
            await queue.put((offset, chunk))
            offset += len(chunk)


class SFTPHandler(ProtocolHandler):
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
        filepath = writer = conn = sftp_client = file = None
        chunksize = chunksize or config.chunksize
        max_splits = max_splits or config.max_splits
        kwargs["max_tries"] = max_tries or config.max_tries
        kwargs["url"] = url

        parse = urllib.parse.urlparse(url)
        try:
            conn, sftp_client = await self._connect_host(parse, **kwargs)
            # async with asyncssh.connect(
            #     parse.hostname,
            #     username=parse.username,
            #     password=parse.password,
            #     port=parse.port or 22,
            #     known_hosts=None,
            #     **kwargs,
            # ) as conn:
            #     async with conn.start_sftp_client() as sftp:
            filepath = get_filepath(filepath_partial(None, url), overwrite)

            # Can throw FileNotFoundError
            total_size = await get_ftp_size(sftp_client, parse.path)

            if callable(file_pb) and total_size:
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

            # Generate tasks to read into queue
            ranges = generate_range(content_length=total_size, max_splits=max_splits)
            file = await sftp_client.open(parse.path, "rb")
            downloaded_chunks_queue = asyncio.Queue()
            writer = asyncio.create_task(
                async_write_worker(downloaded_chunks_queue, file_pb, filepath)
            )
            tasks = []
            for _range in ranges:
                tasks.append(
                    asyncio.create_task(
                        self._download_worker(
                            file, _range[0], chunksize, downloaded_chunks_queue
                        )
                    )
                )

            await asyncio.gather(*tasks)
            await downloaded_chunks_queue.join()  # Ensure all chunks are written

            await file.close()
            sftp_client.exit()
            conn.close()
            return url, str(filepath)

        except (Exception, asyncio.CancelledError) as e:
            if writer:
                await cancel_task(writer)
                writer = None
            if filepath:
                remove_file(filepath)
                filepath = None
            if file is not None:
                await file.close()
            if sftp_client is not None:
                sftp_client.exit()
            if conn is not None:
                conn.close()
            raise FailedDownload(filepath_partial, url, e) from e
        finally:
            if writer:
                writer.cancel()
            pb_callback(file_pb)

    async def _download_worker(self, file, offset, chunksize, queue):
        while True:
            await file.seek(offset)
            chunk = await file.read(chunksize)
            if not chunk:
                break
            await queue.put((offset, chunk))
            offset += len(chunk)

    @retry_sftp
    async def _connect_host(self, parse, **kwargs):
        conn = await asyncssh.connect(
            parse.hostname,
            username=parse.username,
            password=parse.password,
            port=parse.port or 22,
            known_hosts=None,
            # **kwargs,
        )
        sftp_client = await conn.start_sftp_client()
        return conn, sftp_client
