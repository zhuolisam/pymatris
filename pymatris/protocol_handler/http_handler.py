from pymatris.utils import (
    get_filepath,
    allocate_tempfile,
    get_http_size,
    cancel_task,
    retry_http,
    generate_range,
)
from pymatris.exceptions import (
    FailedDownload,
    FailedHTTPRequestError,
    MultiPartDownloadError,
)
import pymatris
from pymatris.write_worker import async_write_worker
from .base_handler import ProtocolHandler
import asyncio
import urllib


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

        filepath = writer = tmpfilepath = None
        tasks = []
        try:
            resp, url = await self._get_download_info(config, session, url, **kwargs)
            parse = urllib.parse.urlparse(url)
            filepath = get_filepath(filepath_partial(resp, parse.path), overwrite)
            tmpfilepath = allocate_tempfile(str(filepath))

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
                async_write_worker(downloaded_chunk_queue, file_pb, tmpfilepath)
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

            # Cleanup
            writer.cancel()
            return url, str(filepath), str(tmpfilepath)

        except (Exception, asyncio.CancelledError) as e:
            for task in tasks:
                task.cancel()
            if writer is not None:
                await cancel_task(writer)
                writer = None
            raise FailedDownload(filepath or filepath_partial, url, e) from e
        finally:
            # Cancel idle writer
            if writer is not None:
                writer.cancel()
            pb_callback(file_pb)

    @retry_http
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
            pymatris.log.debug(
                "%s request made to %s with headers=%s",
                resp.request_info.method,
                resp.request_info.url,
                resp.request_info.headers,
            )
            pymatris.log.debug(
                "%s Response received from %s with headers=%s",
                resp.status,
                resp.request_info.url,
                resp.headers,
            )
            if resp.status < 200 or resp.status >= 400:
                raise FailedHTTPRequestError(resp)
            redirectUrl = resp.headers.get("Location", url)
            return resp, redirectUrl

    @retry_http
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
