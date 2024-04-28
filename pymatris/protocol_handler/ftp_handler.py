from .base_handler import ProtocolHandler
import urllib
from pymatris.utils import (
    allocate_tempfile,
    get_filepath,
    get_ftp_size,
    cancel_task,
    retry_ftp,
)
import pymatris
from pymatris.exceptions import FailedDownload
from pymatris.write_worker import async_write_worker
import asyncio
import aioftp


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
        filepath = tmpfilepath = writer = None
        chunksize = chunksize or config.chunksize

        parse = urllib.parse.urlparse(url)

        # Prepare kwargs for retries handler
        kwargs["max_tries"] = max_tries or config.max_tries
        kwargs["url"] = url

        # Prepare files
        filepath = get_filepath(filepath_partial(None, url), overwrite)
        tmpfilepath = allocate_tempfile(str(filepath))

        try:
            await self._connect_and_download(
                parse=parse,
                filepath=filepath,
                tmpfilepath=tmpfilepath,
                token=token,
                file_pb=file_pb,
                chunksize=chunksize,
                writer=writer,
                **kwargs,
            )
            return url, str(filepath), str(tmpfilepath)
        except (Exception, asyncio.CancelledError) as e:
            if writer is not None:
                await cancel_task(writer)
                writer = None
            raise FailedDownload(filepath or filepath_partial, url, e) from e
        finally:
            if writer is not None:
                writer.cancel()
            pb_callback(file_pb)

    @retry_ftp
    async def _connect_and_download(
        self,
        parse,
        filepath,
        tmpfilepath,
        token,
        file_pb,
        chunksize,
        writer,
        **kwargs,
    ):
        async with aioftp.Client.context(
            parse.hostname, parse.port, parse.username, parse.password
        ) as client:
            pymatris.log.debug(
                "Connected to ftp server %s with credentials %s %s",
                parse.hostname,
                parse.username,
                parse.password,
            )

            total_size = await get_ftp_size(client, parse.path)

            if callable(file_pb):
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
            pymatris.log.debug(
                "Downloading ftp file %s from %s", parse.path, parse.hostname
            )
            async with client.download_stream(parse.path) as stream:
                downloaded_chunks_queue = asyncio.Queue()
                download_workers = []
                writer = asyncio.create_task(
                    async_write_worker(downloaded_chunks_queue, file_pb, tmpfilepath)
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
                # Cleanup
                writer.cancel()

    async def _download_worker(self, stream, chunksize, queue):
        offset = 0
        async for chunk in stream.iter_by_block(chunksize):
            # Write this chunk to the output file.
            await queue.put((offset, chunk))
            offset += len(chunk)
