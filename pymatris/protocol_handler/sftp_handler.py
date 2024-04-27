import pymatris
from .base_handler import ProtocolHandler
import urllib
from pymatris.utils import (
    allocate_tempfile,
    replace_tempfile,
    get_filepath,
    get_ftp_size,
    generate_range,
    retry_sftp,
    remove_file,
    cancel_task,
)
from pymatris.write_worker import async_write_worker
from pymatris.exceptions import FailedDownload
import asyncio
import asyncssh


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
        filepath = tmpfilepath = writer = conn = sftp_client = file = None
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
            tmpfilepath = allocate_tempfile(str(filepath))
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
                async_write_worker(downloaded_chunks_queue, file_pb, tmpfilepath)
            )
            tasks = []
            pymatris.log.debug(
                "Downloading sftp file %s from %s", parse.path, parse.hostname
            )
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
            return url, str(filepath), str(tmpfilepath)

        except (Exception, asyncio.CancelledError) as e:
            if writer:
                await cancel_task(writer)
                writer = None
                # if tmpfilepath is not None:
                #     remove_file(tmpfilepath)
                filepath = None
            if file is not None:
                await file.close()
            if sftp_client is not None:
                sftp_client.exit()
            if conn is not None:
                conn.close()
            raise FailedDownload(filepath or filepath_partial, url, e) from e
        finally:
            if writer:
                writer.cancel()

            # replace_tempfile(str(tmpfilepath))
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
        pymatris.log.debug("Connected to sftp server %s", parse.hostname)

        sftp_client = await conn.start_sftp_client()
        return conn, sftp_client
