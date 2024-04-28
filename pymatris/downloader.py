from functools import partial
from typing import Callable, Optional, Union
import contextlib
import asyncio
from pymatris.config import SessionConfig, DownloaderConfig
from pymatris.protocol_handler import ProtocolResolver
from pymatris.exceptions import FailedDownload
from .utils import run_task_in_thread
from .results import Results
import pathlib
import os
import aiohttp
from pymatris.utils import (
    default_name,
    replace_tempfile,
    remove_file,
    _QueueList,
    Token,
)
from tqdm import tqdm as tqdm_std
import signal
import urllib
import pymatris
import logging
import threading
import warnings


class Downloader:
    def __init__(
        self,
        max_parallel: int = 5,
        max_splits: int = 5,
        all_progress: bool = True,
        overwrite: bool = True,
        session_config: Optional[SessionConfig] = None,
    ):
        self.config = DownloaderConfig(
            max_parallel=max_parallel,
            max_splits=max_splits,
            all_progress=all_progress,
            overwrite=overwrite,
            config=session_config,
        )
        self.download_queue = _QueueList()  # Queue that will hold all download task
        self._configure_logging()  # Configure logging
        self.tqdm = tqdm_std  # Configure progress bar writer

    def enqueue_file(
        self,
        url: str,
        path: Optional[Union[str, os.PathLike]] = None,
        filename: Optional[Union[str, os.PathLike]] = None,
        overwrite: Optional[bool] = None,
        **kwargs,
    ):
        # Build filepath function
        # if not path and not filename:
        #     raise ValueError("Either path or filename must be specified.")
        if not path:
            path = "./"

        path = pathlib.Path(path)
        filepath: Callable[[str, Optional[aiohttp.ClientResponse]], os.PathLike]
        if not filename:
            # If filename is not provided, then we use default name
            filepath = partial(default_name, path)
        else:
            # If filename is provided, then use it, make it a callback
            def filepath(url, resp):
                return path / filename

        overwrite = overwrite or self.config.overwrite

        # Restrict unsupported protocols
        scheme = urllib.parse.urlparse(url).scheme
        if scheme not in ProtocolResolver.supported_protocols():
            raise ValueError(
                f"URL must start with either:  {ProtocolResolver.supported_protocols()}"
            )

        self.download_queue.append((url, filepath, overwrite, kwargs))

    @property
    def queued_downloads(self):
        return len(self.download_queue)

    def _generate_tokens(self):
        queue = asyncio.Queue(maxsize=self.config.max_parallel)
        for i in range(self.config.max_parallel):
            queue.put_nowait(Token(i + 1))
        return queue

    def _format_results_and_remove_tempfile(
        self, dl_results: Results, main_pb: Optional[tqdm_std]
    ):
        errors = sum([isinstance(i, FailedDownload) for i in dl_results])
        if errors:
            total_files = len(dl_results)
            message = f"{errors}/{total_files} files failed to download."
            if main_pb:
                main_pb.write(message)
            else:
                pymatris.log.info(message)

        results = Results()
        for res in dl_results:
            if isinstance(res, FailedDownload):
                remove_file(str(res.filepath_partial) + ".matris")
                results.add_error(res.filepath_partial, res.url, res.exception)
                pymatris.log.info(
                    "%s failed to download with exception\n" "%s",
                    res.url,
                    res.exception,
                )
            elif isinstance(res, Exception):
                raise res
            else:
                requested_url, filepath, tempfilepath = res
                replace_tempfile(str(tempfilepath))
                results.append(path=filepath, url=requested_url)

        return results

    async def run_download(self) -> Results:
        futures = []
        tokens = self._generate_tokens()
        total_files = self.queued_downloads
        dl_queue = self.download_queue.generate_queue()
        results = ret_results = None

        with self._get_main_pb(total_files) as main_pb:
            async with self.config.aiohttp_client_session() as session:
                try:
                    while not dl_queue.empty():
                        (
                            url,
                            filepath_partial,
                            overwrite,
                            kwargs,
                        ) = await dl_queue.get()

                        scheme = url.split("://")[0]
                        handler = ProtocolResolver.get_handler(scheme)

                        file_pb = self.tqdm if self.config.file_progress else False
                        token = await tokens.get()

                        def close_pb_callback(pb):
                            if isinstance(pb, self.tqdm):
                                pb.close()

                        future = asyncio.create_task(
                            handler.run_download(
                                self.config,  # pass configuration
                                session,  # pass session
                                url,  # user defined
                                filepath_partial,  # user defined
                                overwrite,  # user defined
                                token=token,  # injected
                                file_pb=file_pb,  # injected
                                pb_callback=close_pb_callback,  # injected
                                **kwargs,  # user defined, include headers, etc
                            )
                        )

                        def callback(token, future, main_pb):
                            try:
                                tokens.put_nowait(token)
                                if main_pb and not future.exception():
                                    main_pb.update(1)
                            except asyncio.CancelledError:
                                return

                        future.add_done_callback(
                            partial(callback, token, main_pb=main_pb)
                        )
                        futures.append(future)

                    results = await asyncio.gather(*futures, return_exceptions=True)
                except asyncio.CancelledError:
                    for task in futures:
                        task.cancel()
                    results = await asyncio.gather(*futures, return_exceptions=True)
                finally:
                    ret_results = self._format_results_and_remove_tempfile(
                        results, main_pb
                    )
        return ret_results

    def download(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        should_run_in_thread = loop and loop.is_running()

        if should_run_in_thread or loop is None:
            loop = asyncio.new_event_loop()

        task = loop.create_task(self.run_download())

        # Add signal handlers to the loop
        self._add_signal_handler(loop, task)

        if should_run_in_thread:
            return run_task_in_thread(loop, task)

        return loop.run_until_complete(task)

    @staticmethod
    def _add_signal_handler(loop, task):
        if os.name == "nt":
            return

        if threading.current_thread() != threading.main_thread():
            warnings.warn(
                "Pymatris running in different thread, unable to cancel the download",
                UserWarning,
            )
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)

    def _get_main_pb(self, total: int):
        if self.config.all_progress:
            return self.tqdm(
                total=total,
                unit="file",
                desc="Files Downloaded",
                position=0,
            )
        else:
            # If self.config.all_progress is False, then return a dummy context manager
            @contextlib.contextmanager
            def dummy_context_manager():
                yield None

            return dummy_context_manager()

    def _configure_logging(self):
        if self.config.log_level is None:
            return

        sh = logging.StreamHandler()
        sh.setLevel(self.config.log_level)

        formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
        sh.setFormatter(formatter)

        pymatris.log.addHandler(sh)
        pymatris.log.setLevel(self.config.log_level)

        aiohttp_logger = logging.getLogger("aiohttp.client")
        aioftp_logger = logging.getLogger("aioftp.client")
        asyncssh_logger = logging.getLogger("asyncssh")

        aioftp_logger.addHandler(sh)
        aioftp_logger.setLevel(self.config.log_level)

        aiohttp_logger.addHandler(sh)
        aiohttp_logger.setLevel(self.config.log_level)

        asyncssh_logger.addHandler(sh)
        asyncssh_logger.setLevel(self.config.log_level)
        pymatris.log.debug("pymatris configured to debug level logging...")
