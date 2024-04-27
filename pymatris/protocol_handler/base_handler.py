from abc import ABC, abstractmethod
from pymatris.config import DownloaderConfig
from typing import Optional, Callable
import aiohttp
import asyncio
from typing import List


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


class ProtocolResolver:
    _protocols = {}

    @classmethod
    def register_protocol(cls, protocols: List[str]):
        def wrapper(handler_class):
            for protocol in protocols:
                cls._protocols[protocol] = handler_class
            return handler_class

        return wrapper

    @classmethod
    def get_handler(cls, scheme: str) -> ProtocolHandler:
        if scheme in cls._protocols:
            return cls._protocols[scheme]()
        else:
            raise ValueError(f"No handler available for scheme: {scheme}")

    @classmethod
    def supported_protocols(cls):
        return list(cls._protocols.keys())
