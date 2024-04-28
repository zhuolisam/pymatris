import platform
from typing import Dict, Optional
import os


from dataclasses import field, dataclass

import aiohttp

__all__ = ["DownloaderConfig", "SessionConfig"]


def _default_headers():
    return {
        "User-Agent": f"Pymatris Agent"
        f" aiohttp/{aiohttp.__version__}"
        f" python/{platform.python_version()}"
    }


def _default_aiohttp_session(config: "SessionConfig") -> aiohttp.ClientSession:
    return aiohttp.ClientSession(headers=config.headers)


@dataclass
class SessionConfig:
    headers: Optional[Dict[str, str]] = field(default_factory=_default_headers)
    chunksize: float = 1024
    file_progress: bool = True
    timeouts: int = 300  # Default to 5 min timeout
    log_level: Optional[str] = None

    def __post_init__(self):
        if self.log_level is None:
            self.log_level = "debug" if "PYMATRIS_DEBUG" in os.environ else None

        # Default minimum values
        if self.chunksize < 1:
            self.chunksize = 1
        if self.timeouts < 1:
            self.timeouts = 1


@dataclass
class DownloaderConfig:
    """
    Hold all downloader session state.
    """

    max_parallel: int = 5
    max_splits: int = 5
    max_tries: int = 5
    all_progress: bool = True
    overwrite: bool = True
    config: Optional[SessionConfig] = field(default_factory=SessionConfig)

    def __post_init__(self):
        if self.config is None:
            self.config = SessionConfig()

        # If all_progress is turned off, auto disable file progress as well
        if not self.all_progress:
            self.config.file_progress = False

        # Default minimum values
        if self.max_parallel < 1:
            self.max_parallel = 1
        if self.max_splits < 1:
            self.max_splits = 1
        if self.max_tries < 1:
            self.max_tries = 1

    def __getattr__(self, __name: str):
        return getattr(self.config, __name)

    def aiohttp_client_session(self):
        return _default_aiohttp_session(self.config)
