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
    """
    The aiohttp session with the kwargs stored by this config.

    Notes
    -----
    `aiohttp.ClientSession` expects to be instantiated in a asyncio context
    where it can get a running loop.
    """
    return aiohttp.ClientSession(headers=config.headers)


# @dataclass
# class EnvConfig:
#     """
#     Configuration read from environment variables.
#     """

#     # Session scoped env vars
#     serial_mode: bool = field(default=False, init=False)
#     disable_range: bool = field(default=False, init=False)
#     hide_progress: bool = field(default=False, init=False)
#     debug_logging: bool = field(default=False, init=False)
#     timeout_total: float = field(default=0, init=False)
#     timeout_sock_read: float = field(default=90, init=False)
#     override_use_aiofiles: bool = field(default=False, init=False)

#     def __post_init__(self):
#         self.serial_mode = "PARFIVE_SINGLE_DOWNLOAD" in os.environ
#         self.disable_range = "PARFIVE_DISABLE_RANGE" in os.environ
#         self.hide_progress = "PARFIVE_HIDE_PROGRESS" in os.environ
#         self.debug_logging = "PARFIVE_DEBUG" in os.environ
#         self.timeout_total = float(os.environ.get("PARFIVE_TOTAL_TIMEOUT", 0))
#         self.timeout_sock_read = float(os.environ.get("PARFIVE_SOCK_READ_TIMEOUT", 90))
#         self.override_use_aiofiles = "PARFIVE_OVERWRITE_ENABLE_AIOFILES" in os.environ


@dataclass
class SessionConfig:
    headers: Optional[Dict[str, str]] = field(default_factory=_default_headers)
    chunksize: float = 1024
    file_progress: bool = True
    timeouts: int = 300  # Default to 5 min timeout
    max_tries: int = 5


@dataclass
class DownloaderConfig:
    """
    Hold all downloader session state.
    """

    max_conn: int = 5
    max_splits: int = 5
    all_progress: bool = True
    overwrite: bool = True
    log_level: Optional[str] = None
    config: Optional[SessionConfig] = field(default_factory=SessionConfig)

    def __post_init__(self):
        if self.config is None:
            self.config = SessionConfig()

        # If all_progress is turned off, auto disable file progress as well
        if not self.all_progress:
            self.config.file_progress = False

        if "DEBUG_LEVEL" in os.environ:
            self.log_level = os.environ.get("DEBUG_LEVEL", None)

    def __getattr__(self, __name: str):
        return getattr(self.config, __name)

    def aiohttp_client_session(self):
        return _default_aiohttp_session(self.config)
