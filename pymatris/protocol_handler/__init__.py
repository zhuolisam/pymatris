from .base_handler import ProtocolResolver, ProtocolHandler
from .http_handler import HTTPHandler
from .ftp_handler import FTPHandler
from .sftp_handler import SFTPHandler

ProtocolResolver.register_protocol(["http", "https"])(HTTPHandler)
ProtocolResolver.register_protocol(["ftp"])(FTPHandler)
ProtocolResolver.register_protocol(["sftp"])(SFTPHandler)

__all__ = [ProtocolHandler]
