"""
gateways/ — 交易网关层
"""

from .base import BaseGateway
from .paper_gateway import PaperGateway

__all__ = ["BaseGateway", "PaperGateway"]
