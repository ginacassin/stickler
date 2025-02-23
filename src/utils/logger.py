"""
    Logger utils
"""
import logging
from logging import NullHandler

logger = logging.getLogger(f"STICKLER.{__name__}")
logger.addHandler(NullHandler())
