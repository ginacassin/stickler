"""
This module defines enumerations for rule execution types.
"""

from enum import IntEnum


class ExecutionType(IntEnum):
    """
    Enumeration for different types of rule execution.

    Attributes:
        CASCADE: Represents cascade execution type.
        ACCUMULATIVE: Represents accumulative execution type.
    """

    CASCADE = 0
    ACCUMULATIVE = 1
