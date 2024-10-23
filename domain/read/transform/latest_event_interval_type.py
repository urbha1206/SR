from enum import Enum


class LatestEventIntervalType(str, Enum):
    DAY = "DAY"
    HOUR = "HOUR"
    EVENT = "EVENT"