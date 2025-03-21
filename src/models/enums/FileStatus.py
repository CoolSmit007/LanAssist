from enum import Enum

class FileStatus(Enum):
    WAITING_FOR_ACCEPTANCE = 1
    WAITING_FOR_USER_INPUT = 2
    PROCESSING = 3
    PAUSED = 4
    CANCELLED = 5
    PROCESSED = 6
    REJECTED = 7