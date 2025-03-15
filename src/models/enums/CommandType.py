from enum import Enum

class CommandType(Enum):
    ACK_FILE = 1
    FILE_REQUEST = 2
    ACCEPT_FILE = 3
    REJECT_FILE = 4
    ERROR_FILE = 5