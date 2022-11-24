from enum import Enum

class promptType(Enum):
    SYS = '[SYS]'
    WARNING = '[WARNING]'
    ERROR = '[ERROR]'
    INPUT = '[INPUT]'
    DEBUG = '[DEBUG]'