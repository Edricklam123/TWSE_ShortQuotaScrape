from enum import Enum


class PromptType(Enum):
    SYS = '[SYS]'
    WARNING = '[WARNING]'
    ERROR = '[ERROR]'
    INPUT = '[INPUT]'
    DEBUG = '[DEBUG]'