from abc import ABCMeta, abstractproperty, abstractmethod
from dataclasses import dataclass
from dbt.events.serialization import EventSerialization
import os
import threading
from datetime import datetime


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class Cache:
    # Events with this class will only be logged when the `--log-cache-events` flag is passed
    pass


def get_invocation_id() -> str:
    from dbt.events.functions import get_invocation_id

    return get_invocation_id()


# exactly one pid per concrete event
def get_pid() -> int:
    return os.getpid()


# preformatted time stamp
def get_ts_rfc3339() -> str:
    ts = datetime.utcnow()
    ts_rfc3339 = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return ts_rfc3339


# in theory threads can change so we don't cache them.
def get_thread_name() -> str:
    return threading.current_thread().name


# top-level superclass for all events
class Event(metaclass=ABCMeta):
    # Do not define fields with defaults here, it will break subclasses
    msg: str
    invocation_id: str
    ts: str
    pid: int
    level: str
    thread_name: str

    def __post_init__(self):
        if not hasattr(self, "msg") or not self.msg:
            self.msg = self.message()
        self.level = self.level_tag()
        self.invocation_id = get_invocation_id()
        self.ts = get_ts_rfc3339()
        self.pid = get_pid()
        self.thread_name = get_thread_name()

    # four digit string code that uniquely identifies this type of event
    # uniqueness and valid characters are enforced by tests
    @abstractproperty
    @staticmethod
    def code() -> str:
        raise Exception("code() not implemented for event")

    # The 'to_dict' method is added by mashumaro via the EventSerialization.
    # It should be in all subclasses that are to record actual events.
    @abstractmethod
    def to_dict(self):
        raise Exception("to_dict not implemented for Event")

    # do not define this yourself. inherit it from one of the above level types.
    @abstractmethod
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for Event")

    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    # Must override yourself
    @abstractmethod
    def message(self) -> str:
        raise Exception("msg not implemented for Event")


# in preparation for #3977
@dataclass  # type: ignore[misc]
class TestLevel(EventSerialization, Event):
    __test__ = False

    def level_tag(self) -> str:
        return "test"


@dataclass  # type: ignore[misc]
class DebugLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "debug"


@dataclass  # type: ignore[misc]
class InfoLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "info"


@dataclass  # type: ignore[misc]
class WarnLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "warn"


@dataclass  # type: ignore[misc]
class ErrorLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "error"


@dataclass
class BaseEvent:
    """BaseEvent for proto message generated python events"""

    def __post_init__(self):
        super().__post_init__()
        self.info.level = self.level_tag()
        if not hasattr(self.info, "msg") or not self.info.msg:
            self.info.msg = self.message()
        self.info.invocation_id = get_invocation_id()
        self.info.ts = datetime.utcnow()
        self.info.pid = get_pid()
        self.info.thread = get_thread_name()
        self.info.code = self.code()
        self.info.name = type(self).__name__

    def level_tag(self):
        raise Exception("level_tag() not implemented for event")

    def message(self):
        raise Exception("message() not implemented for Event")


@dataclass
class TestLvl(BaseEvent):
    def level_tag(self) -> str:
        return "test"


@dataclass  # type: ignore[misc]
class DebugLvl(BaseEvent):
    def level_tag(self) -> str:
        return "debug"


@dataclass  # type: ignore[misc]
class InfoLvl(BaseEvent):
    def level_tag(self) -> str:
        return "info"


@dataclass  # type: ignore[misc]
class WarnLvl(BaseEvent):
    def level_tag(self) -> str:
        return "warn"


@dataclass  # type: ignore[misc]
class ErrorLvl(BaseEvent):
    def level_tag(self) -> str:
        return "error"


# prevents an event from going to the file
# This should rarely be used in core code. It is currently
# only used in integration tests and for the 'clean' command.
class NoFile:
    pass


# prevents an event from going to stdout
class NoStdOut:
    pass
