import sys
from dbt.events.types import (
    MainReportVersion,
    MainReportArgs,
    RollbackFailed,
    MainEncounteredError,
    PluginLoadError,
)
from dbt.events import core_proto_messages as cpm
from dbt.version import installed
import betterproto

info_keys = {"name", "code", "msg", "level", "invocation_id", "pid", "thread", "ts"}


def test_events():

    # A001 event
    event = MainReportVersion(version=str(installed))
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()
    serialized = bytes(event)
    assert "Running with dbt=" in str(serialized)
    assert set(event_dict.keys()) == {"version", "info"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "A001"

    # Extract EventInfo from serialized message
    generic_event = cpm.GenericMessage().parse(serialized)
    assert generic_event.info.code == "A001"
    # get the message class for the real message from the generic message
    message_class = getattr(sys.modules["dbt.events.core_proto_messages"], generic_event.info.name)
    new_event = message_class().parse(serialized)
    assert new_event.info.code == event.info.code
    assert new_event.version == event.version

    # A002 event
    event = MainReportArgs(args={"one": "1", "two": "2"})
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()

    assert set(event_dict.keys()) == {"info", "args"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "A002"


def test_exception_events():
    event = RollbackFailed(conn_name="test", exc_info="something failed")
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()
    assert set(event_dict.keys()) == {"info", "conn_name", "exc_info"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "E009"

    event = PluginLoadError(exc_info="something failed")
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()
    assert set(event_dict.keys()) == {"info", "exc_info"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "E036"
    # This event has no "msg"/"message"
    assert event.info.msg is None


def test_exception_event():
    # Z002 event
    event = MainEncounteredError(exc="Rollback failed")
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()

    assert set(event_dict.keys()) == {"info", "exc"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "Z002"
