# from dbt.events.types_pb2 import A001, A002, E009, Z002
from dbt.events.types import (
    MainReportVersion,
    MainReportArgs,
    RollbackFailed,
    MainEncounteredError,
)
from dbt.version import installed
import betterproto

info_keys = {"code", "msg", "level", "invocation_id", "pid", "thread_name", "ts"}


def test_events():

    # A001 event
    event = MainReportVersion(version=str(installed))
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()

    assert set(event_dict.keys()) == {"version", "info"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "A001"

    # A002 event
    event = MainReportArgs(args={"one": "1", "two": "2"})
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()

    assert set(event_dict.keys()) == {"info", "args"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "A002"


def test_rollback_event():
    event = RollbackFailed(conn_name="test")
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()
    assert set(event_dict.keys()) == {"info", "conn_name"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "E009"


def test_exception_event():
    # Z002 event
    event = MainEncounteredError(exc="Rollback failed")
    event_dict = event.to_dict(casing=betterproto.Casing.SNAKE)
    event_json = event.to_json()

    assert set(event_dict.keys()) == {"info", "exc"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "Z002"
