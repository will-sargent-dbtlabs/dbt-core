from dbt.events.types_pb2 import A001, A002, E009, Z002
from dbt.events.types import MainReportVersion, MainReportArgs, RollbackFailed, MainEncounteredError
from dbt.version import installed

from google.protobuf import json_format


def test_events():

    # A001 event
    event = MainReportVersion(version=str(installed))
    event_dict = event.to_dict()
    event_json = event.to_json()

    # create empty message
    msg = A001()
    assert msg
    msg = json_format.Parse(event_json, msg)

    msg_dict = json_format.MessageToDict(msg, preserving_proto_field_name=True)
    assert set(msg_dict.keys()) == {
        "code",
        "msg",
        "level",
        "invocation_id",
        "pid",
        "thread_name",
        "ts",
        "version",
    }
    assert event_dict == msg_dict

    # A002 event
    # Note: the "args" dict is defined as <string, string> so the values
    # must be strings.
    event = MainReportArgs(args={"one": "1", "two": "2"})
    event_dict = event.to_dict()
    event_json = event.to_json()

    msg = A002()
    assert msg
    msg = json_format.Parse(event_json, msg)

    msg_dict = json_format.MessageToDict(msg, preserving_proto_field_name=True)
    assert set(msg_dict.keys()) == {
        "code",
        "msg",
        "level",
        "invocation_id",
        "pid",
        "thread_name",
        "ts",
        "args",
    }
    assert event_dict == msg_dict


def test_rollback_event():
    event = RollbackFailed(conn_name="test")
    event_dict = event.to_dict()
    assert set(event_dict.keys()) == {
        "msg",
        "invocation_id",
        "ts",
        "pid",
        "level",
        "thread_name",
        "conn_name",
        "code",
    }
    event_json = event.to_json()

    msg = E009()
    msg = json_format.Parse(event_json, msg)
    msg_dict = json_format.MessageToDict(msg, preserving_proto_field_name=True)
    assert event_dict == msg_dict


def test_exception_event():
    try:
        var = 10/0
    except BaseException as exc:
        event = MainEncounteredError(exc=exc)

    event_dict = event.to_dict()
    event_json = event.to_json()
    msg = Z002()
    msg = json_format.Parse(event_json, msg)
    msg_dict = json_format.MessageToDict(msg, preserving_proto_field_name=True)
    assert event_dict == msg_dict
