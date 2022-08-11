from dbt.events.types_pb2 import A001, A002
from dbt.events.types import MainReportVersion, MainReportArgs
from dbt.version import installed

from google.protobuf import json_format


def test_events():

    # A001 event
    event = MainReportVersion(v=str(installed))
    json_event = event.to_json()

    # create empty message
    event1 = A001()
    assert event1

    event1 = json_format.Parse(json_event, event1)

    event_dict = json_format.MessageToDict(event1)
    assert event_dict == {"code": "A001", "v": str(installed)}

    # A002 event
    # Note: the "args" dict is defined as <string, string> so the values
    # must be strings.
    event = MainReportArgs(args={"one": "1", "two": "2"})
    json_event = event.to_json()
    print(f"--- A002 json_event: {json_event}")

    event2 = A002()
    assert event2

    event2 = json_format.Parse(json_event, event2)

    event_dict = json_format.MessageToDict(event2)
    assert event_dict == {"code": "A002", "args": {"one": "1", "two": "2"}}
