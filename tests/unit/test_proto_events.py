from dbt.events.types_pb2 import A001, A002
from dbt.events.types import MainReportVersion, MainReportArgs
from dbt.version import installed

from google.protobuf import json_format

def test_events():

    event = MainReportVersion(v=str(installed))
    json_event = event.to_json()

    # create empty message
    event1 = A001()
    assert event1

    event1 = json_format.Parse(json_event, event1)

    event_dict = json_format.MessageToDict(event1)
    assert event_dict == {'code': 'A001', 'v': str(installed)}
