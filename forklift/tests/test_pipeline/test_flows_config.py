import prefect

from forklift.pipeline.flows_config import get_flows_to_register


def test_flows_registration():
    for flow in get_flows_to_register():
        serialized_flow = flow.serialize()
        prefect.serialization.flow.FlowSchema().load(serialized_flow)
