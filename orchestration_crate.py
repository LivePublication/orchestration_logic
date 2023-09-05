import json
import os

from python_mermaid.diagram import (MermaidDiagram, Node, Link)
from rocrate.rocrate import ROCrate
from orchestration_types import OrchestrationData

class Orchestration_crate:
    def __init__(self, flow_instance, orchestration_data: OrchestrationData, crate_directory="RO_Crate"):
        self.flow = flow_instance
        self.crate_directory = crate_directory
        self.crate = ROCrate()
        self.flow_data = orchestration_data

    def serialize(self):
        self.crate.write(self.crate_directory)

    def add_workflow(self):
        pass # TODO
        # self.crate.add_workflow(self.flow_data['WEP'], lang='Amazon States Language', )

    def generate_mermaid_diagram(self):
        # Directly access states
        states = self.flow_data.WEP["States"]

        # Create nodes for Start and End
        start_node = Node("StartNode", shape='circle')
        end_node = Node("EndNode", shape='double-circle')
        nodes = {state_name: Node(state_name) for state_name in states.keys()}

        # Create links
        start_state = self.flow_data.WEP["StartAt"]
        links = [Link(start_node, nodes[start_state])]
        links.extend([Link(nodes[state_name], nodes[state_details["Next"]]) for state_name, state_details in states.items() if "Next" in state_details])
        links.extend([Link(nodes[state_name], end_node) for state_name, state_details in states.items() if state_details.get("End")])

        # Create the Mermaid diagram
        self.workflow_mermaid = MermaidDiagram(
            title=self.flow_data.WED["flow_title"],
            nodes=list(nodes.values()) + [start_node, end_node],
            links=links
        )

        return self.workflow_mermaid


