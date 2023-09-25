import json
import os
import yaml
import tempfile
import shutil
import logging

from graphviz import Digraph
from rocrate.rocrate import ROCrate, ContextEntity, DataEntity, ComputationalWorkflow
from .orchestration_types import OrchestrationData
from datetime import datetime

# Set up a logger for your script and set its level to INFO
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Step 3: Get the root logger and set its level to WARNING to suppress logs from other libraries
logging.getLogger().setLevel(logging.INFO)

class Orchestration_crate:
    def __init__(self, 
                 flow_instance, 
                 orchestration_data: OrchestrationData, 
                 crate_directory="RO_Crate", 
                 run_label=None, run_tags=None, 
                 local_data=False):

        if local_data:
            self.flow_data = self.deserialize_data()
            self.crate_directory = crate_directory
            self.crate = ROCrate()
            self.flow = None
        else:
            self.flow = flow_instance
            self.crate_directory = crate_directory
            self.crate = ROCrate()
            self.flow_data = orchestration_data

        # Init configuration for top level metadata
        self.crate.name = os.path.basename(self.flow_data.article_name)
        self.crate.datePublished = datetime.now().date()
        self.crate.creator = "Orchestration Server"
        self.crate.license = "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"
        self.run_label = run_label
        self.run_tags = run_tags

    def serialize(self):
        self.crate.write(self.crate_directory)

    def add_workflow(self):
        """
        Manually creates the workflow meta-data object, as the 
        helper functions assume CWL / other common workflow formats
        """
        # Write flow_data.WEP to file 
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".json")
        temp_file.write(json.dumps(self.flow_data.WEP, indent=4))
        temp_file.close()

        workflow = self.crate.add_file(
            source=temp_file.name,
            properties={
                "@type":["File", "SoftwareSourceCode", "ComputationalWorkflow"],
                "name":self.run_label,
                "encoding":"utf-8",
                "content_type":"application/json",
                "Author": None, # TODO
            }
        )

        # Add workflow image
        workflow_svg = self.generate_flow_diagram()
        generate_flow_diagram = self.crate.add_file(
            source=workflow_svg,
            properties={
                "@type":["File", "ImageObject"],
                "name":"Full Globus Workflow Diagram",
                "encoding":"utf-8",
                "content_type":"image/svg+xml",
            }
        )
        workflow["image"] = generate_flow_diagram

        # Add workflow licence 
        workflow["license"] = "https://creativecommons.org/licenses/by-nc-sa/3.0/au/" # TODO check licence type

        self.workflow = workflow
        return workflow

    def add_users(self):

        for globus_uuid, person in self.flow_data.identity_map.items():
            self.crate.add(
                ContextEntity(
                    self.crate,
                    identifier=globus_uuid,
                    properties = {
                        "name":person.name,
                        "email":person.email,
                        "identifierScheme":"Time-based UUID",
                        "@type":"Person",
                        "affiliation":person.organization,
                    }
                )
            )

    def add_steps(self):
        # Uses WEP_clean as an ordered list of steps (Globus -> currently always sequential)
        current_step = 1
        step_list = []
        for key, value in self.flow_data.WED_clean.items():

            # Metadata to store at the top level of the sub-crate step entries
            action_id = value.get('action_id')
            completion_time = value.get('completion_time')
            creator_id = value.get('creator_id').replace('urn:globus:auth:identity:', '')
            manage_by = [item.split(':')[-1] for item in value.get('manage_by', [])]
            monitor_by = [item.split(':')[-1] for item in value.get('monitor_by', [])]
            start_time = value.get('start_time')
            status = value.get('status')
            state_name = value.get('state_name')
            total_execution_time = value.get('total_execution_time')

            # Add step Contextual Entity
            step = self.crate.add(
                ContextEntity(
                    self.crate,
                    identifier=action_id,
                    properties = {
                        "@type":"WorkflowStep",
                        "action_id":action_id,
                        "completion_time":completion_time,
                        "creator_id":creator_id,
                        "manage_by":manage_by,
                        "monitor_by":monitor_by,
                        "start_time":start_time,
                        "status":status,
                        "state_name":state_name,
                        "total_execution_time":total_execution_time,
                        "step_number":current_step,
                    }
                )
            )

            if 'Transfer' not in key:
                # print("LPAP action")
                sub_crate_path = os.path.join(self.flow_data.article_name, action_id)
                if os.path.isdir(sub_crate_path):
                    sub_crate = self.crate.add_tree(source=sub_crate_path)
                    # Link subcrate to step
                    step["hasPart"] = [sub_crate]
                else:
                    raise Exception(f"Subcrate for LPAP action {action_id} does not exist")
            
            # Link step to workflow steps
            step_list.append(step)
            current_step += 1
        
        # Link workflow to steps
        self.workflow["hasPart"] = step_list

    def deserialize_data(self):
        """
        Deserialize OrchestrationData from json file for testing purposes (so you dont need to keep running the flow)
        """
        # Load data from file
        with open("orchestration_data.json", "r") as json_file:
            data_dict = json.load(json_file)
        return OrchestrationData.from_dict(data_dict)

    def generate_flow_diagram(self):
        """
        Generates a svg of the computational flow.
        # TODO inlcude dynamic links to the Ocrate in the svg

        Parameters
        ----------
        flow_data : OrchestrationData
            The OrchestrationData object containing the data from the flow
        """
        self.diagram = Digraph(comment='Orchestration Flow')

        # Add nodes to the graph with 'state_name' as the title and 'execution_time' as an attribute
        for state_name, details in self.flow_data.WED_clean.items():
  
            # Grab the execution time in seconds
            execution_time = details['total_execution_time']
            
            # Convert execution time to a human-readable format (minutes and seconds)
            minutes, seconds = divmod(execution_time, 60)
            execution_time_str = f"{int(minutes)} minutes, {int(seconds)} seconds"
            
            # Determine the color of the node based on the state_name
            color = "green" if "Transfer" in state_name else "blue"
            
            # Add the node to the graph with the specified attributes
            self.diagram.node(state_name, label=f"{state_name}\nExecution time: {execution_time_str}", shape="box", color=color, style="filled")

        # Add directed edges between the nodes to represent the flow from one step to the next
        state_names = list(self.flow_data.WED_clean.keys())
        for i in range(len(state_names) - 1):
            self.diagram.edge(state_names[i], state_names[i + 1])

        # Render the graph to files in both PNG and SVG formats
        output_base = 'flow_diagram'
        return self.diagram.render(filename=output_base, format='svg')

    def clean_up(self):
        # Removes sub-crates, and other artefacts after constructing the orchestration crate
        # Remove sub-crates
        shutil.rmtree(self.flow_data.article_name)
        # Remove diagram
        os.remove("flow_diagram.svg")
        os.remove("flow_diagram")
        
    def build_crate(self):
        self.add_users() # Add users to orchestration crate
        self.add_workflow()
        self.add_steps()
        self.serialize()

        logger.info(f"Orchestration crate built at {self.crate_directory}")
        

