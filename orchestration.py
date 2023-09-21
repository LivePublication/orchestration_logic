from pathlib import Path
from LidFlow import LidFlow
from orchestration_crate import Orchestration_crate
from orchestration_types import OrchestrationData

from globus_sdk import LocalGlobusConnectPersonal
import json

if __name__ == '__main__':

    # Get local GCP instance so we can set it for sub-crate transfers
    local_gcp = LocalGlobusConnectPersonal()
    ep_id = local_gcp.endpoint_id

    # Configuration for LidFlow as top level attributes (we could easily move this to a config file)
    # Customizable attributes for re-execution by readers? # TODO
    endpoints = {
        "FT_UUID": "5612672e-1ead-11ee-abf1-63e0d97254cd",
        "ST_UUID": "105a24f4-2a94-11ee-8801-056a4e394379",
        "LD_UUID": "21968ff8-29c4-11ee-87ff-056a4e394379",
        "DS_UUID": "d6215ec8-244a-11ee-80c1-a3018385fcef"
    }

        # Data paths for the actual test and validation data
    data_paths = {
        "validation_path": "/home/ubuntu/LiD_Datastore/validation.txt",
        "data_path": "/home/ubuntu/LiD_Datastore/input_data.txt"
    }

        # Intermediate paths for data transfer between LPAPs
    intermediate_paths = {
        "validation_dest_path": "/home/ubuntu/statistics_lpap/input/validation.txt",
        "DS_FT_dest": "/home/ubuntu/fastText_lpap/input/input_data.txt",
        "DS_LD_dest": "/home/ubuntu/langdetect_lpap/input/input_data.txt",
        "FT_ST_dest": "/home/ubuntu/statistics_lpap/input/fastText_predictions.txt",
        "LD_ST_dest": "/home/ubuntu/statistics_lpap/input/langdetect_predictions.txt",
        "FT_output_path": "/home/ubuntu/fastText_lpap/output/fastText_predictions.txt",
        "LD_output_path": "/home/ubuntu/langdetect_lpap/output/langdetect_predictions.txt"
    }

        # LivePublication configuration
    LP_configuration = {
        "orchestration_node": ep_id,
        "article_name": "/Users/eller/Projects/orchestration_logic/sub_crates"
    }

    lid_flow = LidFlow(endpoints, 
                       data_paths, 
                       intermediate_paths, 
                       LP_configuration)    # Create flow object
    lid_flow.run()                          # Run flow
    lid_flow.monitor_run()                  # Wait untill Globus flow complete
    lid_flow.monitor_transfer()             # Wait untill LPAP transfers to orchestration server complete 

    orchestration_data: OrchestrationData = lid_flow.get_data() # Get orchestration data
    orchestration_crate = Orchestration_crate(lid_flow, orchestration_data, (Path.cwd() / "working_crate")) # Create orchestration crate object
    orchestration_crate.generate_flow_diagram() # Generate flow diagram
    orchestration_crate.add_users() # Add users to orchestration crate
    orchestration_crate.add_steps() # Add steps to orchestration crate
    orchestration_crate.serialize() # Serialize orchestration crate


    lid_flow.serrialize_data() # Serialize orchestration data for testing

    # oCrate = Orchestration_crate(None, None, (Path.cwd() / "working_crate"), True)
    # oCrate.deserialize_data() # Using local data for testing
    # oCrate.generate_flow_diagram() # Generate flow diagram
    # oCrate.add_users() # Add users to orchestration crate
    # oCrate.add_steps()
    # oCrate.serialize()
