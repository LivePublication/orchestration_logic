import os
import json

from pathlib import Path
from LidFlow import LidFlow
from orchestration_crate import Orchestration_crate
from orchestration_types import OrchestrationData

from globus_sdk import AuthClient, TransferClient, ConfidentialAppAuthClient, RefreshTokenAuthorizer

if __name__ == '__main__':

    # Create a globus client for searching current sub-crate transfers. 
    # This is used to determine if a transfer is already in progress.

    lid_flow = LidFlow()
    lid_flow.run()
    lid_flow.monitor_run()

    # orchestration_data: OrchestrationData = lid_flow.get_data()
    # orchestration_crate = Orchestration_crate(lid_flow, orchestration_data, (Path.cwd() / "working_crate"))
    # print(orchestration_crate.generate_mermaid_diagram())

