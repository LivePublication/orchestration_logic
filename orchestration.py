import os
import json

from pathlib import Path
from LidFlow import LidFlow
from orchestration_crate import Orchestration_crate
from orchestration_types import OrchestrationData

if __name__ == '__main__':
    lid_flow = LidFlow()
    lid_flow.run()
    lid_flow.monitor_run()


    orchestration_data: OrchestrationData = lid_flow.get_data()
    orchestration_crate = Orchestration_crate(lid_flow, orchestration_data, (Path.cwd() / "working_crate"))
    print(orchestration_crate.generate_mermaid_diagram())

