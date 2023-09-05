from dataclasses import dataclass
from typing import Dict, Any, List
from pathlib import Path

# Data class for returned data object to populate orchestration crate
@dataclass
class OrchestrationData:
    input: Dict[str, Any]
    WEP: Dict[str, Any]
    WED: Dict[str, Any]
    components: Dict[str, Path]