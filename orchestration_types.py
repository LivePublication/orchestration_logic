from dataclasses import dataclass
from typing import Dict, Any, List
from pathlib import Path

@dataclass
class GlobusUser:
    identity_provider: str
    status: str
    email: str
    identity_type: str
    organization: str
    name: str
    id: str
    username: str

# Data class for returned data object to populate orchestration crate
@dataclass
class OrchestrationData:
    input: Dict[str, Any]
    WEP: Dict[str, Any]
    WED: Dict[str, Any]
    WED_clean: Dict[str, Any]
    components: Dict[str, Path]
    flow_id: str
    run_id: str
    article_name: str
    identity_map: Dict[str, GlobusUser]

