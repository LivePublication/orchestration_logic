from dataclasses import dataclass, asdict
from typing import Dict, Any
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

    def to_dict(self):
        # Use the asdict function to get a dictionary representation of the data class
        return asdict(self)
    
    @classmethod
    def from_dict(cls, d):
        return cls(
            identity_provider=d['identity_provider'],
            status=d['status'],
            email=d['email'],
            identity_type=d['identity_type'],
            organization=d['organization'],
            name=d['name'],
            id=d['id'],
            username=d['username']
        )

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

    def to_dict(self):
        # Use the asdict function to get a dictionary representation of the data class
        data_dict = asdict(self)
        
        # Convert Path objects to strings
        data_dict['components'] = {k: str(v) for k, v in data_dict['components'].items()}
        
        # Convert GlobusUser objects to dictionaries, if they are not already dictionaries
        data_dict['identity_map'] = {k: v.to_dict() if hasattr(v, 'to_dict') else v for k, v in data_dict['identity_map'].items()}
        
        return data_dict
    
    @classmethod
    def from_dict(cls, d):
        return cls(
            input=d['input'],
            WEP=d['WEP'],
            WED=d['WED'],
            WED_clean=d['WED_clean'],
            components={k: Path(v) for k, v in d['components'].items()},
            flow_id=d['flow_id'],
            run_id=d['run_id'],
            article_name=d['article_name'],
            identity_map={k: GlobusUser.from_dict(v) for k, v in d['identity_map'].items()}
        )

