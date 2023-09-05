import pprint
import json
import sys
import time

from gladier import GladierBaseClient, generate_flow_definition
from gladier_components.DS_FT_Transfer import DS_FT_Transfer
from gladier_components.DS_ST_Transfer import DS_ST_Transfer
from gladier_components.fastText import fastText
from gladier_components.FT_ST_Transfer import FT_ST_Transfer
from gladier_components.DS_LD_Transfer import DS_LD_Transfer
from gladier_components.langDetect import langDetect
from gladier_components.LD_ST_Transfer import LD_ST_Transfer
from gladier_components.statistics import Statistics

from orchestration_types import OrchestrationData
from pathlib import Path

class LidFlow:
    # Globus personal endpoints UUIDs on LPAPs for transfer definitions
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
        "orchestration_node": "3a8b94cc-4b93-11ee-8138-15041d20ea55",
        "article_name": "full_globus_test"
    }

    @staticmethod
    @generate_flow_definition
    class LiDClient(GladierBaseClient):
        gladier_tools = [
            DS_ST_Transfer,
            DS_FT_Transfer,
            fastText,
            FT_ST_Transfer,
            DS_LD_Transfer,
            langDetect,
            LD_ST_Transfer,
            Statistics
            # DS_ST_Transfer, # Testing
            # DS_FT_Transfer # Testing
        ]

    def __init__(self):
        self.client = self.LiDClient()

    def get_input(self):
        # Input definitions for testing
        # Validation transfer step
        DS_ST_Transfer = {
            'input': {
                'endpoints': {
                    'datastore_uuid': self.endpoints['DS_UUID'],
                    'statistics_uuid': self.endpoints['ST_UUID']
                },
                'datastore_paths': {
                    'validation_path': self.data_paths['validation_path'],
                    'validation_dest_path': self.intermediate_paths['validation_dest_path']
                }
            }
        }
        # Test data for datastore to fasttext transfer step
        DS_FT_Transfer = {
            'input': {
                'endpoints': {
                    'datastore_uuid': self.endpoints['DS_UUID'],
                    'fasttext_uuid': self.endpoints['FT_UUID']
                },
                'datastore_paths': {
                    'data_path': self.data_paths['data_path']
                },
                'fastText_paths': {
                    'DS_FT_dest': self.intermediate_paths['DS_FT_dest']
                }
            }
        }
        # 2 step test
        two_step = {
            'input': {
                'endpoints': {
                    'datastore_uuid': self.endpoints['DS_UUID'],
                    'statistics_uuid': self.endpoints['ST_UUID'],
                    'fasttext_uuid': self.endpoints['FT_UUID'],
                },
                'datastore_paths': {
                    'validation_path': self.data_paths['validation_path'],
                    'validation_dest_path': self.intermediate_paths['validation_dest_path'],
                    'data_path': self.data_paths['data_path']
                },
                'fastText_paths': {
                    'DS_FT_dest': self.intermediate_paths['DS_FT_dest']
                }
            }
        }
        # Test data for fasttext LPAP step
        fastText = {
            'input': {
                'LP_configuration': {
                    'orchestration_node': self.LP_configuration['orchestration_node'],
                    'article_name': self.LP_configuration['article_name']
                },
                'fastText_paths': {
                    'DS_FT_dest': self.intermediate_paths['DS_FT_dest']
                }
            }
        }
        # Test data for fasttext to statistics LPAP step
        FT_ST_Transfer = {
            'input': {
                'endpoints': {
                    'fasttext_uuid': self.endpoints['FT_UUID'],
                    'statistics_uuid': self.endpoints['ST_UUID']
                },
                'fastText_paths': {
                    'FT_output_path': self.intermediate_paths['FT_output_path'],
                    'FT_ST_dest': self.intermediate_paths['FT_ST_dest']
                }
            }
        }
        # Test data for the datastore to langdetect
        DS_LD_Transfer = {
            'input': {
                'endpoints': {
                    'datastore_uuid': self.endpoints['DS_UUID'],
                    'langdetect_uuid': self.endpoints['LD_UUID']
                },
                'datastore_paths': {
                    'data_path': self.data_paths['data_path']
                },
                'langdetect_paths': {
                    'DS_LD_dest': self.intermediate_paths['DS_LD_dest']
                }
            }
        }
        # langDetect LPAP step
        langDetect = {
            'input': {
                'LP_configuration': {
                    'orchestration_node': self.LP_configuration['orchestration_node'],
                    'article_name': self.LP_configuration['article_name']
                },
                'langdetect_paths': {
                    'DS_LD_dest': self.intermediate_paths['DS_LD_dest']
                }
            }
        }
        # Transfer from langdetect to statistics LPAP
        LD_ST_Transfer = {
            'input': {
                'endpoints': {
                    'langdetect_uuid': self.endpoints['LD_UUID'],
                    'statistics_uuid': self.endpoints['ST_UUID']
                },
                'langdetect_paths': {
                    'LD_output_path': self.intermediate_paths['LD_output_path'],
                    'LD_ST_dest': self.intermediate_paths['LD_ST_dest']
                }
            }
        }
        # Statistics LPAP step
        Statistics = {
            'input': {
                'LP_configuration': {
                    'orchestration_node': self.LP_configuration['orchestration_node'],
                    'article_name': self.LP_configuration['article_name']
                },
                'fastText_paths': {
                    'FT_ST_dest': self.intermediate_paths['FT_ST_dest']
                },
                'langdetect_paths': {
                    'LD_ST_dest': self.intermediate_paths['LD_ST_dest']
                },
                'datastore_paths': {
                    'validation_dest_path': self.intermediate_paths['validation_dest_path']
                }
            }
        }
        # Full flow input defintion
        flow_input = {
            'input': {
                'endpoints': {
                    'datastore_uuid': self.endpoints['DS_UUID'],
                    'fasttext_uuid': self.endpoints['FT_UUID'],
                    'statistics_uuid': self.endpoints['ST_UUID'],
                    'langdetect_uuid': self.endpoints['LD_UUID']
                },
                'datastore_paths': {
                    'validation_path': self.data_paths['validation_path'],
                    'validation_dest_path': self.intermediate_paths['validation_dest_path'],
                    'data_path': self.data_paths['data_path']
                },
                'fastText_paths': {
                    'DS_FT_dest': self.intermediate_paths['DS_FT_dest'],
                    'FT_output_path': self.intermediate_paths['FT_output_path'],
                    'FT_ST_dest': self.intermediate_paths['FT_ST_dest']
                },
                'langdetect_paths': {
                    'DS_LD_dest': self.intermediate_paths['DS_LD_dest'],
                    'LD_output_path': self.intermediate_paths['LD_output_path'],
                    'LD_ST_dest': self.intermediate_paths['LD_ST_dest']
                },
                'LP_configuration': {
                    'orchestration_node': self.LP_configuration['orchestration_node'],
                    'article_name': self.LP_configuration['article_name']
                }
              }
            }
            
        return flow_input

    def run(self):
        # Print flow definition
        print(json.dumps(self.client.get_flow_definition(), indent=4))
        self.WEP = self.client.get_flow_definition()
        self.current_run = self.client.run_flow(flow_input=self.get_input())
        return self.current_run
    
    def monitor_run(self):
        # Monitor run and return when complete
        if hasattr(self, 'current_run'):
            start_time = time.time()
            while(self.client.get_status(self.current_run['action_id'])['status'] == 'ACTIVE'):
                elapsed_time = time.time() - start_time
                print(f"Flow {self.client.get_flow_id()} | Run {self.current_run['action_id']} | Time Elapsed: {elapsed_time:.2f}s")
            else:
                print(f"Run {self.current_run['run_id']} complete")
                self.WED = self.client.get_status(self.current_run['action_id'])
                return True
        else:
            raise ValueError('Run has not been started yet')

    def get_components(self):
        # Get components for inlcusion in the OCrate
        return (self.client.gladier_tools)
    
    def get_runID(self):
        # Get the run ID for the OCrate
        return self.current_run['action_id']
    
    def get_WED(self):
        # Get the WED for the OCrate
        return self.WED
    
    def get_data(self) -> OrchestrationData:
        # Get the data for the OCrate
        data = {
            "input": self.get_input(),
            "WEP": self.WEP,
            "WED": self.WED,
            "components": {
                component.__name__: Path(sys.modules[component.__module__].__file__) for component in self.client.gladier_tools
            }
        }
        return OrchestrationData(**data)