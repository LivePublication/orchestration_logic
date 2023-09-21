"""
This module defines the LidFlow class, which is used to run the LiD workflow. The LidFlow class is
responsible for running the workflow, monitoring the workflow, and passing data for Ocrate
generation.

# TODO: Generalise this to work with any Gladier workflow (should not be too difficult)
"""
import json
import sys
import time
import yaml
import logging

from datetime import datetime

# Step 1 and 2: Set up a logger for your script and set its level to INFO
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Step 3: Get the root logger and set its level to WARNING to suppress logs from other libraries
logging.getLogger().setLevel(logging.INFO)

# Step 4: Set up a console handler for your logger and set its level to INFO
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

from gladier import GladierBaseClient, generate_flow_definition
from gladier_components.DS_FT_Transfer import DS_FT_Transfer
from gladier_components.DS_ST_Transfer import DS_ST_Transfer
from gladier_components.fastText import fastText
from gladier_components.FT_ST_Transfer import FT_ST_Transfer
from gladier_components.DS_LD_Transfer import DS_LD_Transfer
from gladier_components.langDetect import langDetect
from gladier_components.LD_ST_Transfer import LD_ST_Transfer
from gladier_components.statistics import Statistics

from globus_sdk.scopes import TransferScopes, FlowsScopes, AuthScopes
from globus_sdk import TransferClient, FlowsClient, AuthClient
from globus_sdk.paging import Paginator

from orchestration_types import OrchestrationData, GlobusUser
from pathlib import Path

class LidFlow:
    
    def __init__(self, endpoints, data_paths, intermediate_paths, LP_configuration):
      self.client = self.LiDClient()
      self.endpoints = endpoints
      self.data_paths = data_paths
      self.intermediate_paths = intermediate_paths
      self.LP_configuration = LP_configuration
      

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
        ]

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
        # Three step test case - including LPAP execution
        three_step = {
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
                },
                'LP_configuration': {
                    'orchestration_node': self.LP_configuration['orchestration_node'],
                    'article_name': self.LP_configuration['article_name']
                },
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
        # Add WEP for OCrate
        self.WEP = yaml.load(yaml.dump(self.client.get_flow_definition()), Loader=yaml.FullLoader)
        """ 
        Make sure the client has access to the Transfer and view_identities APIs for 
        monitoring sub-crate transfer and identity mapping. 
        """
        self.client.login_manager.add_requirements([TransferScopes.all])
        self.client.login_manager.add_requirements([AuthScopes.view_identities])
        self.run = self.client.run_flow(flow_input=self.get_input())
        self.run_id = self.run['action_id']
        self.flow_id = self.client.get_flow_id()
        return self.run
    
    def clean_WED(self):
        # Clean up the WED for ease of use
        cleaned_data = {}

        # Iterating over the YAML data to extract necessary details
        for i in range(len(self.WED)-1):
            # If the current block is "ActionStarted", we extract the "state_name"
            if self.WED[i]['code'] == 'ActionStarted':
                action_name = self.WED[i]['details']['state_name']
                start_time = datetime.fromisoformat(self.WED[i]['time'].replace('Z', '+00:00'))
                # Finding the corresponding "ActionCompleted" block and extracting results
                if self.WED[i+1]['code'] == 'ActionCompleted':
                    result_key1 = f"{action_name}_result"
                    result_key2 = f"{action_name}_results"
                    """
                    There is inconsistancy in the key names for the results. This must be dependent 
                    on how many dependent steps are included in the action log. This is a temporary fix.
                    """
                    action_results = self.WED[i+1]['details']['output'].get(result_key1) or \
                                    self.WED[i+1]['details']['output'].get(result_key2) or \
                                    self.WED[i+1]['details']['output']

                    
                    # Storing the results in the dictionary if action_results is found
                    if action_results:
                        end_time = datetime.fromisoformat(self.WED[i+1]['time'].replace('Z', '+00:00'))
                        total_time = (end_time - start_time).total_seconds()
                        action_results['total_execution_time'] = total_time # Add total execution time to results
                        cleaned_data[action_name] = action_results

        return cleaned_data
    
    def transfers_complete(self, client, action_labels_dir):
        """
        Returns false if a transfer exists, and is not complete. 
        """
        # Ensure self.WED_clean is initialized
        if not hasattr(self, 'WED_clean'):
            raise AttributeError("The attribute 'WED_clean' is not initialized.")
        
        # Grab action labels 
        action_labels = {action_label: action for action, action_label in action_labels_dir.items()}
        transfers = client.task_list(filter={"label": action_labels})

        for transfer in transfers:
            if transfer['status'] != 'SUCCEEDED':
                return False

        # All transfers in success state
        # TODO check / create logic for failed transfer states
        return True

        
    def monitor_transfer(self):
        """
        1. Monitor globus transfer execution
        2. Confirm / wait for transfer to complete
        """
        # Grab the transfer auth token from the login_manager, and use that token
        # to init an API client.
        transfer_authorizor = self.client.login_manager.get_authorizers()[TransferScopes.all]
        transfer_client = TransferClient(authorizer=transfer_authorizor)

        # Filter self.clean_WED for all actions that don't include 'Transfer' in the name
        # Assumes all actions that are not transfers are LPAPs # TODO LPAP type field in WED?
        filtered_actions = {key: value for key, value in self.WED_clean.items() if 'Transfer' not in key}
        action_labels_dir = {key: value['action_id'] for key, value in filtered_actions.items()}

        sleep_time = 5  
        max_sleep_time = 60  
    
        while not self.transfers_complete(transfer_client, action_labels_dir):
            logger.info(f"LPAP background transfers in progress. Backoff {sleep_time} seconds")
            time.sleep(sleep_time)

            # Increase the sleep time exponentially, up to the maximum defined sleep time
            sleep_time = min(sleep_time * 1.5, max_sleep_time)
        else:
            logger.info("LPAP transfers complete. Ready for orchestration crate generation.")


    def monitor_run(self):
        """
        1. Monitor globus flow execution
        2. Confirm / wait for sub-crate transfers to complete
        """
        if hasattr(self, 'run'):
            start_time = time.time()
            while(self.client.get_status(self.run['action_id'])['status'] == 'ACTIVE'):
                elapsed_time = time.time() - start_time
                print(f"Flow {self.flow_id} | Run {self.run_id} | Time Elapsed: {elapsed_time:.2f}s", end='\r', flush=True)
            else:
                print(f"Run {self.run_id} complete | Waiting on subcrate transfers to complete")

                # Set up clients for interfacing with Globus API's
                flow_status_authorizor = self.client.login_manager.get_authorizers()[FlowsScopes.run_status]
                flow_status_client = FlowsClient(authorizer=flow_status_authorizor)

                run_log_paginator = Paginator.wrap(flow_status_client.get_run_logs)
                # Raw WED description -> list of start/complete states with associated action details
                self.WED = [item for item in run_log_paginator(self.run_id).items()]
                # Transformed WED definition for ease of use
                self.WED_clean = self.clean_WED()

                return True
        else:
            raise ValueError('Run has not been started yet')

    def get_components(self):
        # Get components for inlcusion in the OCrate
        return (self.client.gladier_tools)
    
    def get_WED(self):
        # Get the WED for the OCrate
        return self.WED
    
    def get_identities(self):
        """
        Extracts unique Globus IDs from the specified JSON file.

        Parameters:
        self.WED_clean (dict): The cleaned WED definition.

        Returns:
        set: A set of Globus IDs.
        """
        # Ensure self.WED_clean is initialized
        if not hasattr(self, 'WED_clean'):
            raise AttributeError("The attribute 'WED_clean' is not initialized.")
        unique_ids = set()
        # We dont need to classify the category of identity (creator, manager, monitor)
        # This can be done during the OCrate generation
        for entry in self.WED_clean.values():
            if 'creator_id' in entry:
                unique_ids.add(entry['creator_id'].replace('urn:globus:auth:identity:', ''))
            if 'manage_by' in entry:
                for manager in entry['manage_by']:
                    unique_ids.add(manager.replace('urn:globus:auth:identity:', ''))
            if 'monitor_by' in entry:
                for monitor in entry['monitor_by']:
                    unique_ids.add(monitor.replace('urn:globus:auth:identity:', ''))
        return unique_ids
    
    def identity_mapping(self):
        """
        Maps extracted Globus IDs to their associated identities using Globus Auth API.

        Parameters:
        unique_ids (set): A set of Globus IDs.

        Returns:
        self.identity_map Dict[str:GlobusUser]: A dictionary mapping Globus IDs to their GlobusUser objects.
        """
        unique_ids = self.get_identities()      # Get unique Globus IDs
        self.identity_map = {}                  # Init identity map

        # Set up clients for interfacing with Globus Auth API
        auth_authorizor = self.client.login_manager.get_authorizers()[AuthScopes.view_identities]
        auth_client = AuthClient(authorizer=auth_authorizor)

        for g_id in unique_ids:
            identity = auth_client.get_identities(ids=g_id)['identities'][0]
            self.identity_map[g_id] = GlobusUser(**identity)

        return self.identity_map
    
    def serrialize_data(self):
        """
        A function for saving the data used to generate the OCrate to file for testing purposes.

        Parameters:
        self.OrchestrationData (OrchestrationData): An object containing the data for the OCrate.
        """

        # Check that self.OrchestrationData is initialized
        logger.info("Writing orchestration data to file")
        with open("orchestration_data.json", "w") as f:
            f.write(json.dumps(self.get_data().to_dict(), indent=4))
        

    def get_data(self) -> OrchestrationData:
        """
        Collate and return data for OCrate generation.

        Parameters:
        self.WEP (dict): The workflow execution plan.
        self.WED (dict): The workflow execution details.
        self.WED_clean (dict): The cleaned workflow execution details.
        self.client.gladier_tools (list): A list of Gladier tools used in the workflow.
        self.flow_id (str): The ID of the workflow.
        self.run_id (str): The ID of the workflow run.
        self.LP_configuration (dict): The LivePublication configuration.

        Returns:
        OrchestrationData: An object containing the data for the OCrate.
        """

        attributes_to_check = [
            'client', 'flow_id', 'run_id', 'LP_configuration', 
            'WEP', 'WED', 'WED_clean'
        ]

        for attr in attributes_to_check:
            if not hasattr(self, attr):
                raise AttributeError(f"The attribute '{attr}' is not initialized.")

        data = {
            "input": self.get_input(),
            "WEP": self.WEP,
            "WED": self.WED,
            "WED_clean": self.WED_clean,
            "components": {
                component.__name__: Path(sys.modules[component.__module__].__file__) for component in self.client.gladier_tools
            },
            "flow_id": self.flow_id,
            "run_id": self.run_id,
            "article_name": self.LP_configuration['article_name'],
            "identity_map": self.identity_mapping()
        }
        
        return OrchestrationData(**data)