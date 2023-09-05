from gladier import GladierBaseTool

# Data Store to Statistics LPAP action provider definition
class DS_ST_Transfer(GladierBaseTool):  
    flow_definition = {
      "Comment": "Transfer validation data to statistics node for use in validation step",  
      "StartAt": "DS_ST_Transfer", 
      "States": {
        "DS_ST_Transfer": {
          "End": True,        
          "Type": "Action",   
          "Comment": "Transfer step form DataStore to Statistics LPAP", 
          "WaitTime": 600,    
          "ActionUrl": "https://actions.automate.globus.org/transfer/transfer", 
          "Parameters": {    
            "source_endpoint_id.$": "$.input.endpoints.datastore_uuid",
            "destination_endpoint_id.$": "$.input.endpoints.statistics_uuid",
            "transfer_items": [
              {
                "source_path.$": "$.input.datastore_paths.validation_path",
                "destination_path.$": "$.input.datastore_paths.validation_dest_path",
                "recursive": False,
              }
            ]
          },
          "ResultPath": "$.DS_ST_Transfer_results" 
        }
      }
    }

    funcx_functions = []       

    flow_input = {
        'transfer_sync_level': 'checksum'
    }            

    required_input = {
      "endpoints": {
        "datastore_uuid",
        "statistics_uuid"
      },
      "datastore_paths": {
        "validation_path",
        "validation_dest_path"
      }
    }