from gladier import GladierBaseTool

class DS_LD_Transfer(GladierBaseTool):  
    flow_definition = {
      "Comment": "Transfer test data from datastore to langDetect LPAP", 
      "StartAt": "DS_LD_Transfer", 
      "States": {
        "DS_LD_Transfer": {
          "End": True,
          "Type": "Action",
          "Comment": "Transfer text data for language identification (line deliniated text data)",
          "WaitTime": 120,
          "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
          "Parameters": {
              'transfer_items': [
                  {
                  'recursive': False, 
                  'source_path.$': '$.input.datastore_paths.data_path', 
                  'destination_path.$': '$.input.langdetect_paths.DS_LD_dest'
                }
              ], 
              'source_endpoint_id.$': '$.input.endpoints.datastore_uuid', 
              'destination_endpoint_id.$': '$.input.endpoints.langdetect_uuid'
          },
          "ResultPath": "$.DS_LD_Transfer_result"
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
            "langdetect_uuid"
        },
        "datastore_paths": {
            "data_path"
        },
        "langdetect_paths": {
            "DS_LD_dest"
        }
    }
