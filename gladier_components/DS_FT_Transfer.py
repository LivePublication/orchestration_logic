from gladier import GladierBaseTool

# Data Store to FastText action provider definition
class DS_FT_Transfer(GladierBaseTool):  
    flow_definition = {
      "Comment": "Transfer text data for language identification (line deliniated text data)", 
      "StartAt": "DS_FT_Transfer", 
      "States": {
        "DS_FT_Transfer": {
          "End": True,
          "Type": "Action",
          "Comment": "Transfer text data for language identification (line deliniated text data)",
          "WaitTime": 600,
          "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
          "Parameters": {
            "source_endpoint_id.$": "$.input.endpoints.datastore_uuid", 
            "destination_endpoint_id.$": "$.input.endpoints.fasttext_uuid",
            "transfer_items": [
              {
                "source_path.$": "$.input.datastore_paths.data_path", 
                "destination_path.$": "$.input.fastText_paths.DS_FT_dest",
                "recursive": False, 
              }
            ]
          },
          "ResultPath": "$.DS_FT_Transfer_result"
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
        "fasttext_uuid"
      },
      "datastore_paths": {
        "data_path"
      },
      "fastText_paths": {
        "DS_FT_dest"
      }
    }
  
