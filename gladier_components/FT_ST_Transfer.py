from gladier import GladierBaseTool

# fastText to Statistics LPAP action provider definition
class FT_ST_Transfer(GladierBaseTool):  
    flow_definition = {
      "Comment": "Transfer processed text to statistics LPAP", 
      "StartAt": "FT_ST_Transfer", 
      "States": {
        "FT_ST_Transfer": {
          "End": True,
          "Type": "Action",
          "Comment": "Transfer processed text to statistics LPAP",
          "WaitTime": 600,
          "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
          "Parameters": {
              'source_endpoint_id.$': '$.input.endpoints.fasttext_uuid', 
              'destination_endpoint_id.$': '$.input.endpoints.statistics_uuid',
              'transfer_items': [
                  {
                  'recursive': False, 
                  'source_path.$': '$.input.fastText_paths.FT_output_path', 
                  'destination_path.$': '$.input.fastText_paths.FT_ST_dest'
                  }
              ], 
          },
          "ResultPath": "$.FT_ST_Transfer_result"
        }
      }
    }

    funcx_functions = [] 

    flow_input = {
        'transfer_sync_level': 'checksum'
    }

    required_input = {
        "endpoints": {
            "fasttext_uuid",
            "statistics_uuid"
        },
        "fastText_paths": {
            "FT_output_path",
            "FT_ST_dest"
        }
    }
