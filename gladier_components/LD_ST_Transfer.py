from gladier import GladierBaseTool

# Language Detection to Statistics LPAP action provider definition
class LD_ST_Transfer(GladierBaseTool):  
    flow_definition = {
      "Comment": "Transfer text result to statistics lpap", 
      "StartAt": "LD_ST_Transfer", 
      "States": {
        "LD_ST_Transfer": {
          "End": True,
          "Type": "Action",
          "Comment": "Transfer text result to statistics lpap",
          "WaitTime": 600,
          "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
          "Parameters": {
              'source_endpoint_id.$': '$.input.endpoints.langdetect_uuid', 
              'destination_endpoint_id.$': '$.input.endpoints.statistics_uuid',
              'transfer_items': [
                  {
                  'recursive': False, 
                  'source_path.$': '$.input.langdetect_paths.LD_output_path', 
                  'destination_path.$': '$.input.langdetect_paths.LD_ST_dest'
                  }
              ], 
          },
          "ResultPath": "$.LD_ST_Transfer_result"
        }
      }
    }

    funcx_functions = [] 

    flow_input = {}

    required_input = {
        "endpoints": {
            "langdetect_uuid",
            "statistics_uuid"
        },
        "langdetect_paths": {
            "LD_output_path",
            "LD_ST_dest"
        }
    }
