from gladier import GladierBaseTool

# Statistics LPAP action provider definition
class statistics(GladierBaseTool):  
    flow_definition = {
      "Comment": "Calculates comparative statistics against validation data", 
      "StartAt": "statistics", 
      "States": {
        "statistics": {
          "End": True,
          "Type": "Action",
          "Comment": "Calculates comparative statistics against validation data",
          "WaitTime": 120,
          "ActionUrl": "http://130.216.217.136:8080/statistics",
          "Parameters": {
              'orchestration_node.$': '$.input.LP_configuration.orchestration_node',
              'article_name.$': '$.input.LP_configuration.article_name',
              'fastText_predictions.$': '$.input.fastText_paths.FT_ST_dest',
              'langdetect_predictions.$': '$.input.langdetect_paths.LD_ST_dest',
              'primary_validation.$': '$.input.datastore_paths.validation_dest_path'
          }
        }
      }
    }

    funcx_functions = [] 

    flow_input = {}

    required_input = {
      "LP_configuration": {
          "orchestration_node",
          "article_name"
      },
      "fastText_paths": {
          "FT_ST_dest"
      },
      "langdetect_paths": {
          "LD_ST_dest"
      },
      "datastore_paths": {
          "validation_dest_path"
      }
    }
