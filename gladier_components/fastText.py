from gladier import GladierBaseTool

# fastText lpap gladier action provider definition
class fastText(GladierBaseTool):  
    flow_definition = {
      "Comment": "Executes fastText language classification on test data set.", 
      "StartAt": "fastText", 
      "States": {
        "fastText": {
          "End": True,
          "Type": "Action",
          "Comment": "Executes fastText language classification on test data set.",
          "WaitTime": 600,
          "ActionUrl": "http://130.216.216.248:8080/fastText",
          "Parameters": {
              'orchestration_node.$': '$.input.LP_configuration.orchestration_node', 
              'input_data.$': '$.input.fastText_paths.DS_FT_dest', 
              'article_name.$': '$.input.LP_configuration.article_name'
          },
          "ResultPath": "$.fastText_result"
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
        "DS_FT_dest"
      }
    }