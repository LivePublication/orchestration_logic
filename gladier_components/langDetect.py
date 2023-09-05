from gladier import GladierBaseTool

# langDetect lpap gladier action provider definition
class langDetect(GladierBaseTool):  
    flow_definition = {
      "Comment": "Executes langDetect language classification on test data set.", 
      "StartAt": "langDetect", 
      "States": {
        "langDetect": {
          "End": True,
          "Type": "Action",
          "Comment": "Executes langDetect language classification on test data set.",
          "WaitTime": 600,
          "ActionUrl": "http://130.216.217.76:8080/langdetect",
          "Parameters": {
              'orchestration_node.$': '$.input.LP_configuration.orchestration_node',
              'input_data.$': '$.input.langdetect_paths.DS_LD_dest',
              'article_name.$': '$.input.LP_configuration.article_name'
          },
          "ResultPath": "$.langDetect_result"
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
      "langdetect_paths": {
        "DS_LD_dest"
      }
    }
