from gladier import GladierBaseTool

# Template gladier action provider definition
class TemplateClass(GladierBaseTool):  
    flow_definition = {
      "Comment": "",        # Discription of the flow
      "StartAt": "Example", # Single step name
      "States": {
        "Example": {
          "End": True,      # Always true
          "Type": "Action", # Always Action
          "Comment": "", 
          "WaitTime": 600,  # Time to wait for the step to complete
          "ActionUrl": "http://130.216.217.76:8080/langdetect", # URL of the step
          "Parameters": {  # Parameters to pass to the step
            "param1.$": "$.input.param1",
            "param2.$": "$.input.param2",
            "param3.$": "$.input.param3"
          },
          "ResultPath": "$.example_results" # Where to store the results
        }
      }
    }

    funcx_functions = []        # List of funcx functions to run (currently not used)

    flow_input = {}             # Input to the flow

    required_input = [          # Required input to the flow
      'param1',
      'param2',
      'param3'
    ]