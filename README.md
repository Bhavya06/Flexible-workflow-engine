# Flexible-workflow-engine
- Workflow is the sequence of processes through which a piece of work passes from initiation to completion.
- We propose to develop a flexible workflow engine that adapts to changes at runtime.
- The list of tasks to be performed for a business process is given by the organization.

## User Manual 
### Installation Instructions:
- Install python on any operating system
- Install flask webserver, to run the python files
- Keep any browser ready to use the graphical user interface.

### User Manual:
- After all the necessary software is installed, change the path in the flask file to the current working directory.
- Run the flask file present in the zip folder using the command python flask1.py
- Open a browser and type the extension localhost:5000/start to display the user interface.
- When the UI is displayed, it shows few options under the text area:
  - Create task - It enables the user to add the task and time for the task as well.
  - Delete Task/Link - Allows the user to delete the selected task or link between tasks.
  - Links: Links are drawn with the connectors on the sides of the tasks by dragging it from one task to the other.
  - Execute Workflow: Execute the workflow after all tasks and links have been added.
  - Display Logs: Displays the execution logs of the workflow with timestamp.
  - Priority user: Executes the workflow based on the priority given by the user.
  - Calculate Time: Calculates the time taken to execute the workflow.
  - Upload File: Uploads the annotated text file selected by the user.
  - Random Delay: Introduces random delay to the workflow engine based on user preference. 
  - User Query for predicting no.of users: Predicts the total no.of users who can execute the workflow engine in the provided time.
  - User Query for predicting total time: Predicts the total time taken for the execution of the workflow engine by the no.of users provided.
  - The user can drag the tasks or the links after they are created also.
  - The tasks and links can be modified and renamed.

