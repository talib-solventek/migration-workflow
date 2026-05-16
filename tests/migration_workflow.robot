*** Settings ***
Documentation    Example suite driving the ADO -> Jira migration via the
...              MigrationWorkflow keyword library.
...
...              Run from the project root so relative paths (output/,
...              workitems.csv) resolve correctly:
...
...                robot --pythonpath . tests/migration_workflow.robot
Library          MigrationWorkflow.py
Suite Setup      Initialise Migration
Suite Teardown   Close Jira Session


*** Test Cases ***
Pre Flight Checks
    [Documentation]    Verify Jira credentials work and the Work Category
    ...                custom field is configured.
    Validate Work Category Field

Extract ADO Work Items
    [Documentation]    Run the ADO extraction. Reads workitems.csv and
    ...                writes the output/ CSVs.
    [Tags]    extract
    Extract All Work Items
    File Should Exist Locally    output/workitems.csv
    File Should Exist Locally    output/comments.csv
    File Should Exist Locally    output/relations.csv

Create Jira Issues
    [Documentation]    Migrate every row in output/workitems.csv.
    [Tags]    import
    ${summary}=    Import All Work Items
    Log    Created: ${summary}[created]    INFO
    Should Be True    ${summary}[failed] == 0    Some issues failed to create

Migrate Comments And Attachments
    [Tags]    import
    Process Jira Comments
    Process Jira Attachments

Recreate Relations
    [Tags]    import
    Process Jira Relations

Link Children To Epics
    [Documentation]    Requires output/epic_data.csv (ADO_Epic_ID, Jira_Epic_Key).
    [Tags]    epics
    Link All Epics

Update Epic Fields From ADO
    [Tags]    epics
    Update All Jira Epics


*** Keywords ***
Initialise Migration
    Load Migration Config
    Open Jira Session
    Load Ado Jira Mapping

File Should Exist Locally
    [Arguments]    ${path}
    ${ok}=    Evaluate    __import__('pathlib').Path(r'${path}').exists()
    Should Be True    ${ok}    File missing: ${path}
