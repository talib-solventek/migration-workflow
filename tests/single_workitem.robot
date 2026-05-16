*** Settings ***
Documentation    Per-issue test that exercises the granular keywords:
...              fetch one ADO work item, create one Jira issue, add one
...              comment, link two issues.
Library          MigrationWorkflow.py
Library          Collections
Suite Setup      Initialise Migration
Suite Teardown   Close Jira Session


*** Variables ***
${ADO_ID}              12345
${ADO_TARGET_ID}       12346
${COMMENT_TEXT}        Migrated from ADO via Robot Framework.


*** Test Cases ***
Fetch Single ADO Work Item
    ${data}=    Fetch Ado Work Item    ${ADO_ID}
    Should Not Be Equal    ${data}    ${NONE}
    Dictionary Should Contain Key    ${data}    fields

Create Single Jira Issue From Dict
    &{row}=    Create Dictionary
    ...    id=${ADO_ID}
    ...    type=User Story
    ...    title=Robot Framework demo issue
    ...    description=Created from the granular MigrationWorkflow keywords.
    ...    state=To Do
    ...    tags=demo
    ${jira_key}=    Create Jira Issue    ${row}
    Should Not Be Equal    ${jira_key}    ${NONE}
    Set Suite Variable    ${JIRA_KEY}    ${jira_key}

Add A Comment To The New Issue
    ${ok}=    Add Jira Comment    ${JIRA_KEY}    ${COMMENT_TEXT}    robot@example.com
    Should Be True    ${ok}

Persist Mapping
    Set Mapping Entry    ${ADO_ID}    ${JIRA_KEY}
    Save Ado Jira Mapping


*** Keywords ***
Initialise Migration
    Load Migration Config
    Open Jira Session
