# migration-workflow

ADO -> Jira migration scripts, also exposed as a Robot Framework keyword library.

## Layout

```
extract_workitems_data.py     # Pull ADO work items into output/*.csv
jira_import.py                # Create Jira issues / comments / attachments / links
epic_linker.py                # Set Epic Link (customfield_10600) on children
jira_update_epics.py          # Post-migration Epic field sync
MigrationWorkflow.py          # Robot Framework library wrapping the above
tests/                        # Example .robot suites
```

The four scripts can still be run directly (`python jira_import.py`, etc.). The
Robot library imports them unchanged.

## Setup

```bash
python -m venv .venv
. .venv/Scripts/activate     # or source .venv/bin/activate on *nix
pip install -r requirements.txt
```

`.env` must define:

```
ADO_ORG=...
ADO_PROJECT=...
ADO_PAT=...

JIRA_BASE_URL=https://...
JIRA_EMAIL=...
JIRA_API_TOKEN=...
JIRA_PROJECT_KEY=...
JIRA_COMPONENT=ADOImported     # optional
```

## Running Robot Framework

From the project root (so the relative `output/` and `workitems.csv` paths
resolve):

```bash
robot --pythonpath . tests/migration_workflow.robot
robot --pythonpath . tests/single_workitem.robot
```

## Keyword reference

All keywords are methods on the `MigrationWorkflow` library class.
`Load Migration Config` and `Open Jira Session` must run before any Jira keyword.

### Configuration / session
- `Load Migration Config` — read .env, return Jira config dict
- `Open Jira Session` — open authenticated requests.Session
- `Close Jira Session` — close session and clear state

### ADO extraction
- `Fetch Ado Work Item    ${id}` — single work item JSON
- `Fetch Ado Comments    ${id}` — merged modern + history comments
- `Extract Ado Relations    ${id}    ${relations}` — normalise relations, download attachments
- `Download Ado Attachment    ${url}    ${id}    ${filename}` — single file
- `Extract All Work Items` — full extraction; writes `output/*.csv`

### ADO -> Jira mapping
- `Load Ado Jira Mapping` — read `output/ado_jira_mapping.json`
- `Save Ado Jira Mapping` — persist current mapping
- `Set Mapping Entry    ${ado_id}    ${jira_key}`
- `Get Mapping Entry    ${ado_id}` — returns Jira key or `None`

### Jira create / write
- `Validate Work Category Field` — pre-flight check on customfield_11503
- `Search Jira User    ${email}` — returns accountId or `None`
- `Create Jira Issue    ${row_dict}` — returns new Jira key
- `Add Jira Comment    ${jira_key}    ${text}    ${author}=    ${date}=`
- `Upload Jira Attachment    ${jira_key}    ${path}`
- `Create Jira Issue Link    ${source}    ${target}    ${category}=related`
- `Set Jira Epic Link    ${child_key}    ${epic_key}`

### Bulk pipelines
- `Import All Work Items` — Step 1; returns `{'created': N, 'failed': N}`
- `Process Jira Comments` — Step 2
- `Process Jira Attachments` — Step 3
- `Process Jira Relations` — Step 4
- `Link All Epics` — runs epic_linker over `output/epics.csv`
- `Update Jira Epic    ${jira_key}    ${row}    ${epic_name_field}=`
- `Update All Jira Epics    ${csv_path}=    ${epic_name_field}=`

## Example

```robotframework
*** Settings ***
Library    MigrationWorkflow.py

*** Test Cases ***
Migrate One Issue
    Load Migration Config
    Open Jira Session
    &{row}=    Create Dictionary    id=12345    type=User Story    title=Demo
    ${key}=    Create Jira Issue    ${row}
    Should Not Be Equal    ${key}    ${NONE}
    Add Jira Comment    ${key}    Migrated from ADO ${row}[id].
```
