"""Robot Framework keyword library for the ADO -> Jira migration workflow.

Wraps the existing migration scripts (extract_workitems_data.py, jira_import.py,
epic_linker.py, jira_update_epics.py) as granular keywords. The underlying
scripts are imported unchanged.

Usage in a .robot file:

    *** Settings ***
    Library    MigrationWorkflow.py

    *** Test Cases ***
    Migrate Single Work Item
        Load Migration Config
        Open Jira Session
        ${data}=    Fetch Ado Work Item    12345
        Should Not Be Empty    ${data}
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from robot.api import logger as robot_logger
from robot.api.deco import keyword, library

import epic_linker
import extract_workitems_data
import jira_import
import jira_update_epics


@library(scope="GLOBAL", version="1.0.0", auto_keywords=False)
class MigrationWorkflow:
    """ADO -> Jira migration keywords.

    Holds an authenticated Jira session, the Jira config dict, and the
    ADO -> Jira key mapping in instance state so each keyword call doesn't
    need to re-authenticate or reload mappings.
    """

    ROBOT_LIBRARY_DOC_FORMAT = "ROBOT"

    def __init__(self, output_dir: str = "output") -> None:
        self._config: dict | None = None
        self._session: Any = None
        self._mapping: dict[str, str] = {}
        self._output_dir = Path(output_dir)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_session(self) -> None:
        if self._session is None or self._config is None:
            raise RuntimeError(
                "Jira session is not open. Call 'Load Migration Config' "
                "and 'Open Jira Session' first."
            )

    def _refresh_ado_globals(self) -> None:
        """Re-read ADO_ORG / ADO_PROJECT / ADO_PAT from the environment and
        push them into the extract_workitems_data module's globals.

        extract_workitems_data sets these at import time, so a later .env
        change or test-time override won't take effect without this refresh.
        """
        import base64

        load_dotenv(override=True)
        org = os.getenv("ADO_ORG", "")
        project = os.getenv("ADO_PROJECT", "")
        pat = os.getenv("ADO_PAT", "")
        extract_workitems_data.ORG = org
        extract_workitems_data.PROJECT = project
        extract_workitems_data.PAT = pat
        if pat:
            auth = base64.b64encode(f":{pat}".encode()).decode()
            extract_workitems_data.HEADERS = {
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/json",
            }

    # ------------------------------------------------------------------
    # Configuration / session
    # ------------------------------------------------------------------

    @keyword("Load Migration Config")
    def load_migration_config(self) -> dict:
        """Load Jira + ADO credentials from .env into memory.

        Returns the Jira config dict (``base_url``, ``email``, ``api_token``,
        ``project_key``). Must be called before any Jira keyword.
        """
        self._config = jira_import.load_env()
        self._refresh_ado_globals()
        robot_logger.info(
            f"Loaded config for Jira project '{self._config['project_key']}'"
        )
        return self._config

    @keyword("Open Jira Session")
    def open_jira_session(self) -> None:
        """Open an authenticated requests.Session against the Jira REST API.

        Requires ``Load Migration Config`` to have been called.
        """
        if self._config is None:
            self.load_migration_config()
        self._session = jira_import._build_session(self._config)
        robot_logger.info("Jira session opened.")

    @keyword("Close Jira Session")
    def close_jira_session(self) -> None:
        """Close the active Jira session and clear cached state."""
        if self._session is not None:
            try:
                self._session.close()
            except Exception:  # noqa: BLE001 - close is best-effort
                pass
        self._session = None
        self._mapping = {}
        robot_logger.info("Jira session closed.")

    # ------------------------------------------------------------------
    # ADO extraction
    # ------------------------------------------------------------------

    @keyword("Fetch Ado Work Item")
    def fetch_ado_work_item(self, work_item_id: int) -> dict | None:
        """Fetch a single ADO work item by ID, returning the raw JSON dict.

        Returns ``None`` if the work item does not exist or the request fails.
        """
        self._refresh_ado_globals()
        return extract_workitems_data.fetch_workitem(int(work_item_id))

    @keyword("Fetch Ado Comments")
    def fetch_ado_comments(self, work_item_id: int) -> list[dict]:
        """Fetch all comments for an ADO work item.

        Merges the modern Comments API and the legacy History API,
        deduplicating by (author, text-prefix).
        """
        self._refresh_ado_globals()
        return extract_workitems_data.fetch_comments(int(work_item_id))

    @keyword("Extract Ado Relations")
    def extract_ado_relations(
        self, work_item_id: int, relations: list[dict]
    ) -> list[dict]:
        """Parse a list of raw ADO relations into normalised records.

        Attachments referenced in ``relations`` are downloaded to
        ``output/attachments/{work_item_id}/`` as a side effect.
        """
        self._refresh_ado_globals()
        return extract_workitems_data.extract_relations(
            int(work_item_id), relations
        )

    @keyword("Download Ado Attachment")
    def download_ado_attachment(
        self, url: str, work_item_id: int, filename: str
    ) -> str | None:
        """Download a single ADO attachment to ``output/attachments/{id}/``.

        Returns the local file path on success, ``None`` on failure.
        """
        self._refresh_ado_globals()
        return extract_workitems_data.download_attachment(
            url, int(work_item_id), filename
        )

    @keyword("Extract All Work Items")
    def extract_all_work_items(self) -> None:
        """Run the full ADO extraction.

        Reads work item IDs from ``workitems.csv`` and writes:

        - ``output/workitems.csv``
        - ``output/comments.csv``
        - ``output/relations.csv``
        - ``output/attachments/{id}/`` (per-item)
        - ``output/epics.csv``
        """
        self._refresh_ado_globals()
        extract_workitems_data.main()

    # ------------------------------------------------------------------
    # Mapping persistence
    # ------------------------------------------------------------------

    @keyword("Load Ado Jira Mapping")
    def load_ado_jira_mapping(self) -> dict[str, str]:
        """Load ``output/ado_jira_mapping.json`` into memory and return it."""
        self._mapping = jira_import.load_mapping()
        return self._mapping

    @keyword("Save Ado Jira Mapping")
    def save_ado_jira_mapping(self) -> None:
        """Persist the in-memory ADO->Jira mapping to disk."""
        jira_import.save_mapping(self._mapping)

    @keyword("Set Mapping Entry")
    def set_mapping_entry(self, ado_id: str, jira_key: str) -> None:
        """Add or overwrite a single ADO->Jira mapping entry in memory.

        Call ``Save Ado Jira Mapping`` to persist.
        """
        self._mapping[str(ado_id)] = str(jira_key)

    @keyword("Get Mapping Entry")
    def get_mapping_entry(self, ado_id: str) -> str | None:
        """Return the Jira key mapped to an ADO ID, or ``None``."""
        return self._mapping.get(str(ado_id))

    # ------------------------------------------------------------------
    # Jira issue creation / updates
    # ------------------------------------------------------------------

    @keyword("Validate Work Category Field")
    def validate_work_category_field(self) -> None:
        """Pre-flight: verify that customfield_11503 exists in this Jira instance."""
        self._require_session()
        jira_import.validate_work_category_field(self._session, self._config)

    @keyword("Search Jira User")
    def search_jira_user(self, email: str) -> str | None:
        """Look up a Jira account ID by email. Returns ``None`` if not found."""
        self._require_session()
        return jira_import.search_user(self._session, self._config, email)

    @keyword("Create Jira Issue")
    def create_jira_issue(self, row: dict) -> str | None:
        """Create a Jira issue from a workitems.csv row (passed as a dict).

        Returns the new Jira issue key on success, ``None`` otherwise.
        Side effect: the ADO->Jira mapping is updated in memory.
        """
        self._require_session()
        series = pd.Series(row)
        key = jira_import.create_issue(self._session, self._config, series)
        if key:
            ado_id = str(row.get("id", "")).strip()
            if ado_id:
                self._mapping[ado_id] = key
        return key

    @keyword("Add Jira Comment")
    def add_jira_comment(
        self,
        jira_key: str,
        comment_text: str,
        created_by: str | None = None,
        created_date: str | None = None,
    ) -> bool:
        """Add a comment to a Jira issue. Returns ``True`` on success."""
        self._require_session()
        return jira_import.add_comment(
            self._session,
            self._config,
            jira_key,
            comment_text,
            created_by,
            created_date,
        )

    @keyword("Upload Jira Attachment")
    def upload_jira_attachment(self, jira_key: str, file_path: str) -> bool:
        """Upload a local file as an attachment to a Jira issue."""
        self._require_session()
        return jira_import.upload_attachment(
            self._session, self._config, jira_key, Path(file_path)
        )

    @keyword("Create Jira Issue Link")
    def create_jira_issue_link(
        self,
        source_key: str,
        target_key: str,
        relation_category: str = "related",
    ) -> bool:
        """Create a 'Relates' link between two Jira issues."""
        self._require_session()
        return jira_import.create_relation(
            self._session, self._config, source_key, target_key, relation_category
        )

    @keyword("Set Jira Epic Link")
    def set_jira_epic_link(self, child_key: str, epic_key: str) -> bool:
        """Set customfield_10600 (Epic Link) on a child issue."""
        self._require_session()
        return epic_linker.set_epic_link(
            self._session, self._config, child_key, epic_key
        )

    # ------------------------------------------------------------------
    # Bulk operations (drive the CSV-based pipelines)
    # ------------------------------------------------------------------

    @keyword("Import All Work Items")
    def import_all_work_items(self) -> dict[str, int]:
        """Step 1 of the migration: create Jira issues for every row in
        ``output/workitems.csv`` that isn't already mapped.

        Updates the in-memory mapping and persists it to disk.
        Returns a summary dict with ``created`` and ``failed`` counts.
        """
        self._require_session()
        df = jira_import._load_csv(
            "workitems.csv",
            [
                "id", "type", "title", "state", "description",
                "created_by_email", "assigned_to_email", "tags",
                "acceptance_criteria", "repro_steps", "system_info",
                "analysis", "proposed_fix",
                "story_points", "target_date",
                "work_category", "product_impacted", "product_category",
                "team_name", "work_subcategory",
            ],
        )
        if not self._mapping:
            self._mapping = jira_import.load_mapping()

        created, failed = 0, 0
        for _, row in df.iterrows():
            ado_id = str(row.get("id", "")).strip()
            if not ado_id:
                failed += 1
                continue
            if ado_id in self._mapping:
                continue
            key = jira_import.create_issue(self._session, self._config, row)
            if key:
                self._mapping[ado_id] = key
                created += 1
            else:
                failed += 1

        jira_import.save_mapping(self._mapping)
        robot_logger.info(f"Issues -- created: {created} | failed: {failed}")
        return {"created": created, "failed": failed}

    @keyword("Process Jira Comments")
    def process_jira_comments(self) -> None:
        """Step 2: add every comment from ``output/comments.csv`` to its Jira issue."""
        self._require_session()
        df = jira_import._load_csv(
            "comments.csv", ["workitem_id", "comment", "created_by", "created_date"]
        )
        if not self._mapping:
            self._mapping = jira_import.load_mapping()
        jira_import.process_comments(self._session, self._config, df, self._mapping)

    @keyword("Process Jira Attachments")
    def process_jira_attachments(self) -> None:
        """Step 3: upload every file under ``output/attachments/`` to its Jira issue."""
        self._require_session()
        if not self._mapping:
            self._mapping = jira_import.load_mapping()
        jira_import.process_attachments(
            self._session,
            self._config,
            self._mapping,
            self._output_dir / "attachments",
        )

    @keyword("Process Jira Relations")
    def process_jira_relations(self) -> None:
        """Step 4: recreate non-parent/non-attachment relations as Jira links."""
        self._require_session()
        df = jira_import._load_csv(
            "relations.csv",
            ["workitem_id", "relation_type", "relation_category", "target", "url"],
        )
        if not self._mapping:
            self._mapping = jira_import.load_mapping()
        jira_import.process_relations(self._session, self._config, df, self._mapping)

    # ------------------------------------------------------------------
    # Epic linking
    # ------------------------------------------------------------------

    @keyword("Link All Epics")
    def link_all_epics(self) -> None:
        """Run the full Epic Link pass over ``output/epics.csv``.

        Reads epics.csv, ado_jira_mapping.json, and epic_data.csv, then sets
        customfield_10600 on each child issue.
        """
        self._require_session()
        epics_df = epic_linker.load_epics_csv()
        ado_jira_mapping = epic_linker.load_ado_jira_mapping()
        epic_data_mapping = epic_linker.load_epic_data_csv()
        epic_linker.run(
            self._session,
            self._config,
            epics_df,
            ado_jira_mapping,
            epic_data_mapping,
        )

    # ------------------------------------------------------------------
    # Epic updates (post-migration field sync)
    # ------------------------------------------------------------------

    @keyword("Update Jira Epic")
    def update_jira_epic(
        self,
        jira_key: str,
        row: dict,
        epic_name_field: str | None = None,
    ) -> bool:
        """Update a single Jira Epic with field data from an ADO row dict."""
        self._require_session()
        series = pd.Series(row)
        return jira_update_epics.update_epic(
            self._session, self._config, jira_key, series, epic_name_field
        )

    @keyword("Update All Jira Epics")
    def update_all_jira_epics(
        self,
        csv_path: str | None = None,
        epic_name_field: str | None = None,
    ) -> None:
        """Run the full Epic field-sync pass over a workitems CSV.

        Defaults to ``output/workitems.csv`` (must contain a ``jira_id`` column).
        """
        self._require_session()
        path = csv_path or str(self._output_dir / "workitems.csv")
        jira_update_epics.process_csv(
            self._session, self._config, path, epic_name_field
        )
