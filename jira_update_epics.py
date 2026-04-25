"""
Jira Epic Updater — Post-Migration Field Sync
Reads workitems.csv (with a jira_id column) and updates existing Jira Epics
with field data sourced from ADO.

This script is designed to run AFTER the initial migration (jira_import.py).
Epics already exist in Jira with correct summaries; this script fills in
missing fields: description, labels, story points, dates, team name,
work category, etc.

Idempotent: safe to re-run — only PUTs field values (no duplicates created).

Usage:
    python jira_update_epics.py
    python jira_update_epics.py --csv output/workitems.csv
    python jira_update_epics.py --csv output/workitems.csv --epic-name-field customfield_10601
"""

import argparse
import logging
import os
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Import reusable utilities from the existing migration script.
# jira_import.py is NOT modified — we only read from it.
from jira_import import (
    _build_session,
    _api_url,
    _safe_request,
    _build_labels,
    _safe_float,
    _safe_date,
    _is_empty,
    _build_ado_url,
    _heading_node,
    _fetch_field_options_via_createmeta,
    _resolve_work_category,
    _resolve_team_names,
    _resolve_single_option,
    _resolve_array_options,
    _TEAM_NAME_FIELD_ID,
    _CLASS_FIELD_ID,
    _APPLICATION_FIELD_ID,
    _VERTICALS_FIELD_ID,
    convert_html_to_markdown,
    _markdown_to_adf_nodes,
    extract_and_upload_images,
    replace_image_urls,
    add_comment,
    upload_attachment,
    API_DELAY,
    OUTPUT_DIR,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — Jira custom field IDs
# ---------------------------------------------------------------------------

STORY_POINTS_FIELD = "customfield_10003"
TARGET_END_FIELD = "customfield_17401"
TEAM_NAME_FIELD = _TEAM_NAME_FIELD_ID       # customfield_20107 — array/option
WORK_CATEGORY_FIELD = "customfield_11503"    # Work Category — single-select option
CLASS_FIELD = _CLASS_FIELD_ID                # customfield_22607 — Work SubCategory → Class
APPLICATION_FIELD = _APPLICATION_FIELD_ID    # customfield_18255 — Product Impacted → Application
VERTICALS_FIELD = _VERTICALS_FIELD_ID        # customfield_20405 — Product Category → Vertical(s)
CATEGORY_OF_WORK_FIELD = "customfield_11106"  # Category → Category of Work (option)
EXTERNAL_ISSUE_ID_FIELD = "customfield_12100" # External Issue ID (string — ADO URL)
PARENT_LINK_FIELD = "customfield_15200"       # Parent Link → Mission Link

# Acceptance Criteria — Epics may have this field available
ACCEPTANCE_CRITERIA_FIELD = "customfield_13802"

# Default component names — matches extract_workitems_data.py ("ADOImported,UWP")
# Used as fallback when the CSV 'component' column is empty.
DEFAULT_COMPONENTS = os.getenv("JIRA_COMPONENTS", "ADOImported,UWP")

# Default CSV path — points to the extraction output which has full ADO field data
DEFAULT_CSV = os.path.join("output", "workitems.csv")

# HTML fields on Epics that may contain embedded <img> tags.
# Used by _process_epic_images() to download from ADO and re-upload to Jira.
EPIC_HTML_FIELDS = ["description", "analysis", "acceptance_criteria"]


# ---------------------------------------------------------------------------
# Environment / configuration
# ---------------------------------------------------------------------------


def load_config() -> dict:
    """Load Jira credentials from .env and return a config dict."""
    load_dotenv()

    config = {
        "base_url": os.getenv("JIRA_BASE_URL", "").rstrip("/"),
        "email": os.getenv("JIRA_EMAIL", ""),
        "api_token": os.getenv("JIRA_API_TOKEN", ""),
        "project_key": os.getenv("JIRA_PROJECT_KEY", ""),
    }

    missing = [k for k, v in config.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    logger.info("Loaded Jira config — project: %s", config["project_key"])
    return config


# ---------------------------------------------------------------------------
# Epic-specific description builder
# ---------------------------------------------------------------------------
# build_description_adf() in jira_import.py treats Epics as "Task" (else branch)
# and only includes the description field.  Epics should include description
# (body) and analysis sections.  Acceptance Criteria is written exclusively
# to its own custom field (customfield_13802), not duplicated in description.


def _build_epic_description_adf(row: pd.Series) -> dict | None:
    """Build an ADF description for an Epic.

    Sections included (when non-empty):
      1. Description   — main body (no heading)
      2. Analysis       — under "Analysis" heading

    Acceptance Criteria is written to its own custom field
    (customfield_13802), not included in the description.

    Mirrors the pattern used by build_description_adf() in jira_import.py
    for Story/Bug types, using the same HTML→Markdown→ADF pipeline.
    """
    content: list[dict] = []

    def _add_section(title: str, value) -> None:
        if not value or str(value).strip() in ("", "nan"):
            return
        md = convert_html_to_markdown(str(value))
        if not md:
            return
        content.append(_heading_node(title))
        content.extend(_markdown_to_adf_nodes(md))

    def _add_base(value) -> None:
        if not value or str(value).strip() in ("", "nan"):
            return
        md = convert_html_to_markdown(str(value))
        if md:
            content.extend(_markdown_to_adf_nodes(md))

    # Main body — description field (no heading)
    _add_base(row.get("description"))

    # Additional sections under headings
    _add_section("Analysis", row.get("analysis"))

    if not content:
        return None

    return {"version": 1, "type": "doc", "content": content}


# ---------------------------------------------------------------------------
# Image processing — same pattern as jira_import._process_issue_images()
# ---------------------------------------------------------------------------


def _process_epic_images(
    session,
    config: dict,
    jira_key: str,
    row: pd.Series,
) -> None:
    """Upload images embedded in ADO HTML fields and update the Epic description.

    For each HTML field that may contain <img> tags:
      1. Download the image (using ADO auth if hosted on dev.azure.com)
      2. Upload it as a Jira attachment
      3. Patch the HTML so old src URLs point to the new Jira content URLs
      4. Rebuild the description ADF and update the issue via PUT

    Mirrors _process_issue_images() from jira_import.py but uses the
    Epic-specific description builder and field list.
    """
    # Collect a combined old→new URL map across all relevant HTML fields
    all_url_map: dict[str, str] = {}
    for field_name in EPIC_HTML_FIELDS:
        html = str(row.get(field_name, "") or "")
        if html.strip() in ("", "nan"):
            continue
        url_map = extract_and_upload_images(html, jira_key, session, config)
        all_url_map.update(url_map)

    if not all_url_map:
        return  # No images found or all uploads failed

    logger.info(
        "Updating %s: replacing %d image URL(s) with Jira attachment URLs",
        jira_key, len(all_url_map),
    )

    # Patch each HTML field in the row, then rebuild ADF
    patched = row.copy()
    for field_name in EPIC_HTML_FIELDS:
        val = str(patched.get(field_name, "") or "")
        if val.strip() not in ("", "nan"):
            patched[field_name] = replace_image_urls(val, all_url_map)

    fields_update: dict = {}

    # Rebuild description with patched image URLs
    new_desc = _build_epic_description_adf(patched)
    if new_desc:
        fields_update["description"] = new_desc

    # Rebuild acceptance criteria with patched image URLs (rich text ADF)
    ac_raw = str(patched.get("acceptance_criteria", "") or "")
    if ac_raw.strip() not in ("", "nan"):
        ac_nodes = _markdown_to_adf_nodes(convert_html_to_markdown(ac_raw))
        if ac_nodes:
            fields_update[ACCEPTANCE_CRITERIA_FIELD] = {
                "version": 1, "type": "doc", "content": ac_nodes,
            }

    if not fields_update:
        return

    url = _api_url(config, f"issue/{jira_key}")
    put_resp = _safe_request(
        session,
        "PUT",
        url,
        f"update image URLs in description for {jira_key}",
        json={"fields": fields_update},
        headers={"Content-Type": "application/json"},
    )
    if put_resp is not None:
        logger.info(
            "Image fields updated for %s — fields: %s",
            jira_key, list(fields_update.keys()),
        )
    else:
        logger.warning(
            "Failed to update image fields for %s — images may not render inline. "
            "Fields attempted: %s",
            jira_key, list(fields_update.keys()),
        )


# ---------------------------------------------------------------------------
# Core update function
# ---------------------------------------------------------------------------


def update_epic(
    session,
    config: dict,
    jira_key: str,
    row: pd.Series,
    epic_name_field: str | None = None,
) -> bool:
    """Update a single Jira Epic with field data from an ADO work item row.

    Calls PUT /rest/api/3/issue/{jira_key} with only the fields that have
    data in the CSV row.  System fields (project, issuetype, summary) are
    never touched.

    After the main PUT, processes embedded images (downloads from ADO,
    uploads to Jira, patches URLs) — same pattern as jira_import.py.

    Returns True on success, False on failure.
    """
    ado_id = str(row.get("id", "")).strip()
    fields_to_update: dict = {}

    # ── Description (ADF) ────────────────────────────────────────────────
    # Uses the Epic-specific builder that includes description + analysis +
    # acceptance criteria sections (jira_import.py's build_description_adf
    # only uses description for non-Story/Bug types).
    description_adf = _build_epic_description_adf(row)
    if description_adf:
        fields_to_update["description"] = description_adf

    # ── Acceptance Criteria (rich text ADF — customfield_13802) ──
    ac_raw = str(row.get("acceptance_criteria", "") or "")
    if not _is_empty(ac_raw):
        ac_nodes = _markdown_to_adf_nodes(convert_html_to_markdown(ac_raw))
        if ac_nodes:
            fields_to_update[ACCEPTANCE_CRITERIA_FIELD] = {
                "version": 1, "type": "doc", "content": ac_nodes,
            }

    # ── Labels ────────────────────────────────────────────────────────────
    labels = _build_labels(row)
    if labels:
        fields_to_update["labels"] = labels

    # ── Story Points ─────────────────────────────────────────────────────
    sp = _safe_float(row.get("story_points"))
    if sp is not None:
        fields_to_update[STORY_POINTS_FIELD] = sp

    # ── Target Date → duedate + customfield_17401 ────────────────────────
    due = _safe_date(row.get("target_date"))
    if due:
        fields_to_update["duedate"] = due
        fields_to_update[TARGET_END_FIELD] = due

    # ── Team Name (customfield_20107 — array/option, semicolon-separated) ─
    team_name_raw = str(row.get("team_name", "") or "").strip()
    if not _is_empty(team_name_raw):
        allowed_teams = _fetch_field_options_via_createmeta(
            session, config, TEAM_NAME_FIELD
        )
        resolved_teams = _resolve_team_names(team_name_raw, allowed_teams)
        if resolved_teams:
            fields_to_update[TEAM_NAME_FIELD] = resolved_teams
        else:
            logger.warning(
                "Team name(s) skipped for %s (ADO %s) — no valid values from '%s'.",
                jira_key, ado_id, team_name_raw,
            )

    # ── Work Category (customfield_11503 — single-select option) ─────────
    work_category_raw = str(row.get("work_category", "") or "").strip()
    if not _is_empty(work_category_raw):
        allowed_wc = _fetch_field_options_via_createmeta(
            session, config, WORK_CATEGORY_FIELD
        )
        resolved_wc = _resolve_work_category(work_category_raw, allowed_wc)
        if resolved_wc:
            fields_to_update[WORK_CATEGORY_FIELD] = {"value": resolved_wc}
        else:
            logger.warning(
                "Work Category skipped for %s (ADO %s) — '%s' has no valid Jira match.",
                jira_key, ado_id, work_category_raw,
            )

    # ── Category of Work (customfield_11106 — single-select option) ──────
    category_raw = str(row.get("category", "") or "").strip()
    if not _is_empty(category_raw):
        allowed_cow = _fetch_field_options_via_createmeta(
            session, config, CATEGORY_OF_WORK_FIELD
        )
        resolved_cow = _resolve_single_option(
            category_raw, allowed_cow, "Category of Work"
        )
        if resolved_cow:
            fields_to_update[CATEGORY_OF_WORK_FIELD] = {"value": resolved_cow}

    # ── Class (customfield_22607 — single-select option) ← Work SubCategory
    work_subcategory_raw = str(row.get("work_subcategory", "") or "").strip()
    if not _is_empty(work_subcategory_raw):
        allowed_class = _fetch_field_options_via_createmeta(
            session, config, CLASS_FIELD
        )
        resolved_class = _resolve_single_option(
            work_subcategory_raw, allowed_class, "Class"
        )
        if resolved_class:
            fields_to_update[CLASS_FIELD] = {"value": resolved_class}

    # ── Application (customfield_18255 — array/option) ← Product Impacted ─
    product_impacted_raw = str(row.get("product_impacted", "") or "").strip()
    if not _is_empty(product_impacted_raw):
        allowed_app = _fetch_field_options_via_createmeta(
            session, config, APPLICATION_FIELD
        )
        resolved_apps = _resolve_array_options(
            product_impacted_raw, allowed_app, "Application"
        )
        if resolved_apps:
            fields_to_update[APPLICATION_FIELD] = resolved_apps

    # ── Vertical(s) (customfield_20405 — array/option) ← Product Category ─
    product_category_raw = str(row.get("product_category", "") or "").strip()
    if not _is_empty(product_category_raw):
        allowed_vert = _fetch_field_options_via_createmeta(
            session, config, VERTICALS_FIELD
        )
        resolved_verts = _resolve_array_options(
            product_category_raw, allowed_vert, "Vertical(s)"
        )
        if resolved_verts:
            fields_to_update[VERTICALS_FIELD] = resolved_verts

    # ── External Issue ID (customfield_12100 — string, ADO URL) ──────────
    ext_id = str(row.get("external_issue_id", "") or "").strip()
    if not _is_empty(ext_id):
        fields_to_update[EXTERNAL_ISSUE_ID_FIELD] = ext_id
    elif not _is_empty(ado_id):
        fields_to_update[EXTERNAL_ISSUE_ID_FIELD] = _build_ado_url(ado_id)

    # ── Components ────────────────────────────────────────────────────────
    component_raw = str(row.get("component", "") or "").strip()
    if _is_empty(component_raw):
        component_raw = DEFAULT_COMPONENTS
    component_names = [c.strip() for c in component_raw.split(",") if c.strip()]
    if component_names:
        fields_to_update["components"] = [{"name": c} for c in component_names]

    # ── Epic Name (optional) ─────────────────────────────────────────────
    if epic_name_field:
        title = str(row.get("title", "") or "").strip()
        if not _is_empty(title):
            fields_to_update[epic_name_field] = title

    # ── Nothing to update? ───────────────────────────────────────────────
    if not fields_to_update:
        logger.info(
            "No updatable fields for %s (ADO %s) — skipping.",
            jira_key, ado_id,
        )
        return True  # Not a failure; just nothing to do

    # ── Send the PUT request ─────────────────────────────────────────────
    url = _api_url(config, f"issue/{jira_key}")
    resp = _safe_request(
        session,
        "PUT",
        url,
        f"update Epic {jira_key} (ADO {ado_id})",
        json={"fields": fields_to_update},
        headers={"Content-Type": "application/json"},
    )

    if resp is not None:
        logger.info(
            "Updated %s (ADO %s) — fields: %s",
            jira_key, ado_id, list(fields_to_update.keys()),
        )

        # Upload embedded images and patch description with Jira attachment URLs.
        # Same pattern as jira_import.py's create_issue → _process_issue_images.
        _process_epic_images(session, config, jira_key, row)

        return True

    logger.error(
        "Failed to update %s (ADO %s) — fields attempted: %s",
        jira_key, ado_id, list(fields_to_update.keys()),
    )
    return False


# ---------------------------------------------------------------------------
# Comments processing
# ---------------------------------------------------------------------------


def process_epic_comments(
    session,
    config: dict,
    mapping: dict[str, str],
) -> None:
    """Load output/comments.csv and add comments to mapped Epics.

    Uses the same add_comment() function from jira_import.py which handles
    ADF formatting, author/date headers, and image placeholders.

    Args:
        mapping: ADO ID (str) → Jira key (str) for Epics being updated.
    """
    comments_path = Path(OUTPUT_DIR) / "comments.csv"
    if not comments_path.exists():
        logger.info("No comments.csv found at %s — skipping comments.", comments_path)
        return

    try:
        comments_df = pd.read_csv(comments_path, dtype=str)
    except Exception as exc:
        logger.error("Failed to read comments.csv: %s", exc)
        return

    comments_df.columns = comments_df.columns.str.strip().str.lower()

    if comments_df.empty:
        logger.info("comments.csv is empty — no comments to process.")
        return

    total = 0
    added = 0
    skipped_no_mapping = 0
    skipped_empty = 0
    failed_api = 0

    for _, row in comments_df.iterrows():
        ado_id = str(row.get("workitem_id", "")).strip()
        jira_key = mapping.get(ado_id)

        if not jira_key:
            # Not an Epic we're updating — skip silently
            skipped_no_mapping += 1
            continue

        total += 1
        comment_text = str(row.get("comment", "") or "")
        if _is_empty(comment_text):
            skipped_empty += 1
            continue

        success = add_comment(
            session,
            config,
            jira_key,
            comment_text,
            str(row.get("created_by", "") or ""),
            str(row.get("created_date", "") or ""),
        )
        if success:
            added += 1
        else:
            failed_api += 1

    logger.info("=" * 60)
    logger.info("EPIC COMMENTS SUMMARY")
    logger.info("  Relevant comments    : %d", total)
    logger.info("  Successfully added   : %d", added)
    logger.info("  Skipped (empty text) : %d", skipped_empty)
    logger.info("  Skipped (no mapping) : %d", skipped_no_mapping)
    logger.info("  Failed (API error)   : %d", failed_api)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Attachments processing
# ---------------------------------------------------------------------------


def process_epic_attachments(
    session,
    config: dict,
    mapping: dict[str, str],
) -> None:
    """Upload attachments from output/attachments/ for mapped Epics.

    Uses the same upload_attachment() function from jira_import.py.
    Only processes folders whose ADO ID is in the mapping dict.

    Args:
        mapping: ADO ID (str) → Jira key (str) for Epics being updated.
    """
    attachment_base = Path(OUTPUT_DIR) / "attachments"
    if not attachment_base.exists():
        logger.info("No attachments directory found at %s — skipping.", attachment_base)
        return

    total = 0
    failed = 0

    for ado_folder in sorted(attachment_base.iterdir()):
        if not ado_folder.is_dir():
            continue

        ado_id = ado_folder.name
        jira_key = mapping.get(ado_id)

        if not jira_key:
            # Not an Epic we're updating — skip
            continue

        for file_path in sorted(ado_folder.iterdir()):
            if file_path.is_file():
                success = upload_attachment(session, config, jira_key, file_path)
                if success:
                    total += 1
                else:
                    failed += 1

    logger.info("=" * 60)
    logger.info("EPIC ATTACHMENTS SUMMARY")
    logger.info("  Uploaded             : %d", total)
    logger.info("  Failed               : %d", failed)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CSV processing — main orchestration
# ---------------------------------------------------------------------------


def process_csv(
    session,
    config: dict,
    csv_path: str,
    epic_name_field: str | None = None,
) -> None:
    """Read the workitems CSV and update each Epic that has a jira_id.

    Steps:
      1. Update all issue fields (description, labels, team name, etc.)
      2. Add comments from output/comments.csv
      3. Upload attachments from output/attachments/
    """
    try:
        df = pd.read_csv(csv_path, dtype=str)
    except Exception as exc:
        logger.error("Failed to read CSV '%s': %s", csv_path, exc)
        return

    df.columns = df.columns.str.strip().str.lower()

    # Validate that jira_id column exists
    if "jira_id" not in df.columns:
        logger.error(
            "CSV '%s' does not contain a 'jira_id' column. "
            "Expected columns include: %s",
            csv_path, list(df.columns),
        )
        return

    # ── Step 1: Update issue fields ──────────────────────────────────────
    total = len(df)
    updated = 0
    skipped = 0
    failed = 0

    # Build ADO ID → Jira key mapping for comments and attachments
    epic_mapping: dict[str, str] = {}

    logger.info("=" * 60)
    logger.info("STEP 1 — Updating Epic fields (%d rows from %s)", total, csv_path)
    logger.info("=" * 60)

    for idx, row in df.iterrows():
        ado_id = str(row.get("id", "")).strip()
        jira_key = str(row.get("jira_id", "")).strip()

        # Skip rows without a Jira key
        if _is_empty(jira_key):
            logger.debug(
                "Row %d (ADO %s) — no jira_id, skipping.", idx, ado_id
            )
            skipped += 1
            continue

        # Track the mapping for comments/attachments steps
        if not _is_empty(ado_id):
            epic_mapping[ado_id] = jira_key

        success = update_epic(session, config, jira_key, row, epic_name_field)
        if success:
            updated += 1
        else:
            failed += 1

        # Small delay for rate-limit safety (on top of _safe_request's delay)
        time.sleep(API_DELAY)

    logger.info("=" * 60)
    logger.info("FIELD UPDATE SUMMARY")
    logger.info("  Total rows           : %d", total)
    logger.info("  Updated successfully : %d", updated)
    logger.info("  Skipped (no jira_id) : %d", skipped)
    logger.info("  Failed               : %d", failed)
    logger.info("=" * 60)

    if not epic_mapping:
        logger.warning("No Epic mappings found — skipping comments and attachments.")
        return

    # ── Step 2: Add comments ─────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 2 — Adding comments for %d Epic(s)", len(epic_mapping))
    logger.info("=" * 60)
    process_epic_comments(session, config, epic_mapping)

    # ── Step 3: Upload attachments ───────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 3 — Uploading attachments for %d Epic(s)", len(epic_mapping))
    logger.info("=" * 60)
    process_epic_attachments(session, config, epic_mapping)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update existing Jira Epics with ADO field data.",
    )
    parser.add_argument(
        "--csv",
        default=DEFAULT_CSV,
        help="Path to the workitems CSV file (must include a 'jira_id' column). "
             f"Default: {DEFAULT_CSV}",
    )
    parser.add_argument(
        "--epic-name-field",
        default=None,
        help="Jira custom field ID for Epic Name (e.g. customfield_10601). "
             "If set, the ADO title will be written to this field.",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point — load config, build session, process CSV."""
    args = parse_args()

    config = load_config()
    session = _build_session(config)

    logger.info("Epic Update Script — CSV: %s", args.csv)
    if args.epic_name_field:
        logger.info("Epic Name field: %s", args.epic_name_field)

    process_csv(session, config, args.csv, args.epic_name_field)

    logger.info("Done.")


if __name__ == "__main__":
    main()
