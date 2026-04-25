"""
Azure DevOps Work Item Extractor
Extracts work items, comments, relations, and attachments for Jira migration.
"""

import json
import logging
import os
import base64
import re
import time
from html.parser import HTMLParser

import requests
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

ORG = os.getenv("ADO_ORG")
PROJECT = os.getenv("ADO_PROJECT")
PAT = os.getenv("ADO_PAT")

auth = base64.b64encode(f":{PAT}".encode()).decode()

HEADERS = {
    "Authorization": f"Basic {auth}",
    "Content-Type": "application/json",
}

API_VERSION = "7.0"

OUTPUT_DIR = "output"
ATTACHMENT_DIR = os.path.join(OUTPUT_DIR, "attachments")

# Delay between API requests to avoid rate limiting
API_DELAY = 0.2

# Default team name written to workitems.csv when the ADO work item has no team name.
# Change this value to switch the default team.
DEFAULT_TEAM_NAME = "Racoons"

# Label always appended to every work item's labels column in the output CSV.
# Change this value to switch the default label.
DEFAULT_LABEL = "US"

# Relation type → simplified category mapping
RELATION_CATEGORY_MAP = {
    "System.LinkTypes.Hierarchy-Reverse": "parent",
    "System.LinkTypes.Hierarchy-Forward": "child",
    "System.LinkTypes.Related": "related",
    "AttachedFile": "attachment",
    "ArtifactLink": "artifact",
}

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
# Helpers
# ---------------------------------------------------------------------------


def _split_acceptance_criteria(description_html: str) -> tuple[str, str]:
    """Split an ADO description HTML into (description, acceptance_criteria).

    Epics in ADO often have no separate Acceptance Criteria field —
    instead the AC is written inline in the description under a heading
    like ``<b>Acceptance Criteria</b>`` or ``Acceptance Criteria for the EPIC``.

    This function looks for a ``<div>`` containing "Acceptance Criteria"
    (case-insensitive) and splits the HTML there.  Everything before becomes
    the description; the heading ``<div>`` and everything after become
    the acceptance_criteria.

    Returns (description, acceptance_criteria).  If no AC heading is found,
    acceptance_criteria is an empty string.
    """
    if not description_html or not isinstance(description_html, str):
        return (description_html or ""), ""

    # Match a <div> whose visible text starts with "Acceptance Criteria"
    # The heading text may be wrapped in <b> tags or be plain text.
    pattern = re.compile(
        r'<div[^>]*>\s*(?:<b[^>]*>)?\s*Acceptance\s+Criteria\b',
        re.IGNORECASE,
    )
    match = pattern.search(description_html)
    if not match:
        return description_html, ""

    split_pos = match.start()
    desc_part = description_html[:split_pos].strip()
    ac_part = description_html[split_pos:].strip()

    return desc_part, ac_part


def _html_to_text(raw: str) -> str:
    """Convert an HTML string to clean plain text, preserving images.

    Block tags become newlines; table cells are separated by ' | '.
    HTML entities (e.g. &nbsp;) are decoded automatically.
    <img> tags are preserved as [Image: URL] placeholders.
    """
    if not raw or not isinstance(raw, str):
        return raw or ""

    class _Converter(HTMLParser):
        BLOCK_TAGS = {"div", "p", "br", "li", "h1", "h2", "h3",
                      "h4", "h5", "h6", "blockquote", "pre", "tr"}
        CELL_TAGS  = {"td", "th"}

        def __init__(self):
            super().__init__(convert_charrefs=True)
            self._parts: list[str] = []

        def handle_starttag(self, tag, attrs):
            t = tag.lower()
            if t == "img":
                attrs_dict = dict(attrs)
                src = attrs_dict.get("src", "")
                if src:
                    self._parts.append(f"\n[Image: {src}]\n")
            elif t in self.BLOCK_TAGS:
                self._parts.append("\n")
            elif t in self.CELL_TAGS:
                self._parts.append(" | ")

        def handle_endtag(self, tag):
            if tag.lower() in self.BLOCK_TAGS:
                self._parts.append("\n")

        def handle_data(self, data):
            self._parts.append(data)

        def get_text(self) -> str:
            text = "".join(self._parts)
            # Strip leading/trailing ' | ' noise on each line
            lines = [line.strip().strip("|").strip() for line in text.splitlines()]
            # Drop blank lines that are entirely whitespace
            lines = [ln for ln in lines if ln]
            return "\n".join(lines)

    converter = _Converter()
    converter.feed(raw)
    return converter.get_text()


def sanitize_filename(filename: str) -> str:
    """Remove filesystem-unsafe characters and normalise a filename.

    - Strips characters invalid on Windows/Linux/macOS file systems.
    - Replaces spaces with underscores.
    - Preserves the file extension.
    """
    name, ext = os.path.splitext(filename)
    name = re.sub(r'[\\/:*?"<>|]', "", name)
    name = name.replace(" ", "_")
    name = re.sub(r"_+", "_", name).strip("_")
    return f"{name}{ext}" if name else f"attachment{ext}"


def _extract_identity(identity_field: object) -> tuple[str | None, str | None]:
    """Extract display name and email from an Azure DevOps identity object.

    Azure DevOps returns identity fields as dicts with ``displayName`` and
    ``uniqueName`` keys.  When the field is absent or not a dict the function
    returns ``(None, None)``.
    """
    if not isinstance(identity_field, dict):
        return None, None
    return identity_field.get("displayName"), identity_field.get("uniqueName")


# ---------------------------------------------------------------------------
# API functions
# ---------------------------------------------------------------------------


def fetch_workitem(work_item_id: int) -> dict | None:
    """Fetch a single work item from Azure DevOps, expanding all fields."""
    url = (
        f"https://dev.azure.com/{ORG}/_apis/wit/workitems/{work_item_id}"
        f"?$expand=all&api-version={API_VERSION}"
    )
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        time.sleep(API_DELAY)

        if response.status_code != 200:
            logger.warning(
                "Failed to fetch work item %s — HTTP %s", work_item_id, response.status_code
            )
            return None

        return response.json()

    except requests.RequestException as exc:
        logger.error("Request error fetching work item %s: %s", work_item_id, exc)
        return None


def _fetch_comments_modern(work_item_id: int) -> list[dict]:
    """Fetch comments using the modern Comments API (7.0-preview).

    This is the primary source — returns all discussion comments.
    """
    url = (
        f"https://dev.azure.com/{ORG}/_apis/wit/workItems/{work_item_id}"
        f"/comments?api-version=7.0-preview"
    )
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        time.sleep(API_DELAY)

        if response.status_code != 200:
            logger.warning(
                "Modern comments API failed for work item %s — HTTP %s",
                work_item_id, response.status_code,
            )
            return []

        raw_comments = response.json().get("comments", [])

    except requests.RequestException as exc:
        logger.error("Request error (modern comments) for work item %s: %s", work_item_id, exc)
        return []

    comments = []
    for c in raw_comments:
        text = _html_to_text(c.get("text") or "")
        if not text:
            continue
        comments.append({
            "comment": text,
            "created_by": (c.get("createdBy") or {}).get("displayName"),
            "created_date": c.get("createdDate"),
        })

    return comments


def _fetch_comments_history(work_item_id: int) -> list[dict]:
    """Fetch comments from the Updates/System.History API (legacy fallback).

    Some older comments or inline history entries only appear here.
    """
    url = (
        f"https://dev.azure.com/{ORG}/_apis/wit/workItems/{work_item_id}"
        f"/updates?api-version={API_VERSION}"
    )
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        time.sleep(API_DELAY)

        if response.status_code != 200:
            logger.warning(
                "History comments fetch failed for work item %s — HTTP %s",
                work_item_id, response.status_code,
            )
            return []

        updates = response.json().get("value", [])

    except requests.RequestException as exc:
        logger.error("Request error (history comments) for work item %s: %s", work_item_id, exc)
        return []

    comments = []
    for update in updates:
        fields = update.get("fields", {})
        history = fields.get("System.History")
        if history:
            text = _html_to_text(history.get("newValue") or "")
            if not text:
                continue
            comments.append({
                "comment": text,
                "created_by": update.get("revisedBy", {}).get("displayName"),
                "created_date": update.get("revisedDate"),
            })

    return comments


def fetch_comments(work_item_id: int) -> list[dict]:
    """Fetch ALL comments by merging the modern Comments API and the legacy History API.

    Deduplication is done by normalising comment text and matching on a
    (author, trimmed-text-prefix) key to avoid duplicates from the two sources.
    """
    modern = _fetch_comments_modern(work_item_id)
    history = _fetch_comments_history(work_item_id)

    # Build dedup set from modern comments (primary source)
    seen: set[str] = set()
    for c in modern:
        key = (c.get("created_by") or "") + "|" + (c.get("comment") or "")[:120]
        seen.add(key)

    # Merge history comments that are not already present
    merged_from_history = 0
    for c in history:
        key = (c.get("created_by") or "") + "|" + (c.get("comment") or "")[:120]
        if key not in seen:
            modern.append(c)
            seen.add(key)
            merged_from_history += 1

    logger.info(
        "Extracted %d comment(s) for work item %s (modern: %d, added from history: %d)",
        len(modern), work_item_id, len(modern) - merged_from_history, merged_from_history,
    )
    return modern


def download_attachment(url: str, workitem_id: int, filename: str) -> str | None:
    """Download a single attachment and save it to the per-work-item folder."""
    safe_name = sanitize_filename(filename)
    folder = os.path.join(ATTACHMENT_DIR, str(workitem_id))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, safe_name)

    try:
        r = requests.get(url, headers=HEADERS, stream=True, timeout=60)
        time.sleep(API_DELAY)

        if r.status_code == 200:
            with open(path, "wb") as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
            logger.info("Downloaded attachment: %s → %s", filename, path)
            return path
        else:
            logger.warning(
                "Failed to download attachment '%s' — HTTP %s", filename, r.status_code
            )
            return None

    except (requests.RequestException, OSError) as exc:
        logger.error("Error downloading attachment '%s': %s", filename, exc)
        return None


def extract_relations(work_item_id: int, relations: list[dict]) -> list[dict]:
    """Parse the relations list from a work item into a normalised structure.

    Each relation record includes a ``relation_category`` mapped from the raw
    Azure DevOps relation type string.  Attachments are identified here and
    downloaded automatically.

    Added columns (vs original):
      - relation_label  "EPIC" for parent, "CHILD" for child, uppercase category otherwise
      - ado_url         ADO URL for the target work item (non-attachment relations only)
      - ticket_id       Source work item ID (same as workitem_id, for traceability)
      - ticket_url      ADO URL for the source work item
    """
    records = []

    ticket_url = f"https://dev.azure.com/{ORG}/{PROJECT}/_workitems/edit/{work_item_id}"

    for rel in relations:
        rel_type = rel.get("rel")
        url = rel.get("url", "")
        relation_category = RELATION_CATEGORY_MAP.get(rel_type, "other")

        target_id = None
        if "workItems" in url:
            target_id = url.split("/")[-1]

        # Derive human-readable relation label
        if relation_category == "parent":
            relation_label = "EPIC"
        elif relation_category == "child":
            relation_label = "CHILD"
        else:
            relation_label = relation_category.upper()

        # ADO URL for the target — only for non-attachment relations with a known target ID
        is_attachment = rel_type == "AttachedFile"
        ado_url = None
        if not is_attachment and target_id:
            ado_url = (
                f"https://dev.azure.com/{ORG}/{PROJECT}/_workitems/edit/{target_id}"
            )

        records.append(
            {
                "workitem_id": work_item_id,
                "relation_type": rel_type,
                "relation_category": relation_category,
                "relation_label": relation_label,
                "target": target_id,
                "url": url,
                "ado_url": ado_url,
                "ticket_id": work_item_id,
                "ticket_url": ticket_url,
            }
        )

        if is_attachment:
            attributes = rel.get("attributes", {})
            filename = attributes.get("name") or url.split("/")[-1]
            download_attachment(url, work_item_id, filename)

    logger.info(
        "Extracted %d relation(s) for work item %s", len(records), work_item_id
    )
    return records


# ---------------------------------------------------------------------------
# Epics CSV generator
# ---------------------------------------------------------------------------


def _generate_epics_csv(workitems_data: list[dict], relations_data: list[dict]) -> None:
    """Build and write epics.csv — a clean Epic ↔ Ticket mapping.

    Filters relations where ``relation_label == "EPIC"`` (i.e. the work item
    has a parent/Epic link), then enriches each row with:
      - Epic and ticket titles (from workitems_data)
      - Jira IDs and URLs (from output/ado_jira_mapping.json, if present)

    Columns produced:
      epic_id, epic_title, epic_ado_url,
      ticket_id, ticket_title, ticket_ado_url,
      jira_epic_id, jira_epic_url,
      jira_ticket_id, jira_ticket_url
    """
    # Build title lookup: ADO ID (str) → title
    title_lookup: dict[str, str] = {
        str(wi.get("id", "")): str(wi.get("title", "") or "")
        for wi in workitems_data
    }

    # Load Jira mapping if available (ADO ID str → Jira issue key str)
    jira_mapping: dict[str, str] = {}
    mapping_path = os.path.join(OUTPUT_DIR, "ado_jira_mapping.json")
    if os.path.exists(mapping_path):
        try:
            with open(mapping_path, "r", encoding="utf-8") as f:
                jira_mapping = json.load(f)
            logger.info("Loaded ado_jira_mapping.json (%d entries)", len(jira_mapping))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not load ado_jira_mapping.json: %s", exc)

    jira_base = "https://libertymutual.atlassian.net/browse"

    epic_rows = []
    for rel in relations_data:
        if rel.get("relation_label") != "EPIC":
            continue

        epic_id = str(rel.get("target") or "")
        ticket_id = str(rel.get("ticket_id") or "")

        if not epic_id or not ticket_id:
            continue

        epic_ado_url = rel.get("ado_url") or (
            f"https://dev.azure.com/{ORG}/{PROJECT}/_workitems/edit/{epic_id}"
            if epic_id else None
        )
        ticket_ado_url = rel.get("ticket_url") or (
            f"https://dev.azure.com/{ORG}/{PROJECT}/_workitems/edit/{ticket_id}"
            if ticket_id else None
        )

        jira_epic_id = jira_mapping.get(epic_id) or None
        jira_ticket_id = jira_mapping.get(ticket_id) or None

        epic_rows.append(
            {
                "epic_id": epic_id,
                "epic_title": title_lookup.get(epic_id, ""),
                "epic_ado_url": epic_ado_url,
                "ticket_id": ticket_id,
                "ticket_title": title_lookup.get(ticket_id, ""),
                "ticket_ado_url": ticket_ado_url,
                "jira_epic_id": jira_epic_id,
                "jira_epic_url": f"{jira_base}/{jira_epic_id}" if jira_epic_id else None,
                "jira_ticket_id": jira_ticket_id,
                "jira_ticket_url": (
                    f"{jira_base}/{jira_ticket_id}" if jira_ticket_id else None
                ),
            }
        )

    pd.DataFrame(epic_rows).to_csv(f"{OUTPUT_DIR}/epics.csv", index=False)
    logger.info("Saved epics.csv (%d epic mapping(s))", len(epic_rows))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point — orchestrates extraction of all work items.

    Reads work item IDs from ``workitems.csv``, fetches each item from the
    Azure DevOps REST API, and writes the results to:

    - ``output/workitems.csv``
    - ``output/comments.csv``
    - ``output/relations.csv``
    - ``output/attachments/{workitem_id}/``

    New fields extracted (vs original):
      - story_points      (Microsoft.VSTS.Scheduling.StoryPoints)
      - target_date       (Microsoft.VSTS.Scheduling.TargetDate)
      - work_category     (Custom.GSITWorkCategory)
      - product_impacted  (Custom.ProductImpacted)
      - product_category  (Custom.ProductCategory)
      - team_name         (Custom.TeamNamePickList)
      - work_subcategory  (Custom.WorkSubCategory)
      - category          (Custom.GSITCategory)

    Migration-helper fields (computed, not from ADO API):
      - external_issue_id  Full ADO URL: https://dev.azure.com/{ORG}/{PROJECT}/_workitems/edit/{id}
      - component          Fixed value "ADOImported" for Jira component assignment
      - jira_id            Jira issue key from ado_jira_mapping.json (if available)

    All work item types are extracted (Bug, User Story, Task, Epic, etc.).
    No type filtering is applied — every ID in the input CSV is fetched.
    """
    os.makedirs(ATTACHMENT_DIR, exist_ok=True)

    df = pd.read_csv("workitems.csv")
    df.columns = df.columns.str.strip().str.lower()

    workitems_data = []
    comments_data = []
    relations_data = []

    for wid in df["id"]:
        logger.info("Fetching work item %s", wid)

        try:
            data = fetch_workitem(wid)
        except Exception as exc:
            logger.error("Unexpected error processing work item %s: %s", wid, exc)
            continue

        if not data:
            continue

        fields = data.get("fields", {})

        # -- User identity fields ------------------------------------------
        created_by_name, created_by_email = _extract_identity(
            fields.get("System.CreatedBy")
        )
        assigned_to_name, assigned_to_email = _extract_identity(
            fields.get("System.AssignedTo")
        )

        # -- Resolve description and acceptance criteria ----------------
        raw_description = fields.get("System.Description") or ""
        raw_ac = fields.get("Microsoft.VSTS.Common.AcceptanceCriteria") or ""

        # Epics in ADO typically have no separate AC field — the AC is
        # embedded inline in the description HTML.  When the dedicated AC
        # field is empty, try to split it out of the description.
        if not raw_ac.strip():
            raw_description, raw_ac = _split_acceptance_criteria(raw_description)

        workitems_data.append(
            {
                # Core system fields
                "id": wid,
                "type": fields.get("System.WorkItemType"),
                "title": fields.get("System.Title"),
                "state": fields.get("System.State"),
                "description": raw_description or None,
                "created_by_name": created_by_name,
                "created_by_email": created_by_email,
                "assigned_to_name": assigned_to_name,
                "assigned_to_email": assigned_to_email,
                "created_date": fields.get("System.CreatedDate"),
                "changed_date": fields.get("System.ChangedDate"),
                "area_path": fields.get("System.AreaPath"),
                "iteration_path": fields.get("System.IterationPath"),
                "tags": fields.get("System.Tags"),
                "labels": "; ".join(
                    [t.strip() for t in (fields.get("System.Tags") or "").split(";") if t.strip()]
                    + [DEFAULT_LABEL]
                ),
                # Rich-text content fields (HTML)
                "acceptance_criteria": raw_ac or None,
                "repro_steps": fields.get("Microsoft.VSTS.TCM.ReproSteps"),
                "system_info": fields.get("Microsoft.VSTS.TCM.SystemInfo"),
                "analysis": fields.get("Microsoft.VSTS.CMMI.ImpactAssessmentHtml"),
                "proposed_fix": fields.get("Custom.ProposedFix"),
                # Scheduling / numeric fields
                "story_points": fields.get("Microsoft.VSTS.Scheduling.StoryPoints"),
                "target_date": fields.get("Microsoft.VSTS.Scheduling.TargetDate"),
                # Custom classification fields
                "work_category": fields.get("Custom.GSITWorkCategory"),
                "product_impacted": fields.get("Custom.ProductImpacted"),
                "product_category": fields.get("Custom.ProductCategory"),
                "team_name": fields.get("Custom.TeamNamePickList") or DEFAULT_TEAM_NAME,
                "work_subcategory": fields.get("Custom.WorkSubCategory"),
                "category": fields.get("Custom.GSITCategory"),
                # Migration-helper fields (computed, not from ADO API)
                "external_issue_id": (
                    f"https://dev.azure.com/{ORG}/{PROJECT}/_workitems/edit/{wid}"
                ),
                "component": "ADOImported,UWP",
            }
        )

        # -- Comments -------------------------------------------------------
        try:
            comments = fetch_comments(wid)
            for c in comments:
                comments_data.append(
                    {
                        "workitem_id": wid,
                        "comment": c.get("comment"),
                        "created_by": c.get("created_by"),
                        "created_date": c.get("created_date"),
                    }
                )
        except Exception as exc:
            logger.error("Failed to process comments for work item %s: %s", wid, exc)

        # -- Relations & attachments ----------------------------------------
        try:
            raw_relations = data.get("relations", [])
            records = extract_relations(wid, raw_relations)
            relations_data.extend(records)
        except Exception as exc:
            logger.error("Failed to process relations for work item %s: %s", wid, exc)

    # -- Enrich with jira_id from mapping file --------------------------------
    jira_mapping: dict[str, str] = {}
    mapping_path = os.path.join(OUTPUT_DIR, "ado_jira_mapping.json")
    if os.path.exists(mapping_path):
        try:
            with open(mapping_path, "r", encoding="utf-8") as f:
                jira_mapping = json.load(f)
            logger.info(
                "Loaded ado_jira_mapping.json (%d entries) — enriching workitems with jira_id",
                len(jira_mapping),
            )
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not load ado_jira_mapping.json: %s — jira_id will be empty", exc)

    mapped_count = 0
    for wi in workitems_data:
        ado_id = str(wi.get("id", ""))
        jira_key = jira_mapping.get(ado_id)
        wi["jira_id"] = jira_key or ""
        if jira_key:
            mapped_count += 1

    if jira_mapping:
        logger.info(
            "jira_id enrichment: %d of %d work items mapped",
            mapped_count, len(workitems_data),
        )

    # -- Write output files -------------------------------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        pd.DataFrame(workitems_data).to_csv(f"{OUTPUT_DIR}/workitems.csv", index=False)
        logger.info("Saved workitems.csv (%d rows)", len(workitems_data))
    except Exception as exc:
        logger.error("Failed to write workitems.csv: %s", exc)

    try:
        pd.DataFrame(comments_data).to_csv(f"{OUTPUT_DIR}/comments.csv", index=False)
        logger.info("Saved comments.csv (%d rows)", len(comments_data))
    except Exception as exc:
        logger.error("Failed to write comments.csv: %s", exc)

    try:
        pd.DataFrame(relations_data).to_csv(f"{OUTPUT_DIR}/relations.csv", index=False)
        logger.info("Saved relations.csv (%d rows)", len(relations_data))
    except Exception as exc:
        logger.error("Failed to write relations.csv: %s", exc)

    # -- Generate epics.csv -------------------------------------------------
    try:
        _generate_epics_csv(workitems_data, relations_data)
    except Exception as exc:
        logger.error("Failed to generate epics.csv: %s", exc)

    # -- Summary log --------------------------------------------------------
    attachment_count = sum(
        1 for r in relations_data if r.get("relation_category") == "attachment"
    )
    logger.info("=" * 60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("  Work items processed : %d", len(workitems_data))
    logger.info("  Work items attempted : %d", len(df))
    logger.info("  Comments extracted   : %d", len(comments_data))
    logger.info("  Relations extracted  : %d", len(relations_data))
    logger.info("  Attachments found    : %d", attachment_count)
    logger.info("=" * 60)
    logger.info("Extraction completed.")


if __name__ == "__main__":
    main()
