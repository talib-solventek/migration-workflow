"""
Epic Linker — post-migration step
Reads epics.csv, ado_jira_mapping.json, and epic_data.csv, then sets
customfield_10600 (Epic Link) on each child Jira issue.

Run AFTER jira_import.py:
  python extract_workitems_data.py
  python jira_import.py
  python epic_linker.py

Input files:
  output/epics.csv            — columns: epic_id, ticket_id
                         (ADO Epic ID → ADO child work item ID)
  output/ado_jira_mapping.json
                       — ADO ID → Jira issue key  (written by jira_import.py)
  output/epic_data.csv        — columns: ADO_Epic_ID, Jira_Epic_Key
                         (manually provided; maps ADO epics to their
                          already-existing Jira Epic keys)
"""

import json
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
# Constants
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("output")
MAPPING_FILE = OUTPUT_DIR / "ado_jira_mapping.json"
EPICS_CSV = OUTPUT_DIR / "epics.csv"
EPIC_DATA_CSV = OUTPUT_DIR / "epic_data.csv"    

API_DELAY = 0.3  # seconds between Jira API calls


# ---------------------------------------------------------------------------
# Config / session
# ---------------------------------------------------------------------------


def load_env() -> dict:
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


def _build_session(config: dict) -> requests.Session:
    session = requests.Session()
    session.auth = (config["email"], config["api_token"])
    session.headers.update({"Accept": "application/json"})
    session.verify = False  # Internal Jira with self-signed cert
    return session


def _api_url(config: dict, path: str) -> str:
    return f"{config['base_url']}/rest/api/3/{path.lstrip('/')}"


# ---------------------------------------------------------------------------
# File loaders
# ---------------------------------------------------------------------------


def load_epics_csv() -> pd.DataFrame:
    """Load epics.csv — expected columns: epic_id, ticket_id."""
    if not EPICS_CSV.exists():
        raise FileNotFoundError(f"Required file not found: {EPICS_CSV}")
    df = pd.read_csv(EPICS_CSV, dtype=str).fillna("")
    for col in ("epic_id", "ticket_id"):
        if col not in df.columns:
            raise ValueError(f"epics.csv is missing required column: '{col}'")
    logger.info("Loaded %d rows from %s", len(df), EPICS_CSV)
    return df


def load_ado_jira_mapping() -> dict:
    """Load ado_jira_mapping.json — returns {ado_id: jira_key}."""
    if not MAPPING_FILE.exists():
        raise FileNotFoundError(f"Required file not found: {MAPPING_FILE}")
    with open(MAPPING_FILE, encoding="utf-8") as f:
        mapping = json.load(f)
    logger.info("Loaded %d ADO→Jira mappings from %s", len(mapping), MAPPING_FILE)
    return mapping


def load_epic_data_csv() -> dict:
    """Load epic_data.csv — returns {ado_epic_id: jira_epic_key}."""
    if not EPIC_DATA_CSV.exists():
        raise FileNotFoundError(f"Required file not found: {EPIC_DATA_CSV}")
    df = pd.read_csv(EPIC_DATA_CSV, dtype=str).fillna("")
    for col in ("ADO_Epic_ID", "Jira_Epic_Key"):
        if col not in df.columns:
            raise ValueError(f"epic_data.csv is missing required column: '{col}'")
    mapping = {
        str(row["ADO_Epic_ID"]).strip(): str(row["Jira_Epic_Key"]).strip()
        for _, row in df.iterrows()
        if row["ADO_Epic_ID"].strip() and row["Jira_Epic_Key"].strip()
    }
    logger.info("Loaded %d epic mappings from %s", len(mapping), EPIC_DATA_CSV)
    return mapping


# ---------------------------------------------------------------------------
# Jira API helpers
# ---------------------------------------------------------------------------


def _get_current_epic_link(
    session: requests.Session,
    config: dict,
    issue_key: str,
) -> str | None:
    """Return the current customfield_10600 value for an issue, or None."""
    url = _api_url(config, f"issue/{issue_key}")
    try:
        resp = session.get(
            url,
            params={"fields": "customfield_10600"},
            timeout=30,
        )
        time.sleep(API_DELAY)
        if resp.status_code == 200:
            return resp.json().get("fields", {}).get("customfield_10600")
    except requests.RequestException as exc:
        logger.warning("Could not fetch current epic link for %s: %s", issue_key, exc)
    return None


def set_epic_link(
    session: requests.Session,
    config: dict,
    child_key: str,
    epic_key: str,
) -> bool:
    """PUT customfield_10600 on child_key. Returns True on success (HTTP 204)."""
    url = _api_url(config, f"issue/{child_key}")
    payload = {"fields": {"customfield_10600": epic_key}}
    try:
        resp = session.put(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        time.sleep(API_DELAY)
        if resp.status_code == 204:
            logger.info("Epic link set: %s → %s", child_key, epic_key)
            return True
        logger.error(
            "Failed to set epic link %s → %s — HTTP %s: %s",
            child_key, epic_key, resp.status_code, resp.text[:400],
        )
    except requests.RequestException as exc:
        logger.error(
            "Request error setting epic link %s → %s: %s", child_key, epic_key, exc
        )
    return False


# ---------------------------------------------------------------------------
# Main linking logic
# ---------------------------------------------------------------------------


def run(
    session: requests.Session,
    config: dict,
    epics_df: pd.DataFrame,
    ado_jira_mapping: dict,
    epic_data_mapping: dict,
) -> None:
    total = 0
    linked = 0
    skipped = 0
    failed = 0

    for _, row in epics_df.iterrows():
        total += 1
        ticket_id = str(row.get("ticket_id", "")).strip()
        epic_id   = str(row.get("epic_id", "")).strip()

        if not ticket_id or not epic_id:
            logger.warning("Row %d — blank ticket_id or epic_id, skipping.", total)
            skipped += 1
            continue

        # Resolve child Jira key
        child_key = ado_jira_mapping.get(ticket_id)
        if not child_key:
            logger.warning(
                "Child ADO %s not found in ado_jira_mapping.json — skipping.", ticket_id
            )
            skipped += 1
            continue

        # Resolve epic Jira key
        jira_epic_key = epic_data_mapping.get(epic_id)
        if not jira_epic_key:
            logger.warning(
                "Epic ADO %s not found in epic_data.csv — skipping child %s (%s).",
                epic_id, child_key, ticket_id,
            )
            skipped += 1
            continue

        # Skip if already linked to the same epic (idempotency)
        current = _get_current_epic_link(session, config, child_key)
        if current == jira_epic_key:
            logger.info(
                "Already linked: %s → %s — skipping.", child_key, jira_epic_key
            )
            skipped += 1
            continue

        # Set the epic link
        success = set_epic_link(session, config, child_key, jira_epic_key)
        if success:
            linked += 1
        else:
            failed += 1

    logger.info("=" * 60)
    logger.info("EPIC LINKER SUMMARY")
    logger.info("  Total processed      : %d", total)
    logger.info("  Successfully linked  : %d", linked)
    logger.info("  Skipped              : %d", skipped)
    logger.info("  Failed               : %d", failed)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    config = load_env()
    session = _build_session(config)

    epics_df         = load_epics_csv()
    ado_jira_mapping = load_ado_jira_mapping()
    epic_data_mapping = load_epic_data_csv()

    logger.info("=" * 60)
    logger.info("EPIC LINKER — Linking %d child issues to Epics", len(epics_df))
    logger.info("=" * 60)

    run(session, config, epics_df, ado_jira_mapping, epic_data_mapping)


if __name__ == "__main__":
    main()
