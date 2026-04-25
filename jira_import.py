"""
ADO → Jira Migration Importer
Reads extracted output files and creates Jira issues, comments,
attachments, and issue links via the Jira REST API v3.

Migration order:
  1. Create all issues (with all safe fields)
  2. Store ADO ID → Jira key mapping
  3. Update option-type fields (components) — separate step
     to prevent a bad component value from blocking issue creation
  4. Add comments
  5. Upload attachments
  6. Recreate relations (related/child links)
Field mapping (ADO → Jira):
  System.Title                          → summary
  System.WorkItemType                   → issuetype (via ISSUE_TYPE_MAP)
  System.Description + sections         → description (ADF, multi-paragraph)
  System.CreatedBy                      → reporter (accountId lookup)
  System.AssignedTo                     → assignee (accountId lookup)
  System.Tags + product_impacted        → labels
    + work_subcategory
  ADO work item ID                      → customfield_12100 (External Issue ID, URL)
  Microsoft.VSTS.Scheduling.StoryPoints → customfield_10003 (Story Points, float)
  Microsoft.VSTS.Scheduling.TargetDate  → duedate + customfield_17401 (YYYY-MM-DD)
  Custom.TeamNamePickList               → customfield_20107 (Team Name, string)
  Custom.GSITWorkCategory               → customfield_11503 (Work Category, select)
  —                                     → components (JIRA_COMPONENT env var)
"""

import base64
import io
import json
import logging
import os
import re
import time
import urllib3
from datetime import datetime
from pathlib import Path
from html.parser import HTMLParser

import pandas as pd
import requests
from dotenv import load_dotenv

# Suppress SSL verification warnings (Jira internal server)
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

# Delay between API calls to avoid Jira rate limiting
API_DELAY = 0.3

# ADO base used to build the External Issue ID URL (customfield_12100)
# These must match the values in your .env / ADO project
ADO_ORG = os.getenv("ADO_ORG", "libertymutual-grs-liu")
ADO_PROJECT = os.getenv("ADO_PROJECT", "GSIT")

# Optional: name of a Jira component that already exists in your project.
# Set JIRA_COMPONENT="" in .env to skip component assignment.
JIRA_COMPONENT = os.getenv("JIRA_COMPONENT", "ADOImported")

# ADO PAT reused here for downloading images embedded in ADO work item HTML
_ADO_PAT = os.getenv("ADO_PAT", "")

# ADO work item type → Jira issue type
ISSUE_TYPE_MAP = {
    "Bug": "Bug",
    "User Story": "Story",
    "Service Request": "Task",
}

# Jira link type for non-parent relations.
# "Relates" is the only link type guaranteed to exist in all Jira instances.
RELATION_LINK_TYPE_MAP = {
    "related": "Relates",
    "child": "Relates",   # "is child of" may not exist; use Relates as safe fallback
}

# ---------------------------------------------------------------------------
# Environment / configuration
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

    # ADO PAT is optional but required for embedded image downloading.
    # Re-read after load_dotenv() so the .env value is picked up.
    global _ADO_PAT
    _ADO_PAT = os.getenv("ADO_PAT", "")
    if not _ADO_PAT:
        logger.warning(
            "ADO_PAT not set — images embedded in ADO work items will NOT be migrated. "
            "Add ADO_PAT=<personal_access_token> to your .env file "
            "(Azure DevOps → User Settings → Personal Access Tokens, scope: Work Items Read)."
        )
    else:
        logger.info("ADO_PAT configured — embedded images will be downloaded and uploaded to Jira.")

    return config


# ---------------------------------------------------------------------------
# Jira API session helpers
# ---------------------------------------------------------------------------


def _build_session(config: dict) -> requests.Session:
    """Return an authenticated requests.Session for the Jira REST API."""
    session = requests.Session()
    session.auth = (config["email"], config["api_token"])
    session.headers.update({"Accept": "application/json"})
    session.verify = False   # Internal Jira with self-signed cert
    return session


def _api_url(config: dict, path: str) -> str:
    return f"{config['base_url']}/rest/api/3/{path.lstrip('/')}"


def _safe_request(
    session: requests.Session,
    method: str,
    url: str,
    label: str,
    **kwargs,
) -> requests.Response | None:
    """Execute an HTTP request and return the response, or None on failure.

    Logs warnings/errors without raising so the migration can continue.
    Handles 200, 201, and 204 (PUT/issue update returns 204).
    """
    try:
        resp = session.request(method, url, timeout=30, **kwargs)
        time.sleep(API_DELAY)

        if resp.status_code in (200, 201, 204):
            return resp

        logger.warning(
            "%s — HTTP %s: %s",
            label,
            resp.status_code,
            resp.text[:400],
        )
        return None

    except requests.RequestException as exc:
        logger.error("%s — request error: %s", label, exc)
        return None


# ---------------------------------------------------------------------------
# User lookup
# ---------------------------------------------------------------------------


def search_user(session: requests.Session, config: dict, email: str) -> str | None:
    """Search Jira for a user by email and return their accountId.

    Returns None if not found — never raises.
    """
    if not email or not isinstance(email, str) or email.strip() == "":
        return None

    url = _api_url(config, "user/search")
    resp = _safe_request(
        session, "GET", url, f"search user {email}", params={"query": email.strip()}
    )
    if resp is None:
        return None

    users = resp.json()
    if not users:
        logger.warning("User not found in Jira: %s — skipping assignment", email)
        return None

    account_id = users[0].get("accountId")
    logger.debug("Resolved %s → accountId %s", email, account_id)
    return account_id


# ---------------------------------------------------------------------------
# HTML → plain-text helper
# ---------------------------------------------------------------------------


def _html_to_text(raw: str) -> str:
    """Convert an HTML string to Markdown-compatible plain text.

    Block-level tags become newlines; table cells are separated with ' | '.
    HTML entities are decoded. <img> tags are emitted as ![image](src) so
    that _markdown_to_adf_nodes() can convert them to mediaSingle ADF nodes
    even when markdownify is not installed.
    Returns empty string for non-string input.
    """
    if not raw or not isinstance(raw, str):
        return ""

    class _Converter(HTMLParser):
        BLOCK_TAGS = {"div", "p", "br", "li", "tr", "h1", "h2", "h3",
                      "h4", "h5", "h6", "blockquote", "pre"}
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
                    self._parts.append(f"\n![image]({src})\n")
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
            # Collapse 3+ consecutive newlines to 2
            text = re.sub(r"\n{3,}", "\n\n", text)
            # Strip cell separator noise on each line
            lines = [ln.strip().strip("|").strip() for ln in text.splitlines()]
            lines = [ln for ln in lines if ln]
            return "\n".join(lines)

    converter = _Converter()
    converter.feed(raw)
    return converter.get_text()


# ---------------------------------------------------------------------------
# HTML → Markdown conversion
# ---------------------------------------------------------------------------

try:
    from markdownify import markdownify as _md_convert
    _MARKDOWNIFY_AVAILABLE = True
except ImportError:
    _MARKDOWNIFY_AVAILABLE = False
    logger.warning(
        "markdownify not installed — HTML formatting will fall back to plain text. "
        "Install with: pip install markdownify"
    )


def convert_html_to_markdown(html: str) -> str:
    """Convert an HTML string to Markdown.

    Uses markdownify when available; falls back to _html_to_text otherwise.
    Inline images are preserved as ![alt](src) references so they can be
    processed later by extract_and_upload_images().
    """
    if not html or not isinstance(html, str):
        return ""
    if _MARKDOWNIFY_AVAILABLE:
        try:
            result = _md_convert(
                html,
                heading_style="ATX",
                bullets="-",
                strip=["script", "style"],
            )
            # Collapse 3+ blank lines that markdownify sometimes produces
            result = re.sub(r"\n{3,}", "\n\n", result).strip()
            # Unwrap image references from bold/italic markers that markdownify
            # emits when <img> is nested inside <b> or <em> tags.
            # e.g. **![alt](url)** → ![alt](url)  so the image becomes a
            # mediaSingle ADF node instead of bold/italic text.
            result = re.sub(r"\*{1,2}(!\[[^\]]*\]\([^)]+\))\*{1,2}", r"\1", result)
            return result
        except Exception as exc:
            logger.warning(
                "markdownify conversion failed: %s — falling back to plain text", exc
            )
    return _html_to_text(html)


# ---------------------------------------------------------------------------
# Markdown → ADF helpers
# ---------------------------------------------------------------------------


def _parse_inline_markdown(text: str) -> list[dict]:
    """Parse inline Markdown (bold, italic, inline code, links) into ADF inline nodes."""
    nodes: list[dict] = []
    pattern = re.compile(
        r"\*\*(.+?)\*\*"               # **bold**
        r"|\*(.+?)\*"                  # *italic*
        r"|`([^`\n]+)`"               # `inline code`
        r"|\[([^\]]+)\]\(([^)]+)\)"   # [text](url)
        r"|!\[([^\]]*)\]\(([^)]+)\)"  # ![alt](url) — image placeholder
    )
    last = 0
    for m in pattern.finditer(text):
        if m.start() > last:
            plain = text[last : m.start()]
            if plain:
                nodes.append({"type": "text", "text": plain})
        if m.group(1) is not None:  # **bold**
            nodes.append({"type": "text", "text": m.group(1), "marks": [{"type": "strong"}]})
        elif m.group(2) is not None:  # *italic*
            nodes.append({"type": "text", "text": m.group(2), "marks": [{"type": "em"}]})
        elif m.group(3) is not None:  # `code`
            nodes.append({"type": "text", "text": m.group(3), "marks": [{"type": "code"}]})
        elif m.group(4) is not None:  # [text](url)
            nodes.append({
                "type": "text",
                "text": m.group(4),
                "marks": [{"type": "link", "attrs": {"href": m.group(5)}}],
            })
        elif m.group(6) is not None:  # ![alt](url) — ADF mediaSingle block node
            url = m.group(7)
            nodes.append({
                "type": "mediaSingle",
                "attrs": {"layout": "center"},
                "content": [{
                    "type": "media",
                    "attrs": {"type": "external", "url": url},
                }],
            })
        last = m.end()
    if last < len(text):
        remaining = text[last:]
        if remaining:
            nodes.append({"type": "text", "text": remaining})
    return nodes or [{"type": "text", "text": text}]


def _emit_inline_as_blocks(acc: list[dict], inline: list[dict]) -> None:
    """Append *inline* content to *acc* as proper ADF block nodes.

    mediaSingle is a block-level ADF node and MUST NOT be nested inside a
    paragraph.  This helper splits the inline list at each mediaSingle node:
      - consecutive non-media nodes → wrapped in a single paragraph
      - each mediaSingle node       → appended directly as a block
    """
    pending: list[dict] = []
    for node in inline:
        if node.get("type") == "mediaSingle":
            if pending:
                acc.append({"type": "paragraph", "content": pending})
                pending = []
            acc.append(node)
        else:
            pending.append(node)
    if pending:
        acc.append({"type": "paragraph", "content": pending})


def _markdown_to_adf_nodes(markdown: str) -> list[dict]:
    """Convert a Markdown string to ADF block nodes.

    Handles: headings, bullet/ordered lists, tables, fenced code blocks,
    blockquotes, horizontal rules, and paragraphs with inline formatting.
    """
    if not markdown or not markdown.strip():
        return []

    nodes: list[dict] = []
    lines = markdown.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Blank line — skip
        if not stripped:
            i += 1
            continue

        # Heading: # / ## / ...
        h = re.match(r"^(#{1,6})\s+(.*)", stripped)
        if h:
            level = min(len(h.group(1)), 6)
            nodes.append({
                "type": "heading",
                "attrs": {"level": level},
                "content": _parse_inline_markdown(h.group(2)),
            })
            i += 1
            continue

        # Horizontal rule
        if re.match(r"^[-*_]{3,}\s*$", stripped):
            nodes.append({"type": "rule"})
            i += 1
            continue

        # Fenced code block: ```
        if stripped.startswith("```"):
            lang = stripped[3:].strip() or None
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            i += 1  # skip closing ```
            nodes.append({
                "type": "codeBlock",
                "attrs": {"language": lang} if lang else {},
                "content": [{"type": "text", "text": "\n".join(code_lines)}],
            })
            continue

        # Bullet list: - / * / +
        if re.match(r"^[-*+]\s+", stripped):
            items: list[dict] = []
            while i < len(lines):
                bm = re.match(r"^[-*+]\s+(.*)", lines[i].strip())
                if not bm:
                    break
                items.append({
                    "type": "listItem",
                    "content": [{"type": "paragraph", "content": _parse_inline_markdown(bm.group(1))}],
                })
                i += 1
            nodes.append({"type": "bulletList", "content": items})
            continue

        # Ordered list: 1. / 2. / ...
        if re.match(r"^\d+\.\s+", stripped):
            items = []
            while i < len(lines):
                om = re.match(r"^\d+\.\s+(.*)", lines[i].strip())
                if not om:
                    break
                items.append({
                    "type": "listItem",
                    "content": [{"type": "paragraph", "content": _parse_inline_markdown(om.group(1))}],
                })
                i += 1
            nodes.append({"type": "orderedList", "content": items})
            continue

        # Table: starts with |
        if stripped.startswith("|") and "|" in stripped[1:]:
            table_rows: list[dict] = []
            is_header_row = True
            while i < len(lines):
                tline = lines[i].strip()
                if not tline.startswith("|"):
                    break
                # Separator row like |---|---| — marks end of header
                if re.match(r"^\|[-|:\s]+\|$", tline):
                    is_header_row = False
                    i += 1
                    continue
                cells = [c.strip() for c in tline.strip("|").split("|")]
                cell_type = "tableHeader" if is_header_row else "tableCell"
                table_rows.append({
                    "type": "tableRow",
                    "content": [
                        {
                            "type": cell_type,
                            "attrs": {},
                            "content": [{"type": "paragraph", "content": _parse_inline_markdown(c)}],
                        }
                        for c in cells
                    ],
                })
                is_header_row = False  # only first data row is treated as header
                i += 1
            if table_rows:
                nodes.append({"type": "table", "content": table_rows})
            continue

        # Blockquote: > ...
        if stripped.startswith("> "):
            quote_lines: list[str] = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                quote_lines.append(lines[i].strip().lstrip(">").lstrip())
                i += 1
            inner = _markdown_to_adf_nodes("\n".join(quote_lines))
            nodes.append({
                "type": "blockquote",
                "content": inner or [{"type": "paragraph", "content": []}],
            })
            continue

        # Paragraph — gather consecutive non-block lines
        para_parts: list[str] = []
        while i < len(lines):
            s = lines[i].strip()
            if not s:
                break
            if (
                re.match(r"^#{1,6}\s", s)
                or s.startswith("```")
                or re.match(r"^[-*+]\s+", s)
                or re.match(r"^\d+\.\s+", s)
                or re.match(r"^[-*_]{3,}\s*$", s)
                or s.startswith("> ")
                or (s.startswith("|") and "|" in s[1:])
            ):
                break
            para_parts.append(lines[i])
            i += 1
        if para_parts:
            para_text = " ".join(p.strip() for p in para_parts)
            inline = _parse_inline_markdown(para_text)
            if inline:
                _emit_inline_as_blocks(nodes, inline)

    return nodes


# ---------------------------------------------------------------------------
# Image helpers — download from ADO, upload to Jira
# ---------------------------------------------------------------------------


def _get_ado_auth_header() -> dict:
    """Return Basic Authorization header for ADO-authenticated requests."""
    if not _ADO_PAT:
        return {}
    auth = base64.b64encode(f":{_ADO_PAT}".encode()).decode()
    return {"Authorization": f"Basic {auth}"}


def _upload_image_to_jira(
    session: requests.Session,
    config: dict,
    jira_key: str,
    img_url: str,
) -> str | None:
    """Download *img_url* (using ADO auth if hosted on ADO) and upload to Jira.

    Returns the Jira attachment content URL on success, or None on failure.
    The upload uses the existing _safe_request helper so failures are logged,
    not raised.
    """
    is_ado_url = "dev.azure.com" in img_url or "visualstudio.com" in img_url

    if is_ado_url and not _ADO_PAT:
        logger.warning(
            "ADO_PAT not configured — cannot download ADO-hosted image: %s. "
            "Add ADO_PAT=<personal_access_token> to your .env file (scope: Work Items Read).",
            img_url,
        )
        return None

    headers: dict = _get_ado_auth_header() if is_ado_url else {}

    try:
        r = requests.get(img_url, headers=headers, timeout=30, verify=False)
        if r.status_code != 200:
            logger.warning("Image download failed %s — HTTP %s", img_url, r.status_code)
            return None

        # Validate that ADO actually returned an image, not a login/redirect page.
        # HTTP 203 with text/html = unauthenticated request intercepted by SSO proxy.
        content_type = r.headers.get("Content-Type", "")
        if not content_type.startswith("image/"):
            logger.warning(
                "Image URL returned non-image content (Content-Type: %s) for %s — "
                "authentication failed or URL is invalid. Verify ADO_PAT is correct.",
                content_type.split(";")[0].strip(),
                img_url,
            )
            return None

        logger.info("Downloaded image: %s (%d bytes)", img_url, len(r.content))
        img_data = r.content
    except requests.RequestException as exc:
        logger.warning("Could not download image %s: %s", img_url, exc)
        return None

    # Derive a safe filename from the URL (strip query string)
    raw_name = img_url.split("?")[0].rstrip("/").rsplit("/", 1)[-1]
    filename = re.sub(r'[\\/:*?"<>|]', "", raw_name) or "image.png"
    if "." not in filename:
        filename += ".png"

    url = _api_url(config, f"issue/{jira_key}/attachments")
    try:
        resp = _safe_request(
            session,
            "POST",
            url,
            f"upload image {filename} to {jira_key}",
            files={"file": (filename, io.BytesIO(img_data), "application/octet-stream")},
            headers={"X-Atlassian-Token": "no-check"},
        )
        if resp:
            attachments = resp.json()
            if isinstance(attachments, list) and attachments:
                content_url = attachments[0].get("content")
                logger.info("Uploaded image %s → %s", filename, content_url)
                return content_url
            logger.warning(
                "Image upload response contained no attachment data for %s on %s",
                filename, jira_key,
            )
    except Exception as exc:
        logger.warning("Image upload to Jira failed for %s on %s: %s", filename, jira_key, exc)
    return None


def extract_and_upload_images(
    html: str,
    jira_key: str,
    session: requests.Session,
    config: dict,
) -> dict[str, str]:
    """Extract all <img src> URLs from *html*, upload each to Jira as an attachment.

    Returns a mapping of old URL → new Jira content URL for successful uploads.
    Images that fail to upload are omitted so the original URL is preserved.
    """
    if not html or not isinstance(html, str):
        return {}

    seen: set[str] = set()
    img_urls: list[str] = []

    class _ImgExtractor(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag.lower() == "img":
                src = dict(attrs).get("src", "")
                if src and src not in seen:
                    img_urls.append(src)
                    seen.add(src)

    _ImgExtractor().feed(html)

    url_map: dict[str, str] = {}
    for img_url in img_urls:
        new_url = _upload_image_to_jira(session, config, jira_key, img_url)
        if new_url:
            url_map[img_url] = new_url
        else:
            logger.warning("Image upload failed — original URL preserved: %s", img_url)
    return url_map


def replace_image_urls(content: str, url_mapping: dict[str, str]) -> str:
    """Replace old image URLs in *content* with new Jira attachment URLs."""
    for old_url, new_url in url_mapping.items():
        content = content.replace(old_url, new_url)
    return content


# ---------------------------------------------------------------------------
# ADF (Atlassian Document Format) helpers
# ---------------------------------------------------------------------------


def _text_to_adf_nodes(text: str) -> list[dict]:
    """Convert plain text with newlines to a list of ADF paragraph nodes.

    Double newlines create separate paragraphs.
    Single newlines within a paragraph become ADF hardBreak nodes.
    """
    if not text or not text.strip():
        return []

    nodes: list[dict] = []
    paragraphs = re.split(r"\n\n+", text.strip())

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        lines = para.split("\n")
        inline: list[dict] = []

        for i, line in enumerate(lines):
            if line.strip():
                inline.append({"type": "text", "text": line})
            # Add hardBreak between lines (but not after the last line)
            if i < len(lines) - 1 and line.strip():
                inline.append({"type": "hardBreak"})

        # Remove any trailing hardBreak
        while inline and inline[-1].get("type") == "hardBreak":
            inline.pop()

        if inline:
            nodes.append({"type": "paragraph", "content": inline})

    return nodes


def _heading_node(text: str, level: int = 3) -> dict:
    """Create an ADF heading node."""
    return {
        "type": "heading",
        "attrs": {"level": level},
        "content": [{"type": "text", "text": text}],
    }


def _text_with_images_to_adf_nodes(text: str) -> list[dict]:
    """Convert plain text that may contain [Image: URL] placeholders to ADF nodes.

    Used for comment bodies where HTML has already been stripped and images
    are represented as [Image: URL] tokens (produced by extract_workitems_data.py
    and potentially updated to Jira attachment URLs by add_comment()).

    Each [Image: URL] becomes an ADF mediaSingle block node so Jira renders
    the image inline rather than as literal text.
    """
    if not text or not text.strip():
        return []

    nodes: list[dict] = []
    img_pattern = re.compile(r"\[Image:\s*([^\]]+)\]")
    last = 0

    for m in img_pattern.finditer(text):
        before = text[last : m.start()]
        if before.strip():
            nodes.extend(_text_to_adf_nodes(before))
        img_url = m.group(1).strip()
        nodes.append({
            "type": "mediaSingle",
            "attrs": {"layout": "center"},
            "content": [{
                "type": "media",
                "attrs": {"type": "external", "url": img_url},
            }],
        })
        last = m.end()

    remaining = text[last:]
    if remaining.strip():
        nodes.extend(_text_to_adf_nodes(remaining))

    return nodes


def _make_adf_doc(text: str) -> dict | None:
    """Wrap plain text in a minimal ADF document, or return None if text is empty.

    Use this for custom fields that the Jira field metadata lists as 'string'
    but are actually configured as rich-text (ADF) fields in the instance.
    """
    nodes = _text_to_adf_nodes(text)
    if not nodes:
        return None
    return {"version": 1, "type": "doc", "content": nodes}


def build_description_adf(row: pd.Series, issue_type: str = "Task") -> dict | None:
    """Build a structured ADF description document from multiple ADO fields.

    Content varies by issue type so that fields with dedicated Jira mappings
    are NOT duplicated in the description body:

      Story : description (main body) + Analysis section
              Acceptance Criteria → customfield_13802 only (images render there now)

      Bug   : System Info (main body) + Analysis section
              + Possible Solutions section (field unchanged, images don't render there)
              Steps to Reproduce → customfield_11100 only (images render there now)

      Task  : description (main body)

    Returns None when all applicable sections are empty.
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
        """Add a value as the main body (no heading)."""
        if not value or str(value).strip() in ("", "nan"):
            return
        md = convert_html_to_markdown(str(value))
        if md:
            content.extend(_markdown_to_adf_nodes(md))

    if issue_type == "Story":
        _add_base(row.get("description"))
        _add_section("Analysis", row.get("analysis"))
        # Acceptance Criteria goes to customfield_13802 only — images now render there.

    elif issue_type == "Bug":
        _add_base(row.get("system_info"))
        _add_section("Analysis", row.get("analysis"))
        # Steps to Reproduce goes to customfield_11100 only — images now render there.
        # Possible Solutions field is unchanged (images don't render there), so include
        # it here so images display correctly in the description.
        _add_section("Possible Solutions", row.get("proposed_fix"))

    else:  # Task / Service Request / unknown
        _add_base(row.get("description"))

    if not content:
        return None

    return {"version": 1, "type": "doc", "content": content}


# ---------------------------------------------------------------------------
# Field value conversion helpers
# ---------------------------------------------------------------------------


def _safe_float(value) -> float | None:
    """Safely convert a value to float; returns None on failure."""
    if value is None or str(value).strip() in ("", "nan"):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_date(value) -> str | None:
    """Extract and validate a YYYY-MM-DD date string from a datetime value.

    ADO returns datetimes like '2024-05-01T13:00:00Z'.
    Jira 'duedate' expects 'YYYY-MM-DD'.
    """
    if not value or str(value).strip() in ("", "nan"):
        return None
    date_str = str(value).strip()[:10]   # Slice YYYY-MM-DD
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        return None


def _build_ado_url(ado_id: str) -> str:
    """Build the ADO work item URL used as the External Issue ID value."""
    return (
        f"https://dev.azure.com/{ADO_ORG}/{ADO_PROJECT}"
        f"/_workitems/edit/{ado_id}"
    )


def _is_empty(value) -> bool:
    """Return True if a value should be treated as missing/empty."""
    return not value or str(value).strip().lower() in ("", "nan", "none")


# ---------------------------------------------------------------------------
# Option-field helpers: fetch allowed values via createmeta (no admin needed)
# ---------------------------------------------------------------------------

_WORK_CATEGORY_FIELD_ID      = "customfield_11503"   # Work Category (option)
_TEAM_NAME_FIELD_ID          = "customfield_20107"   # Team name(s) (array/option)
_ACCEPTANCE_CRITERIA_FIELD_ID = "customfield_13802"  # Acceptance Criteria (string) — Stories
_STEPS_TO_REPRODUCE_FIELD_ID  = "customfield_11100"  # Steps to Reproduce (string) — Bugs
_POSSIBLE_SOLUTIONS_FIELD_ID  = "customfield_11705"  # Possible Solutions (string) — Bugs
_CLASS_FIELD_ID               = "customfield_22607"  # Class (option) — Work SubCategory
_APPLICATION_FIELD_ID         = "customfield_18255"  # Application (array/option) — Product Impacted
_VERTICALS_FIELD_ID           = "customfield_20405"  # Vertical(s) (array/option) — Product Category

# Cache: field_id → set of allowed option value strings
# Populated lazily on first use; empty set means "validation not available".
_FIELD_OPTIONS_CACHE: dict[str, set[str]] = {}

# Known ADO → Jira value mappings for Work Category.
# Confirmed from live createmeta: this Jira instance stores Work Category values
# WITH backslashes (e.g. "Grow\Transform"), identical to ADO — so exact match
# works without conversion. This map only needs entries where ADO and Jira
# genuinely differ (e.g. a renamed option). Add entries here if needed.
_WORK_CATEGORY_ADO_MAP: dict[str, str] = {}


def _fetch_field_options_via_createmeta(
    session: requests.Session,
    config: dict,
    field_id: str,
) -> set[str]:
    """Return the set of allowed option values for a custom field.

    Uses GET /rest/api/3/issue/createmeta — does NOT require Jira admin access.
    Inspects allowedValues across all issue types in the project so that options
    available only on certain issue types are still discovered.

    Result is cached per field_id for the lifetime of the migration run.
    Returns an empty set on any failure — callers treat empty set as
    "validation unavailable, send value as-is and let Jira decide".
    """
    if field_id in _FIELD_OPTIONS_CACHE:
        return _FIELD_OPTIONS_CACHE[field_id]

    options: set[str] = set()

    url = _api_url(config, "issue/createmeta")
    resp = _safe_request(
        session, "GET", url,
        f"fetch createmeta for field options ({field_id})",
        params={
            "projectKeys": config["project_key"],
            "expand": "projects.issuetypes.fields",
        },
    )

    if resp is None:
        logger.warning(
            "Could not fetch createmeta — option validation disabled for %s. "
            "Values will be sent as-is; Jira will reject invalid ones.",
            field_id,
        )
        _FIELD_OPTIONS_CACHE[field_id] = options
        return options

    try:
        data = resp.json()
        for project in data.get("projects", []):
            for issue_type in project.get("issuetypes", []):
                field_meta = issue_type.get("fields", {}).get(field_id, {})
                for allowed in field_meta.get("allowedValues", []):
                    val = allowed.get("value")
                    if val:
                        options.add(val)
    except Exception as exc:
        logger.warning(
            "Failed to parse createmeta for field %s: %s — validation disabled",
            field_id, exc,
        )

    logger.info(
        "Allowed options for %s (%d): %s",
        field_id,
        len(options),
        sorted(options) if options else "(none fetched — validation disabled)",
    )
    _FIELD_OPTIONS_CACHE[field_id] = options
    return options


def _resolve_work_category(raw: str, allowed: set[str]) -> str | None:
    """Map an ADO Work Category string to a valid Jira option value.

    Resolution order:
      1. Exact match against allowed options (fastest path).
      2. Lookup in _WORK_CATEGORY_ADO_MAP, then check result is allowed.
      3. Replace backslash with forward slash, check against allowed.
      4. Case-insensitive match against allowed options.
      5. Return None — field will be skipped for this issue.

    When *allowed* is empty (createmeta unavailable), step 1 passes through the
    backslash→slash converted value so the best-effort attempt is still made.
    """
    if not raw:
        return None

    # Normalise: ADO uses backslash, Jira typically uses forward slash
    converted = raw.replace("\\", "/")

    # Step 1 — exact match (works when value is already correct)
    if not allowed:
        # No validation available: return the normalised value and let Jira decide
        mapped = _WORK_CATEGORY_ADO_MAP.get(raw, converted)
        logger.debug("Work Category validation unavailable — sending '%s' as-is", mapped)
        return mapped

    if raw in allowed:
        return raw
    if converted in allowed:
        return converted

    # Step 2 — hardcoded ADO→Jira map
    mapped = _WORK_CATEGORY_ADO_MAP.get(raw)
    if mapped and mapped in allowed:
        return mapped

    # Step 3 — case-insensitive match
    raw_lower = converted.lower()
    for opt in allowed:
        if opt.lower() == raw_lower:
            logger.debug(
                "Work Category case-insensitive match: '%s' → '%s'", raw, opt
            )
            return opt

    logger.warning(
        "Work Category value '%s' (normalised: '%s') not found in allowed options %s "
        "— skipping field. Update _WORK_CATEGORY_ADO_MAP if this value is valid.",
        raw, converted, sorted(allowed),
    )
    return None


def _resolve_team_names(raw: str, allowed: set[str]) -> list[dict]:
    """Split a semicolon-separated ADO team name string into Jira option objects.

    ADO stores multiple teams as "Team A; Team B".
    Jira multiselect expects [{"value": "Team A"}, {"value": "Team B"}].

    Each individual name is validated against *allowed* (case-insensitive).
    Invalid names are skipped with a warning; valid names are included.
    When *allowed* is empty (createmeta unavailable), all names are included.
    """
    if not raw or _is_empty(raw):
        return []

    names = [n.strip() for n in raw.split(";") if n.strip()]
    result: list[dict] = []

    for name in names:
        if not allowed:
            # No validation: include as-is
            result.append({"value": name})
            continue

        if name in allowed:
            result.append({"value": name})
            continue

        # Case-insensitive fallback
        name_lower = name.lower()
        match = next((opt for opt in allowed if opt.lower() == name_lower), None)
        if match:
            logger.debug("Team name '%s' matched to '%s' (case-insensitive)", name, match)
            result.append({"value": match})
        else:
            logger.warning(
                "Team name '%s' not in allowed options %s — skipping this value.",
                name, sorted(allowed),
            )

    return result


def _resolve_single_option(raw: str, allowed: set[str], field_name: str) -> str | None:
    """Resolve a single string value to a valid Jira option.

    Tries exact match, then case-insensitive match.
    When *allowed* is empty (createmeta unavailable) passes the value through.
    Returns None when no match is found and *allowed* is non-empty.
    """
    if not raw:
        return None
    if not allowed:
        logger.debug("%s validation unavailable — sending '%s' as-is", field_name, raw)
        return raw
    if raw in allowed:
        return raw
    raw_lower = raw.lower()
    match = next((opt for opt in allowed if opt.lower() == raw_lower), None)
    if match:
        logger.debug("%s case-insensitive match: '%s' → '%s'", field_name, raw, match)
        return match
    logger.warning(
        "%s value '%s' not in allowed options %s — skipping.",
        field_name, raw, sorted(allowed),
    )
    return None


def _resolve_array_options(raw: str, allowed: set[str], field_name: str) -> list[dict]:
    """Split a semicolon-separated string and resolve each value to a Jira option object.

    Returns a list of {"value": "..."} dicts for all values that pass validation.
    When *allowed* is empty all values are passed through without validation.
    """
    values = [v.strip() for v in raw.split(";") if v.strip()]
    result: list[dict] = []
    for val in values:
        if not allowed:
            result.append({"value": val})
            continue
        if val in allowed:
            result.append({"value": val})
            continue
        val_lower = val.lower()
        match = next((opt for opt in allowed if opt.lower() == val_lower), None)
        if match:
            logger.debug("%s case-insensitive match: '%s' → '%s'", field_name, val, match)
            result.append({"value": match})
        else:
            logger.warning(
                "%s value '%s' not in allowed options %s — skipping this value.",
                field_name, val, sorted(allowed),
            )
    return result


# ---------------------------------------------------------------------------
# Labels builder
# ---------------------------------------------------------------------------


def _build_labels(row: pd.Series) -> list[str]:
    """Build the labels list from the pre-computed labels column.

    The labels column is produced by extract_workitems_data.py and contains
    ADO tags plus the DEFAULT_LABEL (e.g. "US") appended automatically.

    product_impacted → Application field (customfield_18255, array/option)
    work_subcategory → Class field       (customfield_22607, option)
    Both are now mapped to proper Jira fields and excluded from labels.
    """
    labels: list[str] = []

    labels_raw = str(row.get("labels", "") or "")
    if not _is_empty(labels_raw):
        labels += [t.strip().replace(" ", "_") for t in labels_raw.split(";") if t.strip()]

    return labels


# ---------------------------------------------------------------------------
# Issue creation
# ---------------------------------------------------------------------------


def _build_issue_payload(
    config: dict,
    row: pd.Series,
    assignee_id: str | None,
    reporter_id: str | None,
) -> dict:
    """Construct the Jira issue creation payload from a workitems.csv row.

    Only includes fields that are safe regardless of Jira project configuration
    (i.e., no option/select fields that may fail validation).
    Option-type fields (components, Work Category) are applied separately
    via update_issue_optional_fields() after creation succeeds.
    """
    ado_id = str(row.get("id", "")).strip()
    issue_type = ISSUE_TYPE_MAP.get(str(row.get("type", "")).strip(), "Task")
    description_adf = build_description_adf(row, issue_type)

    payload: dict = {
        "fields": {
            "project":   {"key": config["project_key"]},
            "summary":   str(row.get("title", "Untitled"))[:255],
            "issuetype": {"name": issue_type},
        }
    }

    if description_adf:
        payload["fields"]["description"] = description_adf

    if assignee_id:
        payload["fields"]["assignee"] = {"accountId": assignee_id}

    if reporter_id:
        payload["fields"]["reporter"] = {"accountId": reporter_id}

    # Labels: ADO tags only (product_impacted and work_subcategory go to proper fields)
    labels = _build_labels(row)
    if labels:
        payload["fields"]["labels"] = labels

    # External Issue ID (customfield_12100 — string)
    ext_id = str(row.get("external_issue_id", "") or "").strip()
    if not _is_empty(ext_id):
        payload["fields"]["customfield_12100"] = ext_id
    elif not _is_empty(ado_id):
        payload["fields"]["customfield_12100"] = _build_ado_url(ado_id)

    # Story Points (customfield_10003 — number)
    sp = _safe_float(row.get("story_points"))
    if sp is not None:
        payload["fields"]["customfield_10003"] = sp
        logger.debug("ADO %s — story_points: %s", ado_id, sp)

    # Target Date → duedate + customfield_17401 (Target end — date)
    due = _safe_date(row.get("target_date"))
    if due:
        payload["fields"]["duedate"] = due
        payload["fields"]["customfield_17401"] = due
        logger.debug("ADO %s — target_date '%s' → duedate + customfield_17401", ado_id, due)

    # --- Issue-type-specific rich-text fields ---
    # customfield_13802, customfield_11100, customfield_11705 are declared as
    # type "string" in the Jira field metadata but are actually configured as
    # rich-text fields in this instance — they require ADF, not plain strings.

    # Story: Acceptance Criteria → customfield_13802 (ADF)
    if issue_type == "Story":
        ac_nodes = _markdown_to_adf_nodes(
            convert_html_to_markdown(str(row.get("acceptance_criteria", "") or ""))
        )
        if ac_nodes:
            payload["fields"][_ACCEPTANCE_CRITERIA_FIELD_ID] = {
                "version": 1, "type": "doc", "content": ac_nodes
            }
            logger.debug("ADO %s — acceptance_criteria mapped (ADF)", ado_id)

    # Bug: Repro Steps → customfield_11100 (ADF)
    #      Proposed Fix  → customfield_11705 (ADF)
    elif issue_type == "Bug":
        repro_nodes = _markdown_to_adf_nodes(
            convert_html_to_markdown(str(row.get("repro_steps", "") or ""))
        )
        if repro_nodes:
            payload["fields"][_STEPS_TO_REPRODUCE_FIELD_ID] = {
                "version": 1, "type": "doc", "content": repro_nodes
            }
            logger.debug("ADO %s — repro_steps mapped (ADF)", ado_id)

        # Possible Solutions is merged into description only — field not updated.

    # NOTE: Option/multiselect fields (Work Category, Team name(s), Class,
    # Application, Vertical(s), components) are set via update_issue_optional_fields()
    # after creation so a bad option value never blocks the issue from being created.

    return payload


def update_issue_optional_fields(
    session: requests.Session,
    config: dict,
    jira_key: str,
    row: pd.Series,
    ado_id: str,
) -> None:
    """Update option-type and config-dependent fields after issue creation.

    Fields updated (all via a single PUT to avoid blocking issue creation):
      - components          (array)        JIRA_COMPONENT env var / CSV column
      - customfield_11503   (option)       Work Category          ← ADO work_category
      - customfield_20107   (array/option) Team name(s)           ← ADO team_name
      - customfield_22607   (option)       Class                  ← ADO work_subcategory
      - customfield_18255   (array/option) Application            ← ADO product_impacted
      - customfield_20405   (array/option) Vertical(s)            ← ADO product_category
    """
    fields_to_update: dict = {}

    # Components — build list from CSV value (comma-separated) plus JIRA_COMPONENT fallback.
    component_raw = str(row.get("component", "") or "").strip()
    if _is_empty(component_raw):
        component_raw = JIRA_COMPONENT
    component_names = [c.strip() for c in component_raw.split(",") if c.strip()]
    if component_names:
        fields_to_update["components"] = [{"name": c} for c in component_names]

    # Work Category (customfield_11503 — single-select option)
    work_category_raw = str(row.get("work_category", "") or "").strip()
    if not _is_empty(work_category_raw):
        allowed_wc = _fetch_field_options_via_createmeta(session, config, _WORK_CATEGORY_FIELD_ID)
        resolved_wc = _resolve_work_category(work_category_raw, allowed_wc)
        if resolved_wc:
            logger.info(
                "Work Category: '%s' → '%s' for %s (ADO %s)",
                work_category_raw, resolved_wc, jira_key, ado_id,
            )
            fields_to_update[_WORK_CATEGORY_FIELD_ID] = {"value": resolved_wc}
        else:
            logger.warning(
                "Work Category skipped for %s (ADO %s) — '%s' has no valid Jira match.",
                jira_key, ado_id, work_category_raw,
            )

    # Team name(s) (customfield_20107 — array/option, semicolon-separated in ADO)
    team_name_raw = str(row.get("team_name", "") or "").strip()
    if not _is_empty(team_name_raw):
        allowed_teams = _fetch_field_options_via_createmeta(session, config, _TEAM_NAME_FIELD_ID)
        resolved_teams = _resolve_team_names(team_name_raw, allowed_teams)
        if resolved_teams:
            logger.info(
                "Team name(s): %s for %s (ADO %s)",
                [t["value"] for t in resolved_teams], jira_key, ado_id,
            )
            fields_to_update[_TEAM_NAME_FIELD_ID] = resolved_teams
        else:
            logger.warning(
                "Team name(s) skipped for %s (ADO %s) — no valid values from '%s'.",
                jira_key, ado_id, team_name_raw,
            )

    # Class (customfield_22607 — single-select option) ← ADO work_subcategory
    work_subcategory_raw = str(row.get("work_subcategory", "") or "").strip()
    if not _is_empty(work_subcategory_raw):
        allowed_class = _fetch_field_options_via_createmeta(session, config, _CLASS_FIELD_ID)
        resolved_class = _resolve_single_option(work_subcategory_raw, allowed_class, "Class")
        if resolved_class:
            logger.info("Class: '%s' for %s (ADO %s)", resolved_class, jira_key, ado_id)
            fields_to_update[_CLASS_FIELD_ID] = {"value": resolved_class}

    # Application (customfield_18255 — array/option) ← ADO product_impacted
    product_impacted_raw = str(row.get("product_impacted", "") or "").strip()
    if not _is_empty(product_impacted_raw):
        allowed_app = _fetch_field_options_via_createmeta(session, config, _APPLICATION_FIELD_ID)
        resolved_apps = _resolve_array_options(product_impacted_raw, allowed_app, "Application")
        if resolved_apps:
            logger.info(
                "Application: %s for %s (ADO %s)",
                [a["value"] for a in resolved_apps], jira_key, ado_id,
            )
            fields_to_update[_APPLICATION_FIELD_ID] = resolved_apps
        else:
            logger.warning(
                "Application skipped for %s (ADO %s) — no valid values from '%s'.",
                jira_key, ado_id, product_impacted_raw,
            )

    # Vertical(s) (customfield_20405 — array/option) ← ADO product_category
    product_category_raw = str(row.get("product_category", "") or "").strip()
    if not _is_empty(product_category_raw):
        allowed_vert = _fetch_field_options_via_createmeta(session, config, _VERTICALS_FIELD_ID)
        resolved_verts = _resolve_array_options(product_category_raw, allowed_vert, "Vertical(s)")
        if resolved_verts:
            logger.info(
                "Vertical(s): %s for %s (ADO %s)",
                [v["value"] for v in resolved_verts], jira_key, ado_id,
            )
            fields_to_update[_VERTICALS_FIELD_ID] = resolved_verts
        else:
            logger.warning(
                "Vertical(s) skipped for %s (ADO %s) — no valid values from '%s'.",
                jira_key, ado_id, product_category_raw,
            )

    if not fields_to_update:
        return

    url = _api_url(config, f"issue/{jira_key}")
    resp = _safe_request(
        session,
        "PUT",
        url,
        f"update optional fields for {jira_key} (ADO {ado_id})",
        json={"fields": fields_to_update},
        headers={"Content-Type": "application/json"},
    )

    if resp is None:
        logger.warning(
            "Optional field update failed for %s (ADO %s) — issue was still created. "
            "Fields attempted: %s. Components attempted: %s in project %s.",
            jira_key, ado_id, list(fields_to_update.keys()),
            component_names, config["project_key"],
        )


def _process_issue_images(
    session: requests.Session,
    config: dict,
    jira_key: str,
    row: pd.Series,
) -> None:
    """Upload images embedded in ADO HTML fields to Jira and update the description.

    Called after issue creation so we have a valid jira_key for the attachment API.
    For each HTML field that may contain <img> tags:
      1. Download the image (using ADO auth if hosted on dev.azure.com)
      2. Upload it as a Jira attachment
      3. Patch the HTML so old src URLs point to the new Jira content URLs
      4. Rebuild the description ADF and update the issue via PUT

    If no images are found, or all uploads fail, the function is a no-op.
    Failures are logged as warnings and never raise — issue content is preserved.
    """
    issue_type = ISSUE_TYPE_MAP.get(str(row.get("type", "")).strip(), "Task")

    if issue_type == "Story":
        html_fields = ["description", "analysis", "acceptance_criteria"]
    elif issue_type == "Bug":
        html_fields = ["system_info", "analysis", "repro_steps", "proposed_fix"]
    else:
        html_fields = ["description"]

    # Collect a combined old→new URL map across all relevant HTML fields
    all_url_map: dict[str, str] = {}
    for field_name in html_fields:
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

    # Patch each HTML field in the row, then rebuild description ADF
    patched = row.copy()
    for field_name in html_fields:
        val = str(patched.get(field_name, "") or "")
        if val.strip() not in ("", "nan"):
            patched[field_name] = replace_image_urls(val, all_url_map)

    fields_update: dict = {}

    new_desc = build_description_adf(patched, issue_type)
    if new_desc:
        fields_update["description"] = new_desc

    # Rebuild the separate rich-text fields that also carry images
    if issue_type == "Story":
        ac_nodes = _markdown_to_adf_nodes(
            convert_html_to_markdown(str(patched.get("acceptance_criteria", "") or ""))
        )
        if ac_nodes:
            fields_update[_ACCEPTANCE_CRITERIA_FIELD_ID] = {
                "version": 1, "type": "doc", "content": ac_nodes
            }
    elif issue_type == "Bug":
        repro_nodes = _markdown_to_adf_nodes(
            convert_html_to_markdown(str(patched.get("repro_steps", "") or ""))
        )
        if repro_nodes:
            fields_update[_STEPS_TO_REPRODUCE_FIELD_ID] = {
                "version": 1, "type": "doc", "content": repro_nodes
            }
        # Possible Solutions is merged into description — field not updated here.

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


def create_issue(
    session: requests.Session,
    config: dict,
    row: pd.Series,
) -> str | None:
    """Create a single Jira issue from a workitems.csv row.

    Steps:
      1. Resolve assignee and reporter by email → accountId
      2. Build and POST the issue creation payload (with assignee when available)
      3. If creation fails AND an assignee was included: strip the assignee
         field and retry exactly once.  Jira rejects assignees when the user
         has no project role or the issue type disallows assignment.
      4. On success, attempt to update option-type fields separately.

    Returns the new Jira issue key (e.g. 'UWP-101'), or None on failure.
    """
    ado_id = str(row.get("id", "")).strip()

    assignee_id = search_user(session, config, row.get("assigned_to_email"))
    reporter_id = search_user(session, config, row.get("created_by_email"))

    payload = _build_issue_payload(config, row, assignee_id, reporter_id)

    url = _api_url(config, "issue")
    resp = _safe_request(
        session,
        "POST",
        url,
        f"create issue for ADO {ado_id}",
        json=payload,
        headers={"Content-Type": "application/json"},
    )

    # --- Assignee fallback (one retry only) ----------------------------------
    # If creation failed and an assignee was set, retry without it.
    # We don't inspect the error body because Jira's assignee rejection message
    # varies across versions; a failed create with assignee_id present is a
    # reliable-enough signal to warrant the retry.
    if resp is None and assignee_id:
        logger.warning(
            "Assignee failed for ADO %s — retrying without assignee (was: %s)",
            ado_id,
            row.get("assigned_to_email", ""),
        )
        payload_no_assignee = {
            **payload,
            "fields": {k: v for k, v in payload["fields"].items() if k != "assignee"},
        }
        resp = _safe_request(
            session,
            "POST",
            url,
            f"create issue for ADO {ado_id} (retry, no assignee)",
            json=payload_no_assignee,
            headers={"Content-Type": "application/json"},
        )
    # -------------------------------------------------------------------------

    if resp is None:
        logger.error("Failed to create Jira issue for ADO work item %s", ado_id)
        return None

    jira_key = resp.json().get("key")
    logger.info("Created Jira issue %s for ADO work item %s", jira_key, ado_id)

    # Update option-type fields (components, Work Category) in a separate PUT.
    # Work Category is set here because it is not on the create screen.
    update_issue_optional_fields(session, config, jira_key, row, ado_id)

    # Verify Work Category was persisted (runs after the PUT that sets it)
    work_category = str(row.get("work_category", "") or "").strip()
    if not _is_empty(work_category):
        _verify_work_category(session, config, jira_key, work_category)

    # Upload images embedded in HTML fields and update description with Jira URLs
    _process_issue_images(session, config, jira_key, row)

    return jira_key


def _verify_work_category(
    session: requests.Session,
    config: dict,
    jira_key: str,
    expected_value: str,
) -> None:
    """Read back the created issue and verify Work Category was persisted."""
    url = _api_url(config, f"issue/{jira_key}")
    resp = _safe_request(
        session, "GET", url,
        f"verify Work Category on {jira_key}",
        params={"fields": _WORK_CATEGORY_FIELD_ID},
    )
    if resp is None:
        logger.warning("Could not verify Work Category for %s — GET failed", jira_key)
        return

    fields = resp.json().get("fields", {})
    field_obj = fields.get(_WORK_CATEGORY_FIELD_ID)
    # customfield_11503 is a select list — Jira returns {"value": "...", "id": "..."}
    actual = field_obj.get("value") if isinstance(field_obj, dict) else None

    if actual:
        logger.info(
            "Work Category successfully set for issue %s (value: '%s')", jira_key, actual
        )
    else:
        logger.warning(
            "Work Category NOT visible for %s — expected '%s' but field is empty/null. "
            "Check: (1) field %s is on the issue create/edit screen; "
            "(2) the value is in the field's allowed option list.",
            jira_key, expected_value, _WORK_CATEGORY_FIELD_ID,
        )


def validate_work_category_field(
    session: requests.Session,
    config: dict,
) -> None:
    """Pre-flight check: verify that customfield_11503 (Work Category) exists in Jira.

    Calls GET /rest/api/3/field and inspects the result.
    """
    url = _api_url(config, "field")
    resp = _safe_request(session, "GET", url, "validate Work Category field")

    if resp is None:
        logger.warning("Could not fetch Jira field list — skipping Work Category validation")
        return

    fields = resp.json()
    wc_field = None
    for f in fields:
        if f.get("id") == _WORK_CATEGORY_FIELD_ID:
            wc_field = f
            break

    if wc_field:
        logger.info(
            "Work Category field FOUND — id: %s, name: '%s', type: %s, schema: %s",
            wc_field.get("id"),
            wc_field.get("name"),
            wc_field.get("schema", {}).get("type", "unknown"),
            wc_field.get("schema", {}),
        )
        logger.info(
            "If the field exists but values are not visible, it may not be added "
            "to the issue create/edit screen configuration for this project."
        )
    else:
        logger.error(
            "Work Category field NOT FOUND in Jira configuration (expected %s). "
            "Please verify: (1) the field exists in Jira admin; "
            "(2) the correct custom field ID is being used.",
            _WORK_CATEGORY_FIELD_ID,
        )


# ---------------------------------------------------------------------------
# Comments
# ---------------------------------------------------------------------------


def add_comment(
    session: requests.Session,
    config: dict,
    jira_key: str,
    comment_text: str,
    created_by: str | None = None,
    created_date: str | None = None,
) -> bool:
    """Add a comment to a Jira issue using ADF format.

    Prepends author/date metadata header when available.
    Returns True on success, False otherwise.
    """
    if _is_empty(comment_text):
        return False

    # Comments arrive as plain text with [Image: URL] placeholders produced by
    # extract_workitems_data.py.  Upload each referenced image to Jira and
    # replace the placeholder URL so the comment body references the attachment.
    img_placeholder = re.compile(r"\[Image:\s*([^\]]+)\]")
    img_urls = list(dict.fromkeys(  # preserve order, deduplicate
        m.group(1).strip() for m in img_placeholder.finditer(comment_text)
    ))
    if img_urls:
        for img_url in img_urls:
            new_url = _upload_image_to_jira(session, config, jira_key, img_url)
            if new_url:
                comment_text = comment_text.replace(
                    f"[Image: {img_url}]", f"[Image: {new_url}]"
                )
            else:
                logger.warning(
                    "Comment image upload failed for %s on %s — original URL kept",
                    img_url, jira_key,
                )

    header_parts = []
    if created_by and not _is_empty(created_by):
        header_parts.append(f"Author: {created_by}")
    if created_date and not _is_empty(created_date):
        header_parts.append(f"Date: {created_date}")

    full_text = comment_text
    if header_parts:
        full_text = "\n".join(header_parts) + "\n\n" + comment_text

    comment_nodes = _text_with_images_to_adf_nodes(str(full_text))
    if not comment_nodes:
        return False

    payload = {
        "body": {
            "version": 1,
            "type": "doc",
            "content": comment_nodes,
        }
    }

    url = _api_url(config, f"issue/{jira_key}/comment")
    resp = _safe_request(
        session,
        "POST",
        url,
        f"add comment to {jira_key}",
        json=payload,
        headers={"Content-Type": "application/json"},
    )

    if resp:
        logger.debug("Comment added to %s", jira_key)
        return True
    return False


def process_comments(
    session: requests.Session,
    config: dict,
    comments_df: pd.DataFrame,
    mapping: dict,
) -> None:
    """Add all comments to their respective Jira issues."""
    if comments_df.empty:
        logger.info("No comments to process.")
        return

    total_in_csv = len(comments_df)
    added = 0
    skipped_no_mapping = 0
    skipped_empty = 0
    failed_api = 0

    for _, row in comments_df.iterrows():
        ado_id = str(row.get("workitem_id", "")).strip()
        jira_key = mapping.get(ado_id)

        if not jira_key:
            logger.warning(
                "COMMENT SKIPPED — no Jira mapping for ADO %s (comment by: %s)",
                ado_id, row.get("created_by", "unknown"),
            )
            skipped_no_mapping += 1
            continue

        comment_text = str(row.get("comment", "") or "")
        if _is_empty(comment_text):
            logger.warning(
                "COMMENT SKIPPED — empty text for ADO %s → %s (by: %s, date: %s)",
                ado_id, jira_key, row.get("created_by", ""), row.get("created_date", ""),
            )
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
            logger.warning(
                "COMMENT FAILED — API error adding comment to %s (ADO %s, by: %s)",
                jira_key, ado_id, row.get("created_by", ""),
            )
            failed_api += 1

    logger.info("=" * 60)
    logger.info("COMMENT SUMMARY")
    logger.info("  Total in CSV         : %d", total_in_csv)
    logger.info("  Successfully added   : %d", added)
    logger.info("  Skipped (no mapping) : %d", skipped_no_mapping)
    logger.info("  Skipped (empty text) : %d", skipped_empty)
    logger.info("  Failed (API error)   : %d", failed_api)
    missing = total_in_csv - added
    if missing > 0:
        logger.warning("  MISSING COMMENTS     : %d (%.1f%%)", missing, missing / total_in_csv * 100)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Attachments
# ---------------------------------------------------------------------------


def upload_attachment(
    session: requests.Session,
    config: dict,
    jira_key: str,
    file_path: Path,
) -> bool:
    """Upload a single file as an attachment to a Jira issue.

    Returns True on success, False otherwise — never raises.
    """
    if not file_path.exists():
        logger.warning("Attachment file not found: %s — skipping", file_path)
        return False

    url = _api_url(config, f"issue/{jira_key}/attachments")

    try:
        with open(file_path, "rb") as f:
            resp = _safe_request(
                session,
                "POST",
                url,
                f"upload attachment {file_path.name} to {jira_key}",
                files={"file": (file_path.name, f, "application/octet-stream")},
                headers={"X-Atlassian-Token": "no-check"},
            )
        if resp:
            logger.info("Uploaded %s to %s", file_path.name, jira_key)
            return True
        return False

    except OSError as exc:
        logger.error("Could not read attachment %s: %s", file_path, exc)
        return False


def process_attachments(
    session: requests.Session,
    config: dict,
    mapping: dict,
    attachment_base: Path,
) -> None:
    """Upload all local attachments from the output/attachments directory."""
    if not attachment_base.exists():
        logger.info("Attachment directory not found — skipping attachment upload.")
        return

    total = 0
    failed = 0

    for ado_folder in sorted(attachment_base.iterdir()):
        if not ado_folder.is_dir():
            continue

        ado_id = ado_folder.name
        jira_key = mapping.get(ado_id)

        if not jira_key:
            logger.debug("No Jira mapping for ADO %s — skipping attachments", ado_id)
            continue

        for file_path in sorted(ado_folder.iterdir()):
            if file_path.is_file():
                success = upload_attachment(session, config, jira_key, file_path)
                if success:
                    total += 1
                else:
                    failed += 1

    logger.info("Attachments — uploaded: %d | failed: %d", total, failed)


# ---------------------------------------------------------------------------
# Relations / issue links
# ---------------------------------------------------------------------------


def create_relation(
    session: requests.Session,
    config: dict,
    source_key: str,
    target_key: str,
    relation_category: str,
) -> bool:
    """Create an issue link between two Jira issues.

    Link type defaults to "Relates" — guaranteed to exist in all Jira instances.
    Returns True on success, False otherwise — never raises.
    """
    link_type_name = RELATION_LINK_TYPE_MAP.get(relation_category, "Relates")

    payload = {
        "type": {"name": link_type_name},
        "inwardIssue":  {"key": source_key},
        "outwardIssue": {"key": target_key},
    }

    url = _api_url(config, "issueLink")
    resp = _safe_request(
        session,
        "POST",
        url,
        f"link {source_key} → {target_key} ({relation_category})",
        json=payload,
        headers={"Content-Type": "application/json"},
    )

    if resp is not None:
        logger.info("Linked %s → %s (%s)", source_key, target_key, relation_category)
        return True
    return False


def process_relations(
    session: requests.Session,
    config: dict,
    relations_df: pd.DataFrame,
    mapping: dict,
) -> None:
    """Recreate all non-parent/non-attachment relations as Jira issue links.

    Skipped categories:
      - parent     → handled by process_epics (Epic Link field)
      - attachment → local files; not a link
      - artifact   → build artifacts; not linkable
    """
    if relations_df.empty:
        logger.info("No relations to process.")
        return

    skip_categories = {"parent", "attachment", "artifact", "related", "child", "other"}

    total = 0
    skipped_category = 0
    skipped_no_mapping = 0
    failed_api = 0

    for _, row in relations_df.iterrows():
        category = str(row.get("relation_category", "")).strip().lower()

        if category in skip_categories:
            skipped_category += 1
            continue

        ado_source = str(row.get("workitem_id", "")).strip()
        ado_target  = str(row.get("target", "")).strip()

        if _is_empty(ado_target):
            skipped_no_mapping += 1
            continue

        source_key = mapping.get(ado_source)
        target_key  = mapping.get(ado_target)

        if not source_key:
            logger.info(
                "RELATION SKIPPED — source ADO %s not in Jira mapping (not migrated)",
                ado_source,
            )
            skipped_no_mapping += 1
            continue

        if not target_key:
            logger.info(
                "RELATION SKIPPED — target ADO %s not in Jira mapping (not migrated); "
                "source %s (%s)",
                ado_target, ado_source, category,
            )
            skipped_no_mapping += 1
            continue

        success = create_relation(session, config, source_key, target_key, category)
        if success:
            total += 1
        else:
            failed_api += 1

    logger.info("=" * 60)
    logger.info("RELATIONS SUMMARY")
    logger.info("  Created              : %d", total)
    logger.info("  Skipped (category)   : %d  (only Epic↔issue links are created; related/child/attachment/artifact skipped)", skipped_category)
    logger.info("  Skipped (no mapping) : %d  (target ticket not in this migration batch)", skipped_no_mapping)
    logger.info("  Failed (API error)   : %d", failed_api)
    if skipped_no_mapping:
        logger.info(
            "  NOTE: %d relation(s) point to tickets outside this migration batch. "
            "Re-run after migrating all linked tickets to create these links.",
            skipped_no_mapping,
        )
    logger.info("=" * 60)




# ---------------------------------------------------------------------------
# Mapping persistence
# ---------------------------------------------------------------------------


def load_mapping() -> dict:
    """Load the ADO → Jira key mapping from disk (if it exists)."""
    if MAPPING_FILE.exists():
        try:
            with open(MAPPING_FILE, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            logger.info("Loaded existing mapping with %d entries.", len(mapping))
            return mapping
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not load mapping file: %s — starting fresh.", exc)
    return {}


def save_mapping(mapping: dict) -> None:
    """Persist the ADO → Jira key mapping to disk."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(MAPPING_FILE, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2)
        logger.info("Mapping saved to %s (%d entries)", MAPPING_FILE, len(mapping))
    except OSError as exc:
        logger.error("Failed to save mapping: %s", exc)


# ---------------------------------------------------------------------------
# CSV loading helpers
# ---------------------------------------------------------------------------


def _load_csv(filename: str, required_columns: list[str]) -> pd.DataFrame:
    """Load a CSV from the output directory.

    Returns an empty DataFrame with the required columns if the file is
    missing or malformed.
    """
    path = OUTPUT_DIR / filename
    if not path.exists():
        logger.warning("File not found: %s — skipping.", path)
        return pd.DataFrame(columns=required_columns)

    try:
        df = pd.read_csv(path, dtype=str)
        df.columns = df.columns.str.strip().str.lower()
        logger.info("Loaded %s — %d rows", filename, len(df))
        return df
    except Exception as exc:
        logger.error("Failed to load %s: %s — skipping.", filename, exc)
        return pd.DataFrame(columns=required_columns)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point — runs the full ADO → Jira migration in order:

      1. Create issues (all safe fields)
      2. Update option/component fields per issue (post-create)
      3. Add comments
      4. Upload attachments
      5. Recreate relations (related/child)

    Idempotent: already-mapped ADO IDs are skipped on re-run.
    """
    # -- Configuration -------------------------------------------------------
    config = load_env()
    session = _build_session(config)

    logger.info("ADO org: %s | project: %s", ADO_ORG, ADO_PROJECT)
    logger.info("Jira project: %s | component: '%s'",
                config["project_key"], JIRA_COMPONENT or "(none)")

    # -- Pre-flight: validate Work Category field ----------------------------
    logger.info("=" * 60)
    logger.info("PRE-FLIGHT — Validating Work Category field in Jira")
    logger.info("=" * 60)
    validate_work_category_field(session, config)

    # -- Load CSVs -----------------------------------------------------------
    workitems_df = _load_csv(
        "workitems.csv",
        ["id", "type", "title", "state", "description",
         "created_by_email", "assigned_to_email", "tags",
         "acceptance_criteria", "repro_steps", "system_info",
         "analysis", "proposed_fix",
         "story_points", "target_date",
         "work_category", "product_impacted", "product_category", "team_name", "work_subcategory"],
    )
    comments_df = _load_csv(
        "comments.csv",
        ["workitem_id", "comment", "created_by", "created_date"],
    )
    relations_df = _load_csv(
        "relations.csv",
        ["workitem_id", "relation_type", "relation_category", "target", "url"],
    )

    if workitems_df.empty:
        logger.error("workitems.csv is empty or missing — nothing to import.")
        return

    # -- Step 1: Create issues -----------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 1 — Creating Jira issues (%d work items)", len(workitems_df))
    logger.info("=" * 60)

    # Load any existing mapping so a re-run skips already-created issues
    mapping: dict = load_mapping()

    created = 0
    failed  = 0

    for _, row in workitems_df.iterrows():
        ado_id = str(row.get("id", "")).strip()

        if _is_empty(ado_id):
            logger.warning("Row with missing ADO id — skipping.")
            failed += 1
            continue

        if ado_id in mapping:
            logger.info(
                "ADO %s already mapped to %s — skipping creation.",
                ado_id, mapping[ado_id],
            )
            continue

        jira_key = create_issue(session, config, row)

        if jira_key:
            mapping[ado_id] = jira_key
            created += 1
        else:
            failed += 1

    save_mapping(mapping)
    logger.info("Issues — created: %d | failed: %d", created, failed)

    # -- Step 2: Add comments ------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 2 — Adding comments (%d rows)", len(comments_df))
    logger.info("=" * 60)
    process_comments(session, config, comments_df, mapping)

    # -- Step 3: Upload attachments ------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 3 — Uploading attachments")
    logger.info("=" * 60)
    process_attachments(session, config, mapping, OUTPUT_DIR / "attachments")

    # -- Step 4: Recreate relations ------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 4 — Creating issue links (%d relations)", len(relations_df))
    logger.info("=" * 60)
    process_relations(session, config, relations_df, mapping)

    logger.info("=" * 60)
    logger.info("Migration complete. Run epic_linker.py to set Epic Links.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
