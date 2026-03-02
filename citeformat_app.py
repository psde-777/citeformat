"""
citeformat_app.py — Streamlit GUI for citeformat
Run with:  streamlit run citeformat_app.py
"""

import streamlit as st
import sys, io, os, tempfile, datetime, base64, json, hashlib, urllib.request, urllib.parse, re

# ── Must be first Streamlit call ─────────────────────────────────────────────
st.set_page_config(
    page_title="CiteFormat",
    page_icon="📑",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# REDIS CACHE LAYER
# =============================================================================
# Uses Upstash Redis via REST API for shared persistent caching.
# Set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN in
# Streamlit Cloud → App settings → Secrets, like:
#
#   [upstash]
#   UPSTASH_REDIS_REST_URL = "https://xxx.upstash.io"
#   UPSTASH_REDIS_REST_TOKEN = "your-token"
#
# If secrets are not configured the app works normally without caching.
# =============================================================================

# =============================================================================
# BIBTEX CACHE HELPERS
# =============================================================================
# We store only the fields needed for citation formatting as a compact BibTeX
# string. Key = "bib:<doi>" (lowercase). This is ~10x smaller than raw CrossRef
# JSON and human-readable in the Upstash Data Browser.
#
# BibTeX field set stored:
#   doi, title, author (Last, F. and ...), journal, year, volume, number,
#   pages, publisher
#
# On read we parse back to a CrossRef-compatible dict so the rest of the
# app needs no changes.
# =============================================================================

def _upstash_credentials():
    """Return (base_url, token) from Streamlit secrets, or (None, None)."""
    try:
        secrets  = st.secrets.get("upstash", {})
        base_url = secrets.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
        token    = secrets.get("UPSTASH_REDIS_REST_TOKEN", "")
        return (base_url, token) if base_url and token else (None, None)
    except Exception:
        return None, None

def _upstash_post(commands):
    """Send a pipeline POST to Upstash. commands = list of Redis command lists."""
    import sys, traceback
    try:
        base_url, token = _upstash_credentials()
        if not base_url:
            print("[upstash] No credentials found in secrets.toml", file=sys.stderr)
            return None
        body = json.dumps(commands).encode("utf-8")
        req  = urllib.request.Request(
            base_url + "/pipeline",
            data=body, method="POST",
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            raw      = r.read().decode()
            response = json.loads(raw)
            # Log any Redis-level errors returned in the response
            if isinstance(response, list):
                for i, item in enumerate(response):
                    if isinstance(item, dict) and item.get("error"):
                        print(f"[upstash] Redis error on command {commands[i]}: {item['error']}", file=sys.stderr)
            return response
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace")
        print(f"[upstash] HTTP {e.code} {e.reason}: {body_text}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[upstash] Unexpected error: {type(e).__name__}: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None

def _redis_get(key):
    result = _upstash_post([["GET", key]])
    if result and isinstance(result, list):
        return result[0].get("result")   # str or None
    return None

def _redis_set(key, value, ex=None):
    """value must be a plain str."""
    import sys
    cmd = ["SET", key, value]
    if ex:
        cmd += ["EX", str(ex)]
    result = _upstash_post([cmd])
    if result and isinstance(result, list):
        status = result[0].get("result")
        if status != "OK":
            print(f"[upstash] SET {key!r} returned unexpected status: {status}", file=sys.stderr)

def _redis_ping():
    result = _upstash_post([["PING"]])
    return bool(result)

# ── BibTeX serialisation ──────────────────────────────────────────────────────

def _msg_to_bibtex(doi, msg):
    """
    Convert a CrossRef message dict to a compact BibTeX string.
    Stored value is small and human-readable in the Upstash Data Browser.
    """
    def _clean(s):
        """Remove BibTeX special chars that would break parsing."""
        return str(s).replace("{", "").replace("}", "").replace('"', "'").replace("\n", " ").strip()

    # Authors: "Last, F. I. and Last2, F."
    authors = msg.get("author", [])
    author_parts = []
    for a in authors:
        family = a.get("family", a.get("name", ""))
        given  = a.get("given", "")
        initials = " ".join(p[0] + "." for p in given.split()) if given else ""
        author_parts.append(f"{family}, {initials}".strip(", "))
    author_str = " and ".join(author_parts) if author_parts else "Unknown"

    title     = _clean((msg.get("title") or [""])[0])
    journal   = _clean((msg.get("container-title") or msg.get("publisher") or [""])[0] if isinstance(msg.get("container-title") or msg.get("publisher"), list) else msg.get("container-title", msg.get("publisher", "")))
    year      = ""
    for field in ("published", "published-print", "published-online", "issued"):
        dp = msg.get(field, {}).get("date-parts")
        if dp and dp[0] and dp[0][0]:
            year = str(dp[0][0]); break
    volume    = _clean(msg.get("volume", ""))
    number    = _clean(msg.get("issue", ""))
    pages     = _clean(msg.get("page", ""))
    publisher = _clean(msg.get("publisher", ""))

    # Cite key: first author last name + year
    first_family = (authors[0].get("family", "unknown") if authors else "unknown").lower()
    first_family = re.sub(r"[^a-z0-9]", "", first_family)
    cite_key = f"{first_family}{year}"

    fields = [
        f"  doi       = {{{doi}}}",
        f"  title     = {{{title}}}",
        f"  author    = {{{author_str}}}",
        f"  journal   = {{{journal}}}",
        f"  year      = {{{year}}}",
    ]
    if volume:    fields.append(f"  volume    = {{{volume}}}")
    if number:    fields.append(f"  number    = {{{number}}}")
    if pages:     fields.append(f"  pages     = {{{pages}}}")
    if publisher: fields.append(f"  publisher = {{{publisher}}}")

    return "@article{" + cite_key + ",\n" + ",\n".join(fields) + "\n}"


def _bibtex_to_msg(bibtex_str):
    """
    Parse a stored BibTeX string back to a CrossRef-compatible dict.
    Returns None if parsing fails.
    """
    try:
        def _field(name):
            m = re.search(r"" + name + r"\s*=\s*\{([^}]*)\}", bibtex_str, re.IGNORECASE)
            return m.group(1).strip() if m else ""

        doi       = _field("doi")
        title     = _field("title")
        journal   = _field("journal")
        year      = _field("year")
        volume    = _field("volume")
        number    = _field("number")
        pages     = _field("pages")
        publisher = _field("publisher")
        author_raw = _field("author")

        # Parse authors: "Last, F. I. and Last2, F2." → list of dicts
        authors = []
        if author_raw:
            for part in author_raw.split(" and "):
                part = part.strip()
                if "," in part:
                    fam, giv = part.split(",", 1)
                    authors.append({"family": fam.strip(), "given": giv.strip()})
                elif part:
                    authors.append({"family": part})

        if not title and not authors:
            return None

        msg = {
            "DOI":              doi,
            "title":            [title] if title else [],
            "author":           authors,
            "container-title":  [journal] if journal else [],
            "publisher":        publisher,
            "volume":           volume,
            "issue":            number,
            "page":             pages,
            "published":        {"date-parts": [[int(year)]]} if year.isdigit() else {},
        }
        return msg
    except Exception:
        return None


# ── Cache read / write ────────────────────────────────────────────────────────

def _doi_cache_key(doi):
    return "bib:" + doi.strip().lower()

def _cache_get_doi(doi):
    """Return CrossRef-compatible dict from cache, or None."""
    raw = _redis_get(_doi_cache_key(doi))
    if not raw or not raw.strip().startswith("@"):
        return None
    return _bibtex_to_msg(raw)

def _cache_set_doi(doi, msg):
    """Convert msg to BibTeX and store under key bib:<doi>."""
    import sys
    if not isinstance(msg, dict):
        print(f"[cache] _cache_set_doi skipped — msg is not a dict: {type(msg)}", file=sys.stderr)
        return
    try:
        bibtex = _msg_to_bibtex(doi, msg)
        key    = _doi_cache_key(doi)
        _redis_set(key, bibtex)
        # Verify write by reading back
        stored = _redis_get(key)
        if stored and stored.strip().startswith("@"):
            print(f"[cache] ✓ Saved {key} ({len(bibtex)} bytes)", file=sys.stderr)
        else:
            print(f"[cache] ✗ Write failed for {key} — readback: {repr(stored)[:80]}", file=sys.stderr)
    except Exception as e:
        print(f"[cache] ✗ Exception in _cache_set_doi({doi}): {e}", file=sys.stderr)

def _ascii_fold(s):
    """Normalize unicode to ASCII for fuzzy matching (e.g. Ebenhöh → ebenh)."""
    import unicodedata
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()

def _fuzzy_cache_search(info):
    """
    Search the bib: cache for entries matching a fuzzy query.
    Uses SCAN to iterate all bib: keys, then scores title/author/year matches.
    Returns list of (doi, msg) tuples for top matches, or [].
    """
    import sys
    try:
        # SCAN all bib: keys
        cursor   = "0"
        bib_keys = []
        for _ in range(20):
            result = _upstash_post([["SCAN", cursor, "MATCH", "bib:*", "COUNT", "100"]])
            if not result or not isinstance(result, list):
                break
            inner = result[0].get("result", [])
            if not isinstance(inner, list) or len(inner) < 2:
                break
            cursor    = str(inner[0])
            bib_keys += inner[1] if isinstance(inner[1], list) else []
            if cursor == "0":
                break

        print(f"[fuzzy] SCAN found {len(bib_keys)} bib: keys", file=sys.stderr)
        if not bib_keys:
            return []

        # Build clean search terms — only meaningful words, ASCII-folded
        terms = []
        for field in ("title", "author", "journal"):
            v = info.get(field, "")
            if v:
                folded = _ascii_fold(v)
                words  = re.sub(r"[^a-z0-9 ]", " ", folded).split()
                # Keep only alphabetic words longer than 3 chars (skip DOI fragments, numbers)
                terms += [w for w in words if len(w) > 3 and not w.isdigit() and re.search(r"[a-z]", w)]
        year = str(info.get("year", ""))

        # Deduplicate terms
        terms = list(dict.fromkeys(terms))

        print(f"[fuzzy] clean terms={terms} year={year!r}", file=sys.stderr)
        if not terms:
            return []

        # Fetch all bib: values in one pipeline call
        keys_to_fetch = bib_keys[:50]
        cmds   = [["GET", k] for k in keys_to_fetch]
        values = _upstash_post(cmds)
        if not values:
            return []

        matches = []
        for key, val_obj in zip(keys_to_fetch, values):
            raw = val_obj.get("result", "") if isinstance(val_obj, dict) else ""
            if not raw or not raw.strip().startswith("@"):
                continue
            # ASCII-fold the stored BibTeX for comparison
            raw_folded = _ascii_fold(raw)
            hit_count  = sum(1 for t in terms if t in raw_folded)
            if year and year in raw:
                hit_count += 2
            # Need at least 2 meaningful term hits OR year + 1 term
            threshold = max(2, len(terms) // 3)
            if hit_count >= threshold:
                doi = key[4:]   # strip "bib:" prefix
                msg = _bibtex_to_msg(raw)
                if msg:
                    msg["_cache_score"] = hit_count
                    matches.append((doi, msg))

        matches.sort(key=lambda x: x[1].get("_cache_score", 0), reverse=True)
        print(f"[fuzzy] → {len(matches)} match(es) from {len(keys_to_fetch)} keys", file=sys.stderr)
        return matches[:3]
    except Exception as e:
        print(f"[cache] fuzzy search error: {e}", file=sys.stderr)
        import traceback; traceback.print_exc(file=sys.stderr)
        return []

def _cache_stats():
    if "cache_hits"   not in st.session_state: st.session_state.cache_hits   = 0
    if "cache_misses" not in st.session_state: st.session_state.cache_misses = 0
    return st.session_state.cache_hits, st.session_state.cache_misses

# ── Cached wrappers ───────────────────────────────────────────────────────────

def _ensure_dict(value, label=""):
    if isinstance(value, dict) and value:
        return value
    import sys
    print(f"[citeformat cache] WARNING: expected dict, got {type(value).__name__} {label!r}: {str(value)[:120]}", file=sys.stderr)
    return None

def cached_fetch_by_doi(doi):
    """fetch_by_doi with Redis cache. Returns (msg, raw_doi)."""
    raw    = doi.strip().lstrip("https://doi.org/").lstrip("http://dx.doi.org/")
    cached = _cache_get_doi(raw)
    if cached is not None:
        validated = _ensure_dict(cached, f"doi:{raw}")
        if validated is not None:
            st.session_state.cache_hits = st.session_state.get("cache_hits", 0) + 1
            return validated, raw
        # Cache returned bad data — delete it and fall through to live fetch
        _redis_set("bib:" + raw.strip().lower(), "")
    st.session_state.cache_misses = st.session_state.get("cache_misses", 0) + 1
    msg, rd = fetch_by_doi(doi)
    # Don't cache here — caller does it explicitly to avoid double writes
    return msg, rd

# search_crossref is called directly — no search-level caching

# ── Import all logic from citeformat.py (must be in the same folder) ─────────
try:
    from citeformat import (
        detect_input_type, resolve_input, search_crossref, fetch_by_doi,
        build_query, FORMATS,
        fmt_plain, fmt_apa, fmt_mla, fmt_chicago, fmt_vancouver,
        fmt_harvard, fmt_ieee, fmt_ama, fmt_acs, fmt_nature,
        _strip_markup, _html_markup, _md_passthrough,
        _build_highlight_targets, _apply_author_highlight_html,
        _apply_author_highlight_md, _apply_author_highlight_pdf,
        render_html, render_markdown, render_pdf,
        get_authors,
    )
except ImportError as e:
    st.error(f"Could not import citeformat.py — make sure it's in the same folder.\n\n{e}")
    st.stop()

# ── Custom CSS — refined academic aesthetic ───────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Lora:ital,wght@0,400;0,600;1,400&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* Page background */
.stApp {
    background: #f7f5f0;
}

/* Ensure container/card text is dark — exclude buttons and inputs */
[data-testid="stMain"] p,
[data-testid="stMain"] .stMarkdown p,
[data-testid="stMain"] .stMarkdown span,
[data-testid="stMain"] [data-testid="stVerticalBlock"] > div > p {
    color: #1c1917;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #1c1917;
    border-right: 1px solid #292524;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div:not([data-testid]),
[data-testid="stSidebar"] label {
    color: #e7e5e4 !important;
}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stTextInput label,
[data-testid="stSidebar"] .stTextArea label {
    color: #a8a29e !important;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
[data-testid="stSidebar"] h1, 
[data-testid="stSidebar"] h2, 
[data-testid="stSidebar"] h3 {
    color: #fafaf9 !important;
    font-family: 'Lora', serif !important;
}

/* Main headings */
h1 { font-family: 'Lora', serif !important; color: #1c1917 !important; }
h2 { font-family: 'Lora', serif !important; color: #292524 !important; }
h3 { font-family: 'DM Sans', sans-serif !important; color: #44403c !important; font-weight: 500 !important; }

/* Buttons */
.stButton > button {
    background: #1c1917 !important;
    color: #fafaf9 !important;
    border: none !important;
    border-radius: 6px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 500 !important;
    letter-spacing: 0.03em !important;
    padding: 0.5rem 1.5rem !important;
    transition: background 0.2s !important;
}
.stButton > button * {
    color: #fafaf9 !important;
}
.stButton > button:hover {
    background: #44403c !important;
    color: #fafaf9 !important;
}
.stButton > button:hover * {
    color: #fafaf9 !important;
}

/* Text areas and inputs */
.stTextArea textarea, .stTextInput input {
    background: #ffffff !important;
    border: 1px solid #d6d3d1 !important;
    border-radius: 6px !important;
    font-family: 'DM Sans', sans-serif !important;
    color: #1c1917 !important;
    cursor: text !important;
    caret-color: #1c1917 !important;
}

/* Select boxes */
.stSelectbox > div > div {
    background: #ffffff !important;
    border: 1px solid #d6d3d1 !important;
    border-radius: 6px !important;
    color: #1c1917 !important;
}
.stSelectbox > div > div > div,
.stSelectbox > div > div input,
.stSelectbox [data-baseweb="select"] span,
.stSelectbox [data-baseweb="select"] div {
    color: #1c1917 !important;
    background: #ffffff !important;
}
/* Sidebar selectbox — keep light text */
[data-testid="stSidebar"] .stSelectbox > div > div,
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] span,
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] div {
    color: #e7e5e4 !important;
    background: #2c2927 !important;
    border-color: #44403c !important;
}

/* Radio buttons */
.stRadio label span {
    color: #1c1917 !important;
}
[data-testid="stSidebar"] .stRadio label span {
    color: #e7e5e4 !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label {
    color: #e7e5e4 !important;
}

/* Progress / spinner */
.stSpinner > div { border-top-color: #1c1917 !important; }

/* Result card */
.ref-card {
    background: #ffffff;
    border: 1px solid #e7e5e4;
    border-left: 3px solid #1c1917;
    border-radius: 8px;
    padding: 1rem 1.25rem;
    margin-bottom: 0.75rem;
    font-family: 'Lora', serif;
    font-size: 0.92rem;
    line-height: 1.7;
    color: #292524;
}
.ref-card a { color: #1c6ef3; text-decoration: none; }
.ref-card a:hover { text-decoration: underline; }

/* Candidate pick card */
.candidate-card {
    background: #fafaf9;
    border: 1px solid #d6d3d1;
    border-radius: 8px;
    padding: 0.85rem 1rem;
    margin-bottom: 0.5rem;
    cursor: pointer;
}
.candidate-card:hover { border-color: #1c1917; }

/* Badge */
.badge {
    display: inline-block;
    background: #e7e5e4;
    color: #57534e;
    font-size: 0.7rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    padding: 2px 8px;
    border-radius: 99px;
    margin-left: 8px;
    vertical-align: middle;
}

/* Download button */
.stDownloadButton > button {
    background: #ffffff !important;
    color: #1c1917 !important;
    border: 1px solid #1c1917 !important;
    border-radius: 6px !important;
    font-family: 'DM Sans', sans-serif !important;
}
.stDownloadButton > button:hover {
    background: #f7f5f0 !important;
}

/* Divider */
hr { border-color: #e7e5e4 !important; }

/* Info / warning boxes */
.stAlert { border-radius: 8px !important; }

/* Hide Streamlit branding */
#MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# SESSION STATE INIT
# =============================================================================

def _init_state():
    defaults = {
        "lines":            [],       # raw input lines
        "entries":          [],       # list of (formatted_str, authors_meta)
        "pending_line":     None,     # line waiting for candidate selection
        "pending_idx":      None,
        "pending_candidates": [],
        "processing":       False,
        "done":             False,
        "current_line_idx": 0,
        "highlight_names":  [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


# =============================================================================
# SIDEBAR — settings
# =============================================================================

with st.sidebar:
    st.markdown("## 📑 CiteFormat")
    st.markdown("---")

    st.markdown("### Citation Style")
    format_labels = [label for label, _ in FORMATS.values()]
    format_keys   = list(FORMATS.keys())
    chosen_fmt_label = st.selectbox(
        "Style", format_labels, index=1, label_visibility="collapsed"
    )
    chosen_fmt_key = format_keys[format_labels.index(chosen_fmt_label)]
    formatter      = FORMATS[chosen_fmt_key][1]

    st.markdown("### Output Format")
    output_fmt = st.radio(
        "Output", ["Console (preview)", "Markdown", "HTML", "PDF"],
        label_visibility="collapsed"
    )

    st.markdown("### Highlight Authors")
    highlight_raw = st.text_area(
        "One name per line",
        placeholder="e.g.\nVaswani\nHinton",
        height=100,
        label_visibility="collapsed",
        help="Enter author surnames to bold in the output. One per line. Fuzzy matched."
    )
    highlight_names = [n.strip().lower() for n in highlight_raw.splitlines() if n.strip()]

    st.markdown("---")
    st.markdown(
        "<span style='color:#78716c;font-size:0.75rem;'>"
        "Uses the CrossRef public API.<br>Internet connection required."
        "</span>",
        unsafe_allow_html=True,
    )

    # Cache stats
    hits, misses = _cache_stats()
    if hits + misses > 0:
        pct = int(100 * hits / (hits + misses))
        st.markdown("---")
        st.markdown(
            f"<span style='color:#78716c;font-size:0.75rem;'>"
            f"Cache: {hits} hits · {misses} misses · {pct}% saved"
            f"</span>",
            unsafe_allow_html=True,
        )
    elif _redis_ping():
        st.markdown("---")
        st.markdown(
            "<span style='color:#78716c;font-size:0.75rem;'>"
            "Cache: connected ✓"
            "</span>",
            unsafe_allow_html=True,
        )


# =============================================================================
# MAIN AREA
# =============================================================================

st.markdown("# CiteFormat")
st.markdown(
    "<p style='color:#78716c;font-family:DM Sans,sans-serif;margin-top:-0.5rem;margin-bottom:1.5rem;'>"
    "Paste references below — DOIs, titles, author names, or any combination. "
    "One per line."
    "</p>",
    unsafe_allow_html=True,
)

INPUT_PLACEHOLDER = """\
# Paste your references here — one per line
# Lines starting with # are ignored

10.1038/nature12345
Attention is all you need | 2017
Hinton | Nature | 2006
BERT | NAACL | 2019
"""

col_input, col_info = st.columns([3, 1])

with col_input:
    raw_input = st.text_area(
        "References input",
        value=INPUT_PLACEHOLDER,
        height=220,
        label_visibility="collapsed",
        key="raw_input",
    )

with col_info:
    st.markdown("""
**Supported formats**

`10.1038/xxx` — DOI

`Title | Year`

`Author | Journal | Year`

`Title | Journal | Year`

`Author | Title | Journal | Year`

`Title only` *(least reliable)*
""")

btn_col, _ = st.columns([1, 4])
with btn_col:
    run_clicked = st.button("⚙ Format References", use_container_width=True)


# =============================================================================
# PROCESSING
# =============================================================================

def _normalise_key(line):
    """Canonical key for deduplication — strips DOI prefixes, lowercases, removes punctuation."""
    import re as _re2
    line = line.strip().lower()
    # Normalise DOI URLs to bare DOI
    line = _re2.sub(r'https?://(?:dx[.])?doi[.]org/', '', line)
    # Replace pipes and punctuation with spaces, then collapse whitespace
    line = _re2.sub(r'[|.,;:\-]', ' ', line)
    line = _re2.sub(r'[^\w\s]', '', line)
    line = _re2.sub(r'\s+', ' ', line).strip()
    return line

def _parse_lines(raw):
    seen_keys = set()
    result = []
    duplicates = []
    for l in raw.splitlines():
        l = l.strip()
        if not l or l.startswith("#"):
            continue
        key = _normalise_key(l)
        if key in seen_keys:
            duplicates.append(l)
        else:
            seen_keys.add(key)
            result.append(l)
    return result, duplicates


def _process_all_auto(lines, fmt_fn, fmt_label):
    """
    Process all lines that don't need user intervention.
    Returns (entries, ambiguous_items).
    entries items are (formatted_str, authors_meta) tuples, or None for ambiguous placeholders.
    Duplicate DOIs resolved at this stage are stored in st.session_state.post_duplicates.
    """
    entries        = []
    ambiguous      = []
    idx            = 1
    seen_dois      = {}   # doi -> original line that first claimed it

    progress = st.progress(0, text="Looking up references…")

    for i, line in enumerate(lines):
        info = detect_input_type(line)
        cached_hit = bool(_cache_get_doi(
            info["doi"].strip().lstrip("https://doi.org/").lstrip("http://dx.doi.org/")
        )) if info.get("type") == "doi" else False
        source_label = "cache" if cached_hit else "CrossRef"
        progress.progress((i) / len(lines), text=f"{i+1}/{len(lines)} · {source_label} · {line[:45]}…")

        if info["type"] == "doi":
            msg, raw_doi = cached_fetch_by_doi(info["doi"])
            if msg:
                doi_key = raw_doi.strip().lower()
                if doi_key in seen_dois:
                    entries.append((f"[Duplicate DOI: {line}]", []))
                else:
                    seen_dois[doi_key] = line
                    authors_meta = msg.get("author", [])
                    entries.append((fmt_fn(msg, raw_doi, idx), authors_meta))
                    idx += 1
            else:
                entries.append((f"[Could not retrieve: {line}]", []))
        else:
            params     = build_query(info)
            candidates = search_crossref(params, rows=3)

            if not candidates:
                entries.append((f"[No results found: {line}]", []))
            elif len(candidates) == 1:
                c       = candidates[0]
                raw_doi = c.get("DOI", "")
                dk      = raw_doi.strip().lower()
                if dk and dk in seen_dois:
                    entries.append((f"[Duplicate: {line}]", []))
                else:
                    full, rd = cached_fetch_by_doi(raw_doi)
                    msg     = _ensure_dict(full) or _ensure_dict(c)
                    rd      = rd  if rd   else raw_doi
                    if msg is None:
                        entries.append((f"[Could not retrieve: {line}]", []))
                        continue
                    if rd:
                        seen_dois[rd.strip().lower()] = line
                        _cache_set_doi(rd, msg)
                    authors_meta = msg.get("author", [])
                    entries.append((fmt_fn(msg, rd, idx), authors_meta))
                    idx += 1
            else:
                ambiguous.append({
                    "line":       line,
                    "entry_idx":  idx,
                    "list_pos":   len(entries),
                    "candidates": candidates,
                })
                entries.append(None)
                idx += 1

    progress.empty()
    # Store seen_dois in session so candidate resolution can check against it
    st.session_state.seen_dois = seen_dois
    return entries, ambiguous


def _candidate_card(c):
    title    = (c.get("title") or ["?"])[0][:90]
    authors  = c.get("author", [])
    auth_str = authors[0].get("family", "?") if authors else "?"
    if len(authors) > 1: auth_str += " et al."
    journal  = (c.get("container-title") or ["?"])[0][:50]
    year     = ""
    for field in ("published", "published-print", "issued"):
        dp = c.get(field, {}).get("date-parts")
        if dp and dp[0]:
            year = str(dp[0][0]); break
    score = c.get("score", 0)
    doi   = c.get("DOI", "")
    return f"**{auth_str}** ({year}) — {title}  \n*{journal}* · score {score:.0f} · `{doi}`"


# =============================================================================
# RUN
# =============================================================================

if run_clicked:
    st.session_state.entries   = []
    st.session_state.done      = False
    lines, duplicates = _parse_lines(raw_input)
    if not lines:
        st.warning("No references found. Add at least one line.")
    else:
        if duplicates:
            with st.expander(f"⚠ {len(duplicates)} duplicate(s) removed before processing"):
                for d in duplicates:
                    st.markdown(f"- `{d}`")
        with st.spinner("Looking up references…"):
            entries, ambiguous = _process_all_auto(lines, formatter, chosen_fmt_label)
        st.session_state.entries   = entries
        st.session_state.ambiguous = ambiguous
        st.session_state.highlight_names = highlight_names
        st.session_state.formatter       = formatter
        st.session_state.fmt_label       = chosen_fmt_label
        st.session_state.done            = not bool(ambiguous)


# =============================================================================
# AMBIGUOUS CANDIDATE RESOLUTION
# =============================================================================

if st.session_state.get("ambiguous") and not st.session_state.done:
    ambiguous = st.session_state.ambiguous
    pending   = [a for a in ambiguous if st.session_state.entries[a["list_pos"]] is None]

    if pending:
        item      = pending[0]
        fmt_fn    = st.session_state.formatter
        entry_idx = item["entry_idx"]
        seen_dois = st.session_state.get("seen_dois", {})

        st.markdown("---")
        st.markdown("### 🔍 Ambiguous match — please pick one")
        st.markdown(
            f"<span style='color:#78716c;'>Query: <code>{item['line']}</code></span>",
            unsafe_allow_html=True,
        )
        st.markdown(" ")

        for n, c in enumerate(item["candidates"]):
            title    = (c.get("title") or ["?"])[0][:90]
            authors  = c.get("author", [])
            auth_str = authors[0].get("family", "?") if authors else "?"
            if len(authors) > 1: auth_str += " et al."
            journal  = (c.get("container-title") or ["?"])[0][:50]
            year     = ""
            for field in ("published", "published-print", "issued"):
                dp = c.get(field, {}).get("date-parts")
                if dp and dp[0]:
                    year = str(dp[0][0]); break
            score = c.get("score", 0)
            doi   = c.get("DOI", "")

            with st.container(border=True):
                col_card, col_btn = st.columns([5, 1])
                with col_card:
                    st.markdown(f"**{auth_str}** ({year})")
                    st.markdown(title)
                    st.caption(f"{journal} · score {score:.0f} · {doi}")
                with col_btn:
                    st.write("")
                    if st.button("Select", key=f"pick_{item['list_pos']}_{n}"):
                        raw_doi = c.get("DOI", "")
                        doi_key = raw_doi.strip().lower()
                        if doi_key and doi_key in seen_dois:
                            st.session_state.entries[item["list_pos"]] = (
                                f"[Duplicate: {item['line']}]", []
                            )
                        else:
                            full, rd = cached_fetch_by_doi(raw_doi)
                            msg      = _ensure_dict(full) or _ensure_dict(c)
                            rd       = rd if rd else raw_doi
                            if msg is None:
                                st.error(f"Could not retrieve metadata for {raw_doi}. Please try again.")
                                st.stop()
                            authors_meta = msg.get("author", [])
                            if rd:
                                seen_dois[rd.strip().lower()] = item["line"]
                                st.session_state.seen_dois    = seen_dois
                                _cache_set_doi(rd, msg)
                            st.session_state.entries[item["list_pos"]] = (
                                fmt_fn(msg, rd, entry_idx), authors_meta
                            )
                        st.rerun()

        skip_col, _ = st.columns([1, 4])
        with skip_col:
            if st.button("⏭ Skip this entry", key=f"skip_{item['list_pos']}"):
                st.session_state.entries[item["list_pos"]] = (
                    f"[Skipped: {item['line']}]", []
                )
                st.rerun()

        remaining = sum(
            1 for a in ambiguous
            if st.session_state.entries[a["list_pos"]] is None
        )
        st.info(f"{remaining} ambiguous entr{'y' if remaining == 1 else 'ies'} remaining.")

    else:
        # All resolved
        st.session_state.done = True
        st.rerun()


# =============================================================================
# RESULTS
# =============================================================================

# Only render results when every entry is a resolved (str, list) tuple
_all_resolved = (
    st.session_state.done
    and st.session_state.entries
    and all(isinstance(e, tuple) for e in st.session_state.entries if e is not None)
)

if _all_resolved:
    # Filter out None placeholders and unpack only valid (str, list) tuples
    all_raw  = [e for e in st.session_state.entries if e is not None and isinstance(e, tuple)]
    hl_names  = st.session_state.get("highlight_names", [])
    fmt_label = st.session_state.get("fmt_label", chosen_fmt_label)

    ERROR_PREFIXES = ("[Skipped", "[Could not", "[No results", "[Duplicate")
    real_entries = [(e, m) for e, m in all_raw if e and not e.startswith(ERROR_PREFIXES)]
    skipped      = [(e, m) for e, m in all_raw if not e or e.startswith(ERROR_PREFIXES)]

    all_meta = [m for _, m in real_entries]
    targets  = _build_highlight_targets(all_meta, hl_names)

    st.markdown("---")
    st.markdown(f"### Results <span class='badge'>{fmt_label}</span>", unsafe_allow_html=True)
    st.markdown(" ")

    # ── Preview ───────────────────────────────────────────────────────────────
    for e, _ in real_entries:
        if not isinstance(e, str) or not e.strip():
            continue
        rendered = _html_markup(e)
        rendered = _apply_author_highlight_html(rendered, targets)
        st.markdown(f"<div class='ref-card'>{rendered}</div>", unsafe_allow_html=True)

    if skipped:
        dupes = [(e, m) for e, m in skipped if e.startswith("[Duplicate")]
        other = [(e, m) for e, m in skipped if not e.startswith("[Duplicate")]
        label_parts = []
        if dupes:  label_parts.append(f"{len(dupes)} duplicate(s)")
        if other:  label_parts.append(f"{len(other)} skipped / unresolved")
        with st.expander("⚠ " + " · ".join(label_parts)):
            if dupes:
                st.markdown("**Duplicates removed** (same paper already in list):")
                for e, _ in dupes:
                    # Strip the prefix for cleaner display
                    original = e.replace("[Duplicate DOI: ", "").replace("[Duplicate: ", "").rstrip("]")
                    st.markdown(f"- `{original}`")
            if other:
                st.markdown("**Skipped / unresolved:**")
                for e, _ in other:
                    st.markdown(f"- `{e}`")

    # ── Download buttons ──────────────────────────────────────────────────────
    st.markdown(" ")
    st.markdown("**Download**")
    dl_cols = st.columns(4)

    # Markdown
    with dl_cols[0]:
        md_buf = io.StringIO()
        now    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        md_buf.write(f"# References\n\n*{fmt_label} — {now}*\n\n")
        for e, _ in real_entries:
            hi = _apply_author_highlight_md(e, targets)
            md_buf.write(_md_passthrough(hi) + "\n\n")
        st.download_button(
            "⬇ Markdown",
            md_buf.getvalue().encode("utf-8"),
            file_name="references.md",
            mime="text/markdown",
            use_container_width=True,
        )

    # HTML
    with dl_cols[1]:
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as tf:
            html_path = tf.name
        render_html(real_entries, fmt_label, html_path, hl_names)
        with open(html_path, "rb") as f:
            html_bytes = f.read()
        os.unlink(html_path)
        st.download_button(
            "⬇ HTML",
            html_bytes,
            file_name="references.html",
            mime="text/html",
            use_container_width=True,
        )

    # PDF
    with dl_cols[2]:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            pdf_path = tf.name
        render_pdf(real_entries, fmt_label, pdf_path, hl_names)
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        os.unlink(pdf_path)
        st.download_button(
            "⬇ PDF",
            pdf_bytes,
            file_name="references.pdf",
            mime="application/pdf",
            use_container_width=True,
        )

    # BibTeX — fetch stored bib: entries from Redis by resolved DOIs
    with dl_cols[3]:
        seen_dois = st.session_state.get("seen_dois", {})
        bib_parts = []
        if seen_dois:
            # Fetch all bib: entries in one pipeline call
            doi_list = list(seen_dois.keys())
            cmds     = [["GET", _doi_cache_key(d)] for d in doi_list]
            results  = _upstash_post(cmds) or []
            for doi, res in zip(doi_list, results):
                raw = res.get("result", "") if isinstance(res, dict) else ""
                if raw and raw.strip().startswith("@"):
                    bib_parts.append(raw.strip())
                else:
                    # Not in cache — generate minimal entry from DOI
                    cite_key = re.sub(r"[^a-z0-9]", "", doi.split("/")[-1].lower())
                    bib_parts.append("@article{" + cite_key + ",\n  doi = {" + doi + "}\n}")
        bib_content = "% Generated by CiteFormat\n\n" + "\n\n".join(bib_parts)
        st.download_button(
            "⬇ BibTeX",
            bib_content.encode("utf-8"),
            file_name="references.bib",
            mime="text/plain",
            use_container_width=True,
        )
