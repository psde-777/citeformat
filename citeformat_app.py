"""
citeformat_app.py — Streamlit GUI for citeformat
Run with:  streamlit run citeformat_app.py
"""

import streamlit as st
import sys, io, os, tempfile, datetime, base64, json, hashlib, urllib.request, urllib.parse

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

def _redis_request(method, path, body=None):
    """Make a raw REST request to Upstash Redis. Returns parsed JSON or None."""
    try:
        secrets  = st.secrets.get("upstash", {})
        base_url = secrets.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
        token    = secrets.get("UPSTASH_REDIS_REST_TOKEN", "")
        if not base_url or not token:
            return None
        url  = f"{base_url}{path}"
        data = json.dumps(body).encode() if body else None
        req  = urllib.request.Request(
            url, data=data, method=method,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
            }
        )
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None   # cache failure is always silent

def _cache_get(doi):
    """Return cached CrossRef message dict for this DOI, or None if not cached."""
    key    = "cf:doi:" + doi.strip().lower()
    result = _redis_request("GET", f"/get/{urllib.parse.quote(key, safe='')}")
    if result and result.get("result"):
        try:
            return json.loads(result["result"])
        except Exception:
            return None
    return None

def _cache_set(doi, msg):
    """Store CrossRef message dict in Redis. Silent on failure."""
    key  = "cf:doi:" + doi.strip().lower()
    data = json.dumps(msg, separators=(",", ":"))
    _redis_request("POST", f"/set/{urllib.parse.quote(key, safe='')}", body=data)

def _search_cache_key(params):
    """Stable cache key for a search query dict."""
    stable = json.dumps(params, sort_keys=True)
    return "cf:search:" + hashlib.md5(stable.encode()).hexdigest()

def _search_cache_get(params):
    """Return cached search results list, or None."""
    key    = _search_cache_key(params)
    result = _redis_request("GET", f"/get/{urllib.parse.quote(key, safe='')}")
    if result and result.get("result"):
        try:
            return json.loads(result["result"])
        except Exception:
            return None
    return None

def _search_cache_set(params, items):
    """Cache search results. Silent on failure."""
    key  = _search_cache_key(params)
    data = json.dumps(items, separators=(",", ":"))
    # Search results cached for 7 days (TTL in seconds)
    _redis_request("POST", f"/set/{urllib.parse.quote(key, safe='')}", body=data)
    _redis_request("POST", f"/expire/{urllib.parse.quote(key, safe='')}", body=604800)

def _cache_stats():
    """Return (hits, misses) from this session, stored in st.session_state."""
    if "cache_hits"   not in st.session_state: st.session_state.cache_hits   = 0
    if "cache_misses" not in st.session_state: st.session_state.cache_misses = 0
    return st.session_state.cache_hits, st.session_state.cache_misses

# ── Cached wrappers ───────────────────────────────────────────────────────────

def cached_fetch_by_doi(doi):
    """fetch_by_doi with Redis cache. Returns (msg, raw_doi)."""
    raw = doi.strip().lstrip("https://doi.org/").lstrip("http://dx.doi.org/")
    cached = _cache_get(raw)
    if cached is not None:
        st.session_state.cache_hits = st.session_state.get("cache_hits", 0) + 1
        return cached, raw
    st.session_state.cache_misses = st.session_state.get("cache_misses", 0) + 1
    msg, rd = fetch_by_doi(doi)
    if msg:
        _cache_set(rd, msg)
    return msg, rd

def cached_search_crossref(params, rows=3):
    """search_crossref with Redis cache. Returns list of candidate dicts."""
    cached = _search_cache_get(params)
    if cached is not None:
        st.session_state.cache_hits = st.session_state.get("cache_hits", 0) + 1
        return cached
    st.session_state.cache_misses = st.session_state.get("cache_misses", 0) + 1
    results = search_crossref(params, rows=rows)
    if results:
        _search_cache_set(params, results)
    return results

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
    elif _redis_request("GET", "/ping") is not None:
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
        progress.progress((i) / len(lines), text=f"Processing {i+1}/{len(lines)}: {line[:50]}…")
        info = detect_input_type(line)

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
            candidates = cached_search_crossref(params, rows=3)

            if not candidates:
                entries.append((f"[No results found: {line}]", []))
            elif len(candidates) == 1:
                c       = candidates[0]
                raw_doi = c.get("DOI", "")
                doi_key = raw_doi.strip().lower()
                if doi_key and doi_key in seen_dois:
                    entries.append((f"[Duplicate: {line}]", []))
                else:
                    full, rd = cached_fetch_by_doi(raw_doi)
                    msg     = full if full else c
                    rd      = rd  if rd   else raw_doi
                    if rd:
                        seen_dois[rd.strip().lower()] = line
                    authors_meta = msg.get("author", [])
                    entries.append((fmt_fn(msg, rd, idx), authors_meta))
                    idx += 1
            else:
                # Need user to pick — store placeholder, resolve later
                # Pass seen_dois so candidate resolution can check too
                ambiguous.append({
                    "line":       line,
                    "entry_idx":  idx,
                    "list_pos":   len(entries),
                    "candidates": candidates,
                })
                entries.append(None)         # placeholder
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
        with st.spinner("Querying CrossRef…"):
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
        item = pending[0]
        st.markdown("---")
        st.markdown(f"### 🔍 Ambiguous match — please pick one")
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
                    st.markdown(f"{title}")
                    st.caption(f"{journal} · score {score:.0f} · {doi}")
                with col_btn:
                    st.write("")  # vertical alignment spacer
                    if st.button("Select", key=f"pick_{item['list_pos']}_{n}"):
                        raw_doi   = c.get("DOI", "")
                        doi_key   = raw_doi.strip().lower()
                        seen_dois = st.session_state.get("seen_dois", {})
                        fmt_fn    = st.session_state.formatter
                        entry_idx = item["entry_idx"]
                        if doi_key and doi_key in seen_dois:
                            st.session_state.entries[item["list_pos"]] = (
                                f"[Duplicate: {item['line']}]", []
                            )
                        else:
                            full, rd = cached_fetch_by_doi(raw_doi)
                            msg      = full if full else c
                            rd       = rd   if rd   else raw_doi
                            authors_meta = msg.get("author", [])
                            if rd:
                                seen_dois[rd.strip().lower()] = item["line"]
                                st.session_state.seen_dois = seen_dois
                            st.session_state.entries[item["list_pos"]] = (
                                fmt_fn(msg, rd, entry_idx),
                                authors_meta,
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
    dl_cols = st.columns(3)

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
