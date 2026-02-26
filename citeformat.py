import sys
import re
import urllib.request
import urllib.error
import urllib.parse
import json

# Explicit imports required for PyInstaller bundling —
# urllib depends on these at runtime but PyInstaller's
# static analyser misses them without these lines.
import email
import email.message
import email.parser
import email.feedparser
import email.header
import email.errors
import email.charset
import email.encoders
import email.utils
import email.contentmanager
import email.policy
import html
import html.parser
import xml
import xml.etree
import xml.etree.ElementTree

# =============================================================================
# INPUT AUTO-DETECTION
# =============================================================================
# Supported line formats (auto-detected, no tags needed):
#
#   DOI (bare or URL):
#     10.1038/nature12345
#     https://doi.org/10.1038/nature12345
#
#   Title | Year
#     Attention is all you need | 2017
#
#   Author | Journal | Year
#     Vaswani | NeurIPS | 2017
#
#   Title | Journal | Year
#     Attention is all you need | Nature | 2023
#
#   Author | Title | Journal | Year  (most specific fuzzy input)
#     Vaswani | Attention is all you need | NeurIPS | 2017
#
#   Title only (least reliable — will show top 3 candidates)
#     Attention is all you need
#
# Lines starting with # are treated as comments and skipped.
# =============================================================================

DOI_RE = re.compile(r'^(https?://(?:dx\.)?doi\.org/)?(10\.\d{4,9}/\S+)$', re.I)
YEAR_RE = re.compile(r'\b(19|20)\d{2}\b')

# Known journal / venue keywords to help distinguish title vs journal
JOURNAL_HINTS = {
    "nature", "science", "cell", "lancet", "nejm", "jama", "pnas",
    "plos", "ieee", "acm", "neurips", "nips", "icml", "iclr", "cvpr",
    "aaai", "emnlp", "acl", "naacl", "arxiv", "biorxiv", "medrxiv",
    "annals", "journal", "letters", "review", "proceedings", "transactions",
    "frontiers", "nature medicine", "nature methods", "nature communications",
}

def looks_like_journal(text):
    t = text.lower().strip()
    if any(hint in t for hint in JOURNAL_HINTS):
        return True
    # Short strings with no spaces are more likely journal abbreviations
    if len(t.split()) <= 3 and len(t) <= 40:
        return True
    return False

def looks_like_author(text):
    """Heuristic: short, no numbers, likely a surname or 'Lastname, F.' pattern."""
    t = text.strip()
    if re.search(r'\d', t):
        return False
    words = t.split()
    if len(words) <= 3 and all(len(w) >= 2 for w in words):
        return True
    return False

def detect_input_type(line):
    """
    Returns a dict: {
        'type': 'doi' | 'title_year' | 'author_journal_year' |
                'title_journal_year' | 'author_title_journal_year' | 'title_only',
        'doi': str | None,
        'title': str | None,
        'author': str | None,
        'journal': str | None,
        'year': str | None,
        'raw': str   (original line)
    }
    """
    result = {'raw': line, 'doi': None, 'title': None,
              'author': None, 'journal': None, 'year': None}

    line = line.strip()

    # ── DOI? ──────────────────────────────────────────────────────────────────
    m = DOI_RE.match(line)
    if m:
        result['type'] = 'doi'
        result['doi']  = m.group(2)
        return result

    # ── Split on pipe ─────────────────────────────────────────────────────────
    parts = [p.strip() for p in line.split('|')]

    # Extract year from any part
    year = None
    clean_parts = []
    for p in parts:
        ym = YEAR_RE.search(p)
        if ym and len(p.strip()) <= 6:   # part IS the year
            year = ym.group(0)
        else:
            ym2 = YEAR_RE.search(p)
            if ym2:
                year = ym2.group(0)
                p = YEAR_RE.sub('', p).strip(' ,')
            if p:
                clean_parts.append(p)

    result['year'] = year

    # ── No pipes → could be title only or just a DOI we missed ───────────────
    if len(clean_parts) == 1:
        result['type']  = 'title_only'
        result['title'] = clean_parts[0]
        return result

    # ── Two parts ─────────────────────────────────────────────────────────────
    if len(clean_parts) == 2:
        a, b = clean_parts
        if looks_like_author(a) and looks_like_journal(b):
            # e.g. "Hinton | Nature | 2006" but year already stripped → author+journal
            result['type']    = 'author_journal_year'
            result['author']  = a
            result['journal'] = b
        elif looks_like_journal(b):
            result['type']    = 'title_journal_year'
            result['title']   = a
            result['journal'] = b
        else:
            # Assume title + year (year already extracted) or title + journal
            result['type']  = 'title_year'
            result['title'] = a
            # b might be a journal without a year
            if not year:
                result['journal'] = b
        return result

    # ── Three parts ───────────────────────────────────────────────────────────
    if len(clean_parts) == 3:
        a, b, c = clean_parts
        if looks_like_author(a) and looks_like_journal(c):
            result['type']    = 'author_title_journal_year'
            result['author']  = a
            result['title']   = b
            result['journal'] = c
        elif looks_like_author(a) and looks_like_journal(b):
            result['type']    = 'author_journal_year'
            result['author']  = a
            result['journal'] = b
        else:
            result['type']    = 'title_journal_year'
            result['title']   = a
            result['journal'] = b
        return result

    # ── Four+ parts: best effort ──────────────────────────────────────────────
    result['type']    = 'author_title_journal_year'
    result['author']  = clean_parts[0]
    result['title']   = " ".join(clean_parts[1:-1])
    result['journal'] = clean_parts[-1]
    return result


# =============================================================================
# API CALLS — POLITE POOL CONFIGURATION
# =============================================================================
#
# CrossRef operates a "Polite Pool" for well-behaved clients:
#   - Identify yourself with a real contact email in the User-Agent header
#   - Keep requests to ~1 per second (RATE_DELAY below)
#   - Retry on transient failures with exponential back-off
#   - Respect 429 Too Many Requests and honour Retry-After headers
#
# Set your email below. CrossRef uses it only to contact you if your client
# causes problems — they will not spam you.
# See: https://api.crossref.org/swagger-ui/index.html
# =============================================================================

import time

CONTACT_EMAIL = "your@email.com"   # <-- replace with your real email
RATE_DELAY    = 1.0                # minimum seconds between API requests
MAX_RETRIES   = 3                  # retries on 5xx / timeout before giving up
BACKOFF_BASE  = 2.0                # exponential back-off multiplier

HEADERS = {
    "User-Agent": f"doi-formatter/1.0 (mailto:{CONTACT_EMAIL})",
}

_last_request_time = 0.0   # tracks timestamp of the last API call

def _polite_wait():
    """Enforce a minimum gap between consecutive requests."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    wait    = RATE_DELAY - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_request_time = time.time()

def _get(url):
    """
    HTTP GET with polite rate limiting, 429 handling, and exponential back-off
    retries on 5xx errors and network timeouts.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        _polite_wait()
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())

        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 60))
                print(f"  [429 Too Many Requests — waiting {retry_after}s]",
                      file=sys.stderr)
                time.sleep(retry_after)
            elif e.code >= 500:
                wait = BACKOFF_BASE ** attempt
                print(f"  [HTTP {e.code} on attempt {attempt}/{MAX_RETRIES}"
                      f" — retrying in {wait:.0f}s]", file=sys.stderr)
                time.sleep(wait)
            else:
                raise   # 4xx errors (except 429) are not retryable

        except (urllib.error.URLError, TimeoutError, OSError) as e:
            wait = BACKOFF_BASE ** attempt
            print(f"  [Network error on attempt {attempt}/{MAX_RETRIES}: {e}"
                  f" — retrying in {wait:.0f}s]", file=sys.stderr)
            time.sleep(wait)

    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {url}")


def fetch_by_doi(doi):
    """Exact lookup — returns (msg, raw_doi) or (None, doi)."""
    raw = doi.strip().lstrip('https://doi.org/').lstrip('http://dx.doi.org/')
    try:
        data = _get(f"https://api.crossref.org/works/{urllib.parse.quote(raw, safe='/')}")
        return data.get("message", {}), raw
    except urllib.error.HTTPError as e:
        print(f"  [HTTP {e.code} for DOI {doi}]", file=sys.stderr)
        return None, raw
    except Exception as e:
        print(f"  [Error: {e}]", file=sys.stderr)
        return None, raw

def search_crossref(query_params, rows=3):
    """
    Fuzzy search CrossRef. query_params is a dict of CrossRef query fields, e.g.:
      {'query.title': '...', 'query.author': '...', 'query.container-title': '...'}
    Returns list of up to `rows` message dicts.
    """
    params = {**query_params, 'rows': rows, 'select': ','.join([
        'DOI','title','author','container-title','published','volume','issue','page','score'
    ])}
    url = "https://api.crossref.org/works?" + urllib.parse.urlencode(params)
    try:
        data = _get(url)
        return data.get("message", {}).get("items", [])
    except Exception as e:
        print(f"  [Search error: {e}]", file=sys.stderr)
        return []

def build_query(info):
    """Build CrossRef query params from detected input info."""
    params = {}
    if info.get('title'):
        params['query.title'] = info['title']
    if info.get('author'):
        params['query.author'] = info['author']
    if info.get('journal'):
        params['query.container-title'] = info['journal']
    if info.get('year'):
        y = info['year']
        params['filter'] = f"from-pub-date:{y},until-pub-date:{y}"
    if not params:
        params['query.bibliographic'] = info['raw']
    return params

def resolve_input(info):
    """
    Given detected input info, return (msg, raw_doi).
    For DOI: exact lookup.
    For fuzzy: fetch top 3 candidates and let user pick.
    """
    if info['type'] == 'doi':
        return fetch_by_doi(info['doi'])

    params  = build_query(info)
    candidates = search_crossref(params, rows=3)

    if not candidates:
        print(f"  [No results found for: {info['raw']}]", file=sys.stderr)
        return None, ''

    # If only one result and score is high, auto-accept
    if len(candidates) == 1:
        c = candidates[0]
        raw_doi = c.get('DOI', '')
        return c, raw_doi

    # ── Show top 3 and ask user to pick ───────────────────────────────────────
    print(f"\n  Query : {info['raw']}")
    print(f"  Detected as: {info['type'].replace('_', ' ')}")
    print(f"  Top candidates:\n")
    for n, c in enumerate(candidates, 1):
        title   = (c.get('title') or ['?'])[0][:80]
        authors = c.get('author', [])
        auth_str = authors[0].get('family', '?') if authors else '?'
        if len(authors) > 1: auth_str += ' et al.'
        journal  = (c.get('container-title') or ['?'])[0][:40]
        year     = ''
        for field in ('published', 'published-print', 'issued'):
            dp = c.get(field, {}).get('date-parts')
            if dp and dp[0]:
                year = str(dp[0][0])
                break
        score = c.get('score', 0)
        doi   = c.get('DOI', 'unknown')
        print(f"    [{n}] {auth_str} ({year}). {title}")
        print(f"        {journal} | Score: {score:.1f} | doi:{doi}\n")

    print(f"    [s] Skip this entry")

    while True:
        choice = input(f"  Pick 1–{len(candidates)} or s to skip: ").strip().lower()
        if choice == 's':
            return None, ''
        if choice.isdigit() and 1 <= int(choice) <= len(candidates):
            chosen  = candidates[int(choice) - 1]
            raw_doi = chosen.get('DOI', '')
            # Fetch full metadata via DOI for complete record
            full, rd = fetch_by_doi(raw_doi)
            return (full if full else chosen), (rd if rd else raw_doi)
        print(f"  Please enter a number between 1 and {len(candidates)}, or 's'.")


# =============================================================================
# FIELD HELPERS
# =============================================================================

def get_authors(msg):   return msg.get("author", [])
def get_volume(msg):    return msg.get("volume", "")
def get_issue(msg):     return msg.get("issue", "")
def get_pages(msg):     return msg.get("page", "")
def get_publisher(msg): return msg.get("publisher", "")

def get_last_names(authors):
    return [a.get("family", a.get("name", "?")) for a in authors]

def get_full_names_last_first(authors):
    parts = []
    for a in authors:
        family   = a.get("family", a.get("name", "?"))
        given    = a.get("given", "")
        initials = "".join(f"{p[0]}." for p in given.split()) if given else ""
        parts.append(f"{family}, {initials}".strip(", "))
    return parts

def get_full_names_first_last(authors):
    parts = []
    for a in authors:
        family   = a.get("family", a.get("name", "?"))
        given    = a.get("given", "")
        initials = "".join(f"{p[0]}." for p in given.split()) if given else ""
        parts.append(f"{initials} {family}".strip())
    return parts

def get_year(msg):
    for field in ("published", "published-print", "published-online", "issued"):
        date = msg.get(field, {}).get("date-parts")
        if date and date[0] and date[0][0]:
            return str(date[0][0])
    return "n.d."

def get_journal(msg):
    container = msg.get("container-title")
    if container:
        return container[0]
    return msg.get("publisher", "")

def get_title(msg):
    titles = msg.get("title", [])
    return titles[0] if titles else "Untitled"


# =============================================================================
# CITATION FORMATTERS
# =============================================================================

def fmt_plain(msg, raw_doi, idx):
    last_names = get_last_names(get_authors(msg))
    if   len(last_names) == 0: authors = "Unknown"
    elif len(last_names) == 1: authors = last_names[0]
    elif len(last_names) == 2: authors = f"{last_names[0]} and {last_names[1]}"
    else:                      authors = f"{last_names[0]} et al."
    return f"{idx}. {authors}. {get_journal(msg)}. {get_year(msg)}. https://doi.org/{raw_doi}."

def fmt_apa(msg, raw_doi, idx):
    names = get_full_names_last_first(get_authors(msg))
    if   len(names) == 0:  authors = "Unknown"
    elif len(names) == 1:  authors = names[0]
    elif len(names) <= 20: authors = ", ".join(names[:-1]) + f", & {names[-1]}"
    else:                  authors = ", ".join(names[:19]) + f", ... {names[-1]}"
    vol, issue, pages = get_volume(msg), get_issue(msg), get_pages(msg)
    source = get_journal(msg)
    if vol:   source += f", {vol}"
    if issue: source += f"({issue})"
    if pages: source += f", {pages}"
    return (f"{idx}. {authors} ({get_year(msg)}). {get_title(msg)}. "
            f"{source}. https://doi.org/{raw_doi}")

def fmt_mla(msg, raw_doi, idx):
    authors = get_authors(msg)
    if not authors:
        author_str = "Unknown"
    else:
        first  = authors[0]
        family = first.get("family", "?")
        given  = first.get("given", "")
        author_str = f"{family}, {given}".strip(", ")
        if len(authors) == 2:
            s = authors[1]
            si = "".join(f"{p[0]}." for p in s.get("given","").split())
            author_str += f", and {si} {s.get('family','?')}".strip()
        elif len(authors) > 2:
            author_str += ", et al."
    vol, issue, pages = get_volume(msg), get_issue(msg), get_pages(msg)
    sp = [get_journal(msg)]
    if vol:   sp.append(f"vol. {vol}")
    if issue: sp.append(f"no. {issue}")
    sp.append(get_year(msg))
    if pages: sp.append(f"pp. {pages}")
    return (f'{idx}. {author_str}. "{get_title(msg)}." '
            f'{", ".join(sp)}, https://doi.org/{raw_doi}.')

def fmt_chicago(msg, raw_doi, idx):
    names = get_full_names_last_first(get_authors(msg))
    if   len(names) == 0: authors = "Unknown"
    elif len(names) == 1: authors = names[0]
    elif len(names) <= 3: authors = ", ".join(names[:-1]) + f", and {names[-1]}"
    else:                 authors = f"{names[0]}, et al."
    vol, issue, pages = get_volume(msg), get_issue(msg), get_pages(msg)
    source = get_journal(msg)
    if vol:   source += f" {vol}"
    if issue: source += f" ({issue})"
    if pages: source += f": {pages}"
    return (f'{idx}. {authors}. {get_year(msg)}. "{get_title(msg)}." '
            f'{source}. https://doi.org/{raw_doi}.')

def fmt_vancouver(msg, raw_doi, idx):
    names = get_full_names_last_first(get_authors(msg))
    if   len(names) == 0: authors = "Unknown"
    elif len(names) <= 6: authors = ", ".join(names)
    else:                 authors = ", ".join(names[:6]) + ", et al."
    vol, issue, pages = get_volume(msg), get_issue(msg), get_pages(msg)
    source = get_journal(msg) + "."
    if vol or issue or pages:
        source += f" {get_year(msg)}"
        if vol:   source += f";{vol}"
        if issue: source += f"({issue})"
        if pages: source += f":{pages}"
    else:
        source += f" {get_year(msg)}"
    return f"{idx}. {authors}. {get_title(msg)}. {source}. https://doi.org/{raw_doi}."

def fmt_harvard(msg, raw_doi, idx):
    names = get_full_names_last_first(get_authors(msg))
    if   len(names) == 0: authors = "Unknown"
    elif len(names) == 1: authors = names[0]
    elif len(names) <= 3: authors = ", ".join(names[:-1]) + f" and {names[-1]}"
    else:                 authors = f"{names[0]} et al."
    vol, issue, pages = get_volume(msg), get_issue(msg), get_pages(msg)
    source = f"*{get_journal(msg)}*"
    if vol:   source += f", {vol}"
    if issue: source += f"({issue})"
    if pages: source += f", pp. {pages}"
    return (f"{idx}. {authors} ({get_year(msg)}) '{get_title(msg)}', "
            f"{source}. doi: https://doi.org/{raw_doi}.")

def fmt_ieee(msg, raw_doi, idx):
    names = get_full_names_first_last(get_authors(msg))
    if   len(names) == 0: authors = "Unknown"
    elif len(names) <= 6: authors = ", ".join(names)
    else:                 authors = ", ".join(names[:6]) + " et al."
    vol, issue, pages = get_volume(msg), get_issue(msg), get_pages(msg)
    source = f"*{get_journal(msg)}*"
    if vol:   source += f", vol. {vol}"
    if issue: source += f", no. {issue}"
    if pages: source += f", pp. {pages}"
    return (f"[{idx}] {authors}, \"{get_title(msg)},\" "
            f"{source}, {get_year(msg)}, doi: https://doi.org/{raw_doi}.")

def fmt_ama(msg, raw_doi, idx):
    names = get_full_names_last_first(get_authors(msg))
    if   len(names) == 0: authors = "Unknown"
    elif len(names) <= 6: authors = ", ".join(names)
    else:                 authors = ", ".join(names[:6]) + ", et al."
    vol, issue, pages = get_volume(msg), get_issue(msg), get_pages(msg)
    source = get_journal(msg) + "."
    if vol:
        source += f" {get_year(msg)};{vol}"
        if issue: source += f"({issue})"
        if pages: source += f":{pages}"
    else:
        source += f" {get_year(msg)}"
    return (f"{idx}. {authors}. {get_title(msg)}. "
            f"{source}. doi:https://doi.org/{raw_doi}")

def fmt_acs(msg, raw_doi, idx):
    names = get_full_names_last_first(get_authors(msg))
    if   len(names) == 0: authors = "Unknown"
    elif len(names) == 1: authors = names[0]
    else:                 authors = "; ".join(names)
    vol, issue, pages = get_volume(msg), get_issue(msg), get_pages(msg)
    source = f"*{get_journal(msg)}*"
    if vol:
        source += f" **{get_year(msg)}**, *{vol}*"
        if issue: source += f" ({issue})"
        if pages: source += f", {pages}"
    else:
        source += f" {get_year(msg)}"
    return (f"{idx}. {authors}. {get_title(msg)}. "
            f"{source}. https://doi.org/{raw_doi}.")

def fmt_nature(msg, raw_doi, idx):
    names = get_full_names_first_last(get_authors(msg))
    if   len(names) == 0: authors = "Unknown"
    elif len(names) <= 5: authors = ", ".join(names)
    else:                 authors = ", ".join(names[:5]) + " et al."
    vol, pages = get_volume(msg), get_pages(msg)
    source = f"*{get_journal(msg)}*"
    if vol:   source += f" **{vol}**"
    if pages: source += f", {pages}"
    return (f"{idx}. {authors}. {get_title(msg)}. "
            f"{source} ({get_year(msg)}). https://doi.org/{raw_doi}")


# =============================================================================
# FORMAT REGISTRY & SELECTION
# =============================================================================
#
# Citation formatters return strings with lightweight markup:
#   *text*   = italic   (used for journal names)
#   **text** = bold     (used for volume numbers in ACS/Nature)
#
# The output renderers below interpret these markers for each output format.
# =============================================================================

FORMATS = {
    "1":  ("Plain summary",                       fmt_plain),
    "2":  ("APA 7th",                             fmt_apa),
    "3":  ("MLA 9th",                             fmt_mla),
    "4":  ("Chicago 17th (author-date)",          fmt_chicago),
    "5":  ("Vancouver / ICMJE",                   fmt_vancouver),
    "6":  ("Harvard",                             fmt_harvard),
    "7":  ("IEEE",                                fmt_ieee),
    "8":  ("AMA (American Medical Association)",  fmt_ama),
    "9":  ("ACS (American Chemical Society)",     fmt_acs),
    "10": ("Nature",                              fmt_nature),
}

def choose_citation_format():
    print("\n" + "─" * 60)
    print("  CITATION FORMAT")
    print("─" * 60)
    for key, (label, _) in FORMATS.items():
        print(f"  [{key:>2}] {label}")
    while True:
        choice = input("\n  Enter number (default 1): ").strip() or "1"
        if choice in FORMATS:
            label = FORMATS[choice][0]
            print(f"\n  Selected: {label}\n", file=sys.stderr)
            return FORMATS[choice][1], label
        print(f"  Invalid choice. Please enter a number between 1 and {len(FORMATS)}.")


# =============================================================================
# OUTPUT RENDERERS
# =============================================================================
#
# Each renderer takes:
#   entries     — list of formatted citation strings (with *italic* / **bold**)
#   cite_style  — name of the citation format (e.g. "APA 7th")
#   out_path    — file path for file-based outputs (None for console)
#
# Markup conventions used in citation strings:
#   *word*      → italic
#   **word**    → bold
# =============================================================================

import re as _re
import os
import glob
import datetime

def _strip_markup(text):
    """Remove all *italic* and **bold** markers → plain text."""
    text = _re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = _re.sub(r'\*(.*?)\*',   r'\1', text)
    return text


# =============================================================================
# SMART AUTHOR HIGHLIGHT
# =============================================================================

def _author_matches_query(author_dict, query):
    """Return True if query (lowercased) matches this author dict."""
    family = author_dict.get("family", "").lower().strip()
    given  = author_dict.get("given",  "").lower().strip()
    full   = f"{given} {family}".strip()
    initials = "".join(p[0] for p in given.split()) if given else ""
    q = query.strip().lower()
    name_parts = given.split()
    first_given = name_parts[0] if name_parts else ""
    return (
        q == family                                        # "doe"
        or q == given                                      # "jon andrew"
        or q == full                                       # "jon andrew doe"
        or q == f"{family}, {given}"                       # "doe, jon andrew"
        or q == f"{given} {family}".strip()                # "jon doe"
        or q == f"{first_given} {family}".strip()          # "jon doe" (first given only)
        or q == f"{family}, {first_given}".strip()         # "doe, jon"
        or q == f"{family} {initials}"                     # "doe ja"
        or q == f"{initials} {family}"                     # "ja doe"
        or q == f"{family}, {initials}"                    # "doe, ja"
        or family.startswith(q)                            # prefix: "do"
        or (len(q) >= 3 and q in family)                   # substring: "oe"
        or (len(q) >= 3 and q == first_given)              # first given name: "jon"
    )

def _rendered_name_variants(author_dict):
    """All textual forms this author might appear as in a citation string."""
    family = author_dict.get("family", "").strip()
    given  = author_dict.get("given",  "").strip()
    if not family:
        name = author_dict.get("name", "")
        return [name] if name else []

    given_parts    = given.split() if given else []
    initials_dot   = "".join(p[0] + "."  for p in given_parts)
    initials_dot_sp= " ".join(p[0] + "." for p in given_parts)
    first_init     = given_parts[0][0] + "." if given_parts else ""

    variants = []
    if given:
        variants = [
            f"{given} {family}",
            f"{family}, {given}",
            f"{family} {given}",
            f"{family}, {initials_dot}",
            f"{family}, {initials_dot_sp}",
            f"{family}, {first_init}",
            f"{family} {initials_dot}",
            f"{family} {first_init}",
            f"{initials_dot} {family}",
            f"{initials_dot_sp} {family}",
            f"{first_init} {family}",
        ]
        seen, deduped = set(), []
        for v in variants:
            if v and v.lower() not in seen:
                seen.add(v.lower())
                deduped.append(v)
        variants = deduped

    if family.lower() not in {v.lower() for v in variants}:
        variants.append(family)

    return sorted(variants, key=len, reverse=True)

def _build_highlight_targets(authors_meta_list, query_list):
    """
    Given all author metadata lists and user queries, return a flat list of
    rendered name strings to bold, longest-first.
    """
    targets = {}
    for authors in authors_meta_list:
        for author in authors:
            for q in query_list:
                if _author_matches_query(author, q):
                    for variant in _rendered_name_variants(author):
                        targets[variant.lower()] = variant
    return sorted(targets.values(), key=len, reverse=True)

def _apply_highlight(text, targets, open_tag, close_tag):
    """Bold each target string using open/close tags. Avoids double-wrapping."""
    for target in targets:
        escaped = _re.escape(target)
        pattern = _re.compile(
            r'(?<![A-Za-z.\'])(' + escaped + r')(?![A-Za-z])',
            _re.IGNORECASE
        )
        pos = 0
        result = []
        for m in pattern.finditer(text):
            before = text[pos:m.start()]
            # skip if already inside our tag
            if before.count(open_tag) > before.count(close_tag):
                result.append(text[pos:m.end()])
            else:
                result.append(before + open_tag + m.group(1) + close_tag)
            pos = m.end()
        result.append(text[pos:])
        text = "".join(result)
    return text

def _apply_author_highlight_md(text, targets):
    return _apply_highlight(text, targets, "**", "**")

def _apply_author_highlight_html(text, targets):
    return _apply_highlight(text, targets, "<strong>", "</strong>")

def _apply_author_highlight_pdf(text, targets):
    return _apply_highlight(text, targets, "<b>", "</b>")

def _md_passthrough(text):
    """Markdown already uses * for emphasis — pass through as-is."""
    return text

def _html_markup(text):
    """Convert *italic* / **bold** markers to HTML tags, and make URLs clickable."""
    import html as _html
    text = _html.escape(text)
    # bold before italic to avoid mis-parsing **
    text = _re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', text)
    text = _re.sub(r'\*(.*?)\*',   r'<em>\1</em>',           text)
    # Make URLs clickable — match URL up to but not including trailing punctuation
    text = _re.sub(
        r'(https?://[^\s<>"]+?)([.,:;!?)\]]*(?=\s|$|<))',
        r'<a href="\1">\1</a>\2',
        text
    )
    return text


# ── Console (plaintext) ───────────────────────────────────────────────────────

def render_console(entries, cite_style, out_path=None, highlight_authors=None):
    print("\n" + "─" * 60)
    print(f"  References  [{cite_style}]")
    print("─" * 60 + "\n")
    for e, _authors in entries:
        print(_strip_markup(e))
    print()


# ── Markdown ──────────────────────────────────────────────────────────────────

def render_markdown(entries, cite_style, out_path, highlight_authors=None):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# References",
        f"",
        f"*Citation style: {cite_style} — generated {now}*",
        f"",
    ]
    all_meta = [a for _, a in entries]
    targets  = _build_highlight_targets(all_meta, highlight_authors or [])
    for e, _authors in entries:
        e = _apply_author_highlight_md(e, targets)
        lines.append(_md_passthrough(e))
        lines.append("")
    content = "\n".join(lines)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"\n  Markdown saved → {out_path}", file=sys.stderr)


# ── HTML ──────────────────────────────────────────────────────────────────────

def render_html(entries, cite_style, out_path, highlight_authors=None):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    all_meta = [a for _, a in entries]
    targets  = _build_highlight_targets(all_meta, highlight_authors or [])
    def _render_entry(e):
        e = _html_markup(e)
        e = _apply_author_highlight_html(e, targets)
        return f"    <li>{e}</li>"
    items = "\n".join(_render_entry(e) for e, _ in entries)
    # entries already carry their own index number (e.g. "1. Smith...")
    # so we use a plain unstyled list to avoid browser auto-numbering
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>References — {cite_style}</title>
  <style>
    body {{
      font-family: Georgia, "Times New Roman", serif;
      font-size: 14px;
      line-height: 1.8;
      max-width: 860px;
      margin: 40px auto;
      padding: 0 24px;
      color: #1a1a1a;
      background: #fafafa;
    }}
    h1 {{
      font-size: 1.5em;
      border-bottom: 2px solid #333;
      padding-bottom: 6px;
      margin-bottom: 4px;
    }}
    .meta {{
      color: #666;
      font-size: 0.85em;
      margin-bottom: 28px;
      font-style: italic;
    }}
    li {{
      margin-bottom: 10px;
    }}
    a {{
      color: #1a5276;
    }}
    em  {{ font-style: italic; }}
    strong {{ font-weight: bold; }}
  </style>
</head>
<body>
  <h1>References</h1>
  <p class="meta">Citation style: {cite_style} &mdash; generated {now}</p>
  <ul style="list-style:none; padding-left:0;">
{items}
  </ul>
</body>
</html>"""
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  HTML saved → {out_path}", file=sys.stderr)


# ── PDF ───────────────────────────────────────────────────────────────────────

def render_pdf(entries, cite_style, out_path, highlight_authors=None):
    try:
        from reportlab.platypus import (SimpleDocTemplate, Paragraph,
                                         Spacer, HRFlowable)
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        import html as _html_mod
    except ImportError:
        print("  [reportlab not installed — run: pip install reportlab]",
              file=sys.stderr)
        return

    def _pdf_markup(text):
        """Convert *italic* / **bold** markers to ReportLab XML tags."""
        text = _html_mod.escape(text)
        text = _re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>',   text)
        text = _re.sub(r'\*(.*?)\*',   r'<i>\1</i>',   text)
        # Make URLs clickable — strip trailing punctuation (.,:;) from both href and label
        def _linkify(m):
            full = m.group(1)
            trail_match = _re.search(r'[.,;:!?)]+$', full)
            if trail_match:
                url   = full[:trail_match.start()]
                trail = full[trail_match.start():]
            else:
                url, trail = full, ''
            return f'<a href="{url}" color="#1a5276">{url}</a>{trail}'
        text = _re.sub(r'(https?://\S+)', _linkify, text)
        return text

    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        leftMargin=2.5*cm, rightMargin=2.5*cm,
        topMargin=2.5*cm,  bottomMargin=2.5*cm,
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "RefTitle",
        parent=styles["Heading1"],
        fontSize=16,
        spaceAfter=4,
        textColor=colors.HexColor("#1a1a1a"),
    )
    meta_style = ParagraphStyle(
        "Meta",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#666666"),
        fontName="Helvetica-Oblique",
        spaceAfter=16,
    )
    entry_style = ParagraphStyle(
        "Entry",
        parent=styles["Normal"],
        fontName="Times-Roman",
        fontSize=11,
        leading=16,
        spaceAfter=8,
        leftIndent=18,
        firstLineIndent=-18,   # hanging indent
    )

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    story = [
        Paragraph("References", title_style),
        Paragraph(f"Citation style: {cite_style} &mdash; generated {now}", meta_style),
        HRFlowable(width="100%", thickness=0.5,
                   color=colors.HexColor("#aaaaaa"), spaceAfter=14),
    ]

    def _pdf_safe(text):
        """Replace unicode chars outside ReportLab built-in font range with ASCII."""
        replacements = {
            "—": "--", "–": "-",  "‘": "'",  "’": "'",
            "“": '"',  "”": '"',  "…": "...","→": "->",
            "─": "-",  "━": "-",  "·": ".",  "×": "x",
            "α": "alpha", "β": "beta", "μ": "mu",
        }
        for uni, asc in replacements.items():
            text = text.replace(uni, asc)
        # Fallback: drop any remaining non-latin-1 characters
        return text.encode("latin-1", errors="ignore").decode("latin-1")

    all_meta = [a for _, a in entries]
    targets  = _build_highlight_targets(all_meta, highlight_authors or [])
    for e, _authors in entries:
        e_safe    = _pdf_safe(e)
        e_marked  = _pdf_markup(e_safe)
        e_highlighted = _apply_author_highlight_pdf(e_marked, targets)
        story.append(Paragraph(e_highlighted, entry_style))

    doc.build(story)
    print(f"\n  PDF saved → {out_path}", file=sys.stderr)


# =============================================================================
# OUTPUT FORMAT SELECTION
# =============================================================================

OUTPUT_FORMATS = {
    "1": ("Console (plain text)",  "console",   None),
    "2": ("Markdown (.md)",        "markdown",  ".md"),
    "3": ("HTML (.html)",          "html",      ".html"),
    "4": ("PDF (.pdf)",            "pdf",       ".pdf"),
}

RENDERERS = {
    "console":  render_console,
    "markdown": render_markdown,
    "html":     render_html,
    "pdf":      render_pdf,
}

def choose_output_format(input_file):
    print("\n" + "─" * 60)
    print("  OUTPUT FORMAT")
    print("─" * 60)
    for key, (label, _, _ext) in OUTPUT_FORMATS.items():
        print(f"  [{key}] {label}")

    while True:
        choice = input("\n  Enter number (default 1): ").strip() or "1"
        if choice not in OUTPUT_FORMATS:
            print(f"  Invalid choice. Please enter 1–{len(OUTPUT_FORMATS)}.")
            continue

        label, fmt_key, ext = OUTPUT_FORMATS[choice]

        if ext is None:
            print(f"\n  Selected: {label}\n", file=sys.stderr)
            return RENDERERS[fmt_key], None

        # Suggest an output filename based on the input filename
        base = os.path.splitext(os.path.basename(input_file))[0]
        default_out = f"{base}_references"
        raw = input(f"  Output filename (no extension needed) [{default_out}]: ").strip()
        name = raw if raw else default_out
        # Strip any extension the user may have typed, then add the correct one
        name = os.path.splitext(name)[0]
        out_path = name + ext
        print(f"\n  Selected: {label} → {out_path}\n", file=sys.stderr)
        return RENDERERS[fmt_key], out_path


# =============================================================================
# FILE SELECTION
# =============================================================================

def choose_file(cli_arg=None):
    """
    Resolve the input file via (in order of priority):
      1. Command-line argument passed directly
      2. Interactive prompt — lists .txt files in cwd as shortcuts,
         but also accepts any path the user types
    """
    if cli_arg:
        if not os.path.isfile(cli_arg):
            print(f"Error: file not found: {cli_arg}", file=sys.stderr)
            sys.exit(1)
        return cli_arg

    print("\n" + "─" * 60)
    print("  INPUT FILE SELECTION")
    print("─" * 60)

    txt_files = sorted(glob.glob("*.txt"))

    if txt_files:
        print("\n  .txt files found in current directory:\n")
        for i, f in enumerate(txt_files, 1):
            size  = os.path.getsize(f)
            nlines = sum(1 for _ in open(f) if _.strip() and not _.startswith("#"))
            print(f"    [{i}] {f}  ({nlines} entries, {size} bytes)")
        print()
        hint = f"Enter 1\u2013{len(txt_files)} to pick from the list above,\n  or type a full file path: "
    else:
        print("\n  No .txt files found in the current directory.")
        hint = "Enter the path to your input file: "

    while True:
        raw = input(f"  {hint}").strip()

        if raw.isdigit() and txt_files:
            idx = int(raw) - 1
            if 0 <= idx < len(txt_files):
                chosen = txt_files[idx]
                print(f"\n  Selected: {chosen}\n")
                return chosen
            print(f"  Please enter a number between 1 and {len(txt_files)}.")
            continue

        if raw:
            if os.path.isfile(raw):
                print(f"\n  Selected: {raw}\n")
                return raw
            print(f"  File not found: '{raw}'. Please try again.")
            continue

        default = "refs.txt"
        if os.path.isfile(default):
            print(f"\n  Using default: {default}\n")
            return default

        print("  No input given and 'refs.txt' not found. Please enter a file path.")


# =============================================================================
# MAIN
# =============================================================================

def print_tldr():
    print("""
╔══════════════════════════════════════════════════════════════╗
║  citeformat  --  DOI & Reference Formatter                   ║
╠══════════════════════════════════════════════════════════════╣
║  INPUT  one reference per line, auto-detected:               ║
║    10.1038/nature12345             DOI (most reliable)       ║
║    Attention is all you need | 2017                          ║
║    Hinton | Nature | 2006                                    ║
║    BERT | NAACL | 2019                                       ║
║    # lines starting with # are comments and are ignored      ║
╠══════════════════════════════════════════════════════════════╣
║  STEPS                                                       ║
║    1. Pick your input file                                   ║
║    2. Choose a citation style   (APA, MLA, IEEE, Nature ...) ║
║    3. Choose an output format   (console / MD / HTML / PDF)  ║
║    4. Optionally bold one or more author names               ║
╠══════════════════════════════════════════════════════════════╣
║  Ambiguous entries show the top 3 CrossRef matches to pick   ║
║  from. Uses CrossRef API -- internet connection required.    ║
╚══════════════════════════════════════════════════════════════╝
""")


def main():
    print_tldr()
    file_path = choose_file(sys.argv[1] if len(sys.argv) > 1 else None)

    with open(file_path, "r") as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    if not lines:
        print("No entries found in file. Exiting.", file=sys.stderr)
        sys.exit(0)

    formatter, cite_label = choose_citation_format()
    renderer,  out_path   = choose_output_format(file_path)

    print(f"Processing {len(lines)} entr{'y' if len(lines)==1 else 'ies'}...\n",
          file=sys.stderr)

    entries      = []   # list of (formatted_string, [author_dicts])
    idx = 1
    for line in lines:
        info = detect_input_type(line)
        print(f"\u2192 [{info['type']:30s}] {line[:60]}", file=sys.stderr)
        msg, raw_doi = resolve_input(info)
        if msg is None:
            print(f"  [Skipped] {line}", file=sys.stderr)
        else:
            authors_meta = msg.get("author", [])
            entries.append((formatter(msg, raw_doi, idx), authors_meta))
            idx += 1

    # Author highlight — only for rich output formats
    highlight_authors = []
    if out_path is not None:   # not console
        print("\n" + "─" * 60)
        print("  AUTHOR HIGHLIGHT  (bold in output)")
        print("─" * 60)
        print("  Enter author name(s) to highlight in bold, one per line.")
        print("  These are matched case-insensitively against author surnames.")
        print("  Press Enter on an empty line when done (or just Enter to skip).\n")
        while True:
            name = input("  Author name (or Enter to finish): ").strip()
            if not name:
                break
            highlight_authors.append(name.lower())
        if highlight_authors:
            print(f"\n  Will highlight: {', '.join(highlight_authors)}\n", file=sys.stderr)

    renderer(entries, cite_label, out_path, highlight_authors)

if __name__ == "__main__":
    main()
