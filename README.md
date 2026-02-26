# CiteFormat

Convert a list of paper references into formatted citations.

Paste DOIs, titles, author names, or any combination — CiteFormat looks them up via CrossRef and outputs properly formatted references in your chosen style.

**[→ Open the web app](https://citeformat.streamlit.app/)**

---

## Features

- **10 citation styles** — APA, MLA, Chicago, Vancouver, Harvard, IEEE, AMA, ACS, Nature, and plain summary
- **Smart input detection** — paste DOIs, titles, author+journal combinations, or free text
- **4 output formats** — preview in browser, or download as Markdown, HTML, or PDF
- **Author highlighting** — bold one or more author names across all citations
- **Duplicate detection** — automatically removes repeated entries even if entered in different formats

---

## Input formats

One reference per line. Lines starting with `#` are ignored.

```
# DOI — most reliable
10.1038/nature12345
https://doi.org/10.1126/science.abc1234

# Title | Year
Attention is all you need | 2017

# Author | Journal | Year
Hinton | Nature | 2006

# Title | Journal | Year
BERT pre-training | NAACL | 2019

# Author | Title | Journal | Year
Vaswani | Attention is all you need | NeurIPS | 2017
```

---

## Run locally

Requires Python 3.9+ and an internet connection.

```bash
# Install dependencies
pip install streamlit reportlab

# Launch the web app
streamlit run citeformat_app.py

# Or use the command-line version
python3 citeformat.py refs.txt
```

---

## Files

| File | Purpose |
|------|---------|
| `citeformat.py` | Core logic + CLI application |
| `citeformat_app.py` | Streamlit web interface |
| `requirements.txt` | Python dependencies |

---

## Requirements

- Python 3.9+
- Internet connection (uses the [CrossRef public API](https://api.crossref.org))
- No API key needed
