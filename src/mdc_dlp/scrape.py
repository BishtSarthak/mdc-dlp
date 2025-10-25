#!/usr/bin/env python3
import csv, json, re, time, sys
from typing import Dict, Iterable, List, Optional, Tuple
import requests

try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except Exception:
    BeautifulSoup = None

BASE = "https://datacollective.mozillafoundation.org"
LIST_URL = f"{BASE}/datasets"
DETAIL_URL = f"{BASE}/datasets/{{id}}"

# Paste a current hash from your HAR if POST starts failing with 400/403.
NEXT_ACTION = "407ce5370400fda06eccef00a55d6a3c4e59ed8291"

# Paste cookies if needed to mirror your browser state (usually not needed for public browse).
COOKIE_HEADER: Optional[str] = None  # e.g., 'OptanonConsent=...; OptanonAlertBoxClosed=...'

OUT_CSV = "datasets_all.csv"
PAGE_LIMIT = 24
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_CALLS_SEC = 0.2

# ----------------- RSC helpers -----------------
def normalize_rsc_tokens(s: str) -> str:
    # "$n12345" -> 12345   (number)
    s = re.sub(r'"\$n(\d+)"', r'\1', s)
    s = re.sub(r'\$n(\d+)', r'\1', s)
    # "$D..." -> "..."     (ISO date)
    s = re.sub(r'"\$D([^"]+)"', r'"\1"', s)
    s = re.sub(r'\$D([^\s,\]\}"]+)', r'"\1"', s)
    # other $-refs -> null
    s = re.sub(r'"\$[@A-Za-z0-9_]+"', 'null', s)
    s = re.sub(r'\$[@A-Za-z0-9_]+', 'null', s)
    return s

def strip_chunk_prefixes(rsc_body: str) -> str:
    # Lines look like '0:{...}', '1:{...}'
    return re.sub(r'^\s*\d+\s*:\s*', '', rsc_body, flags=re.M)

def extract_json_array_from_rsc(body_text: str, key: str) -> List[dict]:
    """
    Find and parse `"key": [ ... ]` from an RSC stream.
    """
    joined = strip_chunk_prefixes(body_text)
    m = re.search(rf'"{re.escape(key)}"\s*:\s*\[', joined)
    if not m:
        return []
    start = m.end() - 1
    depth = 0
    i = start
    while i < len(joined):
        ch = joined[i]
        if ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0:
                arr_text = joined[start:i+1]
                arr_text = normalize_rsc_tokens(arr_text)
                try:
                    return json.loads(arr_text)
                except Exception:
                    return []
        elif ch == '"':  # skip strings
            i += 1
            while i < len(joined):
                if joined[i] == '\\':
                    i += 2
                elif joined[i] == '"':
                    break
                else:
                    i += 1
        i += 1
    return []

def extract_first_object_with_id(body_text: str, target_id: str) -> Optional[dict]:
    """
    For detail pages: locate an object containing `"id": "<target_id>"` and parse it by brace matching.
    This is a heuristic but works well on RSC detail responses.
    """
    joined = strip_chunk_prefixes(body_text)
    pos = joined.find(f'"id":"{target_id}"')
    if pos == -1:
        return None
    # Walk left to find the opening '{' that starts this object
    start = pos
    while start > 0 and joined[start] != '{':
        start -= 1
    # Brace-match to the end
    depth = 0
    i = start
    while i < len(joined):
        ch = joined[i]
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                obj_text = joined[start:i+1]
                obj_text = normalize_rsc_tokens(obj_text)
                try:
                    return json.loads(obj_text)
                except Exception:
                    return None
        elif ch == '"':
            i += 1
            while i < len(joined):
                if joined[i] == '\\':
                    i += 2
                elif joined[i] == '"':
                    break
                else:
                    i += 1
        i += 1
    return None

# ----------------- HTTP helpers -----------------
def base_headers() -> Dict[str, str]:
    h = {
        "User-Agent": "Mozilla/5.0",
        "Origin": BASE,
        "Referer": f"{BASE}/datasets",
    }
    if COOKIE_HEADER:
        h["Cookie"] = COOKIE_HEADER
    return h

def get_initial_via_post(session: requests.Session) -> List[dict]:
    """
    Try to use the same server action as 'Load more' but without lastId to fetch the first page.
    We try without lastId, then with lastId="".
    """
    headers = base_headers()
    headers.update({
        "Accept": "text/x-component",
        "Content-Type": "text/plain;charset=UTF-8",
        "next-action": NEXT_ACTION,
    })
    # 1) No lastId key
    payload1 = json.dumps([{"search": "", "limit": PAGE_LIMIT}], separators=(",", ":"))
    for payload in (payload1, json.dumps([{"search": "", "limit": PAGE_LIMIT, "lastId": ""}], separators=(",", ":"))):
        r = session.post(LIST_URL, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            continue
        rows = extract_json_array_from_rsc(r.text, "datasets")
        if rows:
            return rows
    return []

def get_initial_ids_from_html(session: requests.Session) -> List[str]:
    """
    Fallback: fetch /datasets HTML and scrape the first 24 dataset IDs from links: /datasets/<id>
    """
    if BeautifulSoup is None:
        return []
    headers = base_headers()
    headers.setdefault("Accept", "text/html,application/xhtml+xml")
    r = session.get(LIST_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    ids = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.match(r"^/datasets/([a-z0-9]+)$", href)
        if m:
            ids.append(m.group(1))
    # keep order, unique, first 24
    seen = set()
    ordered = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            ordered.append(i)
    return ordered[:PAGE_LIMIT]

def get_detail_for_id(session: requests.Session, ds_id: str) -> Optional[dict]:
    """
    Fetch dataset detail page RSC and extract a dataset-like object (heuristic).
    """
    headers = base_headers()
    headers.update({
        "Accept": "text/x-component",
        "rsc": "1",
        "next-url": f"/datasets/{ds_id}",
        # minimal router state; detail fetches are tolerant
    })
    url = DETAIL_URL.format(id=ds_id)
    r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        return None
    obj = extract_first_object_with_id(r.text, ds_id)
    return obj

def post_load_more(session: requests.Session, last_id: str) -> Tuple[List[dict], Optional[str]]:
    headers = base_headers()
    headers.update({
        "Accept": "text/x-component",
        "Content-Type": "text/plain;charset=UTF-8",
        "next-action": NEXT_ACTION,
    })
    payload = json.dumps([{"search": "", "limit": PAGE_LIMIT, "lastId": last_id}], separators=(",", ":"))
    r = session.post(LIST_URL, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    rows = extract_json_array_from_rsc(r.text, "datasets")
    next_last = rows[-1]["id"] if rows else None
    return rows, next_last

# ----------------- CSV helpers -----------------
def flatten(d: dict, parent: str = "", sep: str = ".") -> Dict[str, object]:
    out = {}
    for k, v in d.items():
        key = f"{parent}{sep}{k}" if parent else k
        if isinstance(v, dict):
            out.update(flatten(v, key, sep))
        else:
            out[key] = v
    return out

def write_csv(rows: Iterable[dict], path: str) -> None:
    flat = [flatten(r) for r in rows]
    keys_order: List[str] = []
    seen = set()
    for fr in flat:
        for k in fr:
            if k not in seen:
                seen.add(k); keys_order.append(k)
    preferred = ["id","slug","name","description","locale","sizeBytes","createdAt",
                 "metadata.task","metadata.format","metadata.license",
                 "organization.name","organization.slug"]
    ordered = [k for k in preferred if k in seen] + [k for k in keys_order if k not in preferred]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ordered)
        w.writeheader()
        for fr in flat:
            w.writerow(fr)

# ----------------- Main flow -----------------
def main():
    s = requests.Session()
    all_rows: List[dict] = []

    # A) Try to get first 24 via POST (no lastId / empty lastId)
    print("Trying initial page via POST server-action…")
    init_rows = get_initial_via_post(s)
    if init_rows:
        print(f"Initial via POST: {len(init_rows)}")
        all_rows.extend(init_rows)
        last_id = init_rows[-1]["id"]
    else:
        print("Initial via POST returned 0; falling back to HTML scrape for first 24 IDs…")
        ids = get_initial_ids_from_html(s)
        print(f"HTML first-page IDs: {len(ids)}")
        # Fetch detail for each ID to gather full fields
        for ds_id in ids:
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)
            obj = get_detail_for_id(s, ds_id) or {"id": ds_id}
            all_rows.append(obj)
        last_id = ids[-1] if ids else None

    # B) Page 2+ via POST (cursor = last_id)
    page = 2
    while last_id:
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
        rows, next_last = post_load_more(s, last_id)
        print(f"Page {page}: +{len(rows)}")
        if not rows or next_last == last_id:
            break
        all_rows.extend(rows)
        last_id = next_last
        page += 1

    print(f"Total datasets collected: {len(all_rows)}")
    write_csv(all_rows, OUT_CSV)
    print(f"Wrote CSV to {OUT_CSV}")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print("HTTP error:", e, file=sys.stderr)
        print("If this happens on the POST, refresh NEXT_ACTION from a fresh HAR (Load More click).", file=sys.stderr)
        sys.exit(1)
