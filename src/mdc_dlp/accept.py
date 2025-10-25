import sys, re, json, urllib.parse, requests

BASE_URL = "https://datacollective.mozillafoundation.org"

# ---------- Parsers ----------
RSC_CHUNK_RE = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', re.DOTALL)
TERMS_BLOCK_RE = re.compile(r'"terms"\s*:\s*\{(.*?)\}\s*,\s*"hasAgreed"', re.DOTALL)
TERMS_ID_RE = re.compile(r'"id"\s*:\s*"([^"]+)"')
LOCALIZATION_IDS_RE = re.compile(r'"localizationId"\s*:\s*"([^"]+)"')
DATASET_ID_IN_TERMS_RE = re.compile(r'"dataset"\s*:\s*\{[^}]*"id"\s*:\s*"([^"]+)"', re.DOTALL)

# script tags and the specific dataset page chunk
SCRIPT_SRC_RE = re.compile(r'<script[^>]+src="(/_next/static/chunks/[^"]+\.js)"', re.I)
DATASET_PAGE_CHUNK_RE = re.compile(r'/_next/static/chunks/app/datasets/%5BdatasetId%5D/page-[^"]+\.js', re.I)

# Next action ids are 40-64 lowercase hex
ACTION_ID_RE = re.compile(r'\b[a-f0-9]{40,64}\b')

def _unescape(s: str) -> str:
    return s.encode("utf-8", "ignore").decode("unicode_escape", "ignore").replace("\\/", "/")

def extract_terms_info_from_html(html: str):
    """
    Returns (terms_id, localization_ids[], dataset_id_from_terms or None)
    """
    m = TERMS_BLOCK_RE.search(html)
    candidates = [m.group(1)] if m else []
    for cm in RSC_CHUNK_RE.finditer(html):
        chunk = _unescape(cm.group(1))
        tm = TERMS_BLOCK_RE.search(chunk)
        if tm:
            candidates.append(tm.group(1))
    for blob in candidates:
        tid = TERMS_ID_RE.search(blob)
        locs = LOCALIZATION_IDS_RE.findall(blob)
        did = DATASET_ID_IN_TERMS_RE.search(blob)
        if tid:
            return tid.group(1), list(dict.fromkeys(locs)), (did.group(1) if did else None)
    return None, [], None

def json_or_text(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return resp.text[:800]

def build_next_router_state_tree(dataset_id: str) -> str:
    tree = ["", {
        "children": ["datasets", {
            "children": [
                ["datasetId", dataset_id, "d"],
                {"children": ["__PAGE__", {}, None, None]},
                None, None
            ]
        }, None, None]
    }, None, None, True]
    raw = json.dumps(tree, separators=(",", ":"))
    return urllib.parse.quote(raw, safe="")

def _get(s: requests.Session, url: str):
    r = s.get(url, timeout=30)
    r.encoding = r.encoding or "utf-8"
    return r

def discover_action_ids_for_dataset(session: requests.Session, page_html: str) -> list[str]:
    """
    Targeted discovery: only scan the dataset page chunk and immediate chunk scripts
    referenced in the HTML. This avoids false positives.
    """
    # 1) get the dataset page chunk url
    page_chunk = None
    for m in DATASET_PAGE_CHUNK_RE.finditer(page_html):
        page_chunk = m.group(0)
        break
    # 2) collect script chunk urls seen on the page
    scripts = list(dict.fromkeys(SCRIPT_SRC_RE.findall(page_html)))
    prioritized = []
    if page_chunk:
        prioritized.append(page_chunk)
    # prefer page chunk, then other chunks
    for srel in scripts:
        if srel not in prioritized:
            prioritized.append(srel)

    candidates = []
    for rel in prioritized:
        url = f"{BASE_URL}{rel}"
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 200 and "javascript" in r.headers.get("Content-Type",""):
                # Heuristic: in the page chunk, action id usually appears near “next-action” usage.
                # Narrow window: scan line-by-line; keep ids that appear on same line as words like "action","next","server","$ACTION".
                lines = r.text.splitlines()
                for line in lines:
                    if ("action" in line) or ("next" in line) or ("server" in line):
                        for m in ACTION_ID_RE.finditer(line):
                            candidates.append(m.group(0))
        except Exception:
            pass

    # de-dupe but keep order (page chunk ids first)
    seen = set()
    ordered = []
    for x in candidates:
        if x not in seen and re.fullmatch(r"[a-f0-9]{40,64}", x):
            seen.add(x)
            ordered.append(x)
    return ordered

def parse_has_agreed_from_html(html: str) -> bool:
    m = TERMS_BLOCK_RE.search(html)
    if not m:
        # try streamed chunks too
        for cm in RSC_CHUNK_RE.finditer(html):
            chunk = _unescape(cm.group(1))
            if TERMS_BLOCK_RE.search(chunk):
                html = chunk
                break
        else:
            return False
    # we already matched a "... 'terms':{...}, 'hasAgreed' ..." region; just look for "hasAgreed":true
    return '"hasAgreed":true' in html

def accept_terms(session_token: str, dataset_id: str):
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Python/requests",
    })
    s.cookies.set(
        "__Secure-authjs.session-token",
        session_token,
        domain="datacollective.mozillafoundation.org",
        path="/",
        secure=True,
    )

    # (1) Load dataset page and extract terms + ids
    page_url = f"{BASE_URL}/datasets/{dataset_id}"
    pr = _get(s, page_url)
    html = pr.text

    terms_id, localization_ids, dataset_id_from_terms = extract_terms_info_from_html(html)
    if not terms_id:
        raise RuntimeError(
            "Could not find terms.id in the page payload.\n"
            f"Content-Type: {pr.headers.get('Content-Type')}\n"
            f"URL: {page_url}\n"
            f"Status: {pr.status_code}\n"
            f"Sample:\n{html[:1000]}"
        )
    print(f"Found terms.id via HTML stream: {terms_id}")
    if localization_ids:
        print(f"Found localization IDs: {localization_ids}")
    if dataset_id_from_terms and dataset_id_from_terms != dataset_id:
        print(f"Note: terms block references dataset {dataset_id_from_terms}")

    # (2) Discover *targeted* next-action ids
    action_ids = discover_action_ids_for_dataset(s, html)
    if not action_ids:
        print("Warning: no action ids found in page chunk; will fall back to all chunk ids (less precise).")
        # conservative fallback: scan all chunks, but you probably won’t need this branch
        action_ids = sorted(set(ACTION_ID_RE.findall(html)))

    # (3) Prepare headers for Server Action
    tree_header = build_next_router_state_tree(dataset_id)
    base_headers = {
        "Accept": "text/x-component",
        "Content-Type": "text/plain;charset=UTF-8",
        "Origin": BASE_URL,
        "Referer": page_url,
        "next-router-state-tree": tree_header,
    }
    action_url = f"{BASE_URL}/datasets/{dataset_id}"
    body = json.dumps([terms_id, dataset_id])

    # (4) Try each action id; after each POST, re-fetch page and verify hasAgreed == true
    last_status = None
    for aid in action_ids:
        if not re.fullmatch(r"[a-f0-9]{40,64}", aid):
            continue
        headers = dict(base_headers)
        headers["next-action"] = aid
        try:
            r = s.post(action_url, data=body, headers=headers, timeout=30)
            last_status = (aid, r.status_code, r.headers.get("Content-Type",""))
            if r.status_code == 200 and "text/x-component" in r.headers.get("Content-Type",""):
                # verify by reloading page
                verify = _get(s, page_url)
                if parse_has_agreed_from_html(verify.text):
                    print(f"Accepted via Server Action (next-action={aid[:12]}..., 200).")
                    # (5) Optional: confirm via download endpoint
                    dv = s.post(
                        f"{BASE_URL}/api/datasets/{dataset_id}/download",
                        json={"termsId": terms_id, "datasetId": dataset_id},
                        headers={"Accept": "application/json", "Origin": BASE_URL, "Referer": page_url},
                        timeout=30,
                    )
                    if dv.status_code == 200:
                        print("✅ Terms accepted and download session created.")
                        print(json.dumps(dv.json(), indent=2)[:800])
                    else:
                        print(f"✅ Terms accepted (verified on page), but download API returned {dv.status_code}: {json_or_text(dv)}")
                    return
                # else: false positive; try next candidate id
        except Exception as e:
            last_status = (aid, None, str(e))
            continue

    aid, code, extra = last_status if last_status else ("<none>", "<no status>", "")
    raise RuntimeError(f"Failed to find the correct Server Action (last tried {aid}, status={code}, info={extra}).")

if __name__ == "__main__":
    try:
        session_token = input("Paste your __Secure-authjs.session-token: ").strip()
        dataset_id = input("Enter dataset_id: ").strip()
        accept_terms(session_token, dataset_id)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
