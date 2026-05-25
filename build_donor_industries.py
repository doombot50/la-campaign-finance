"""
build_donor_industries.py
─────────────────────────
Classify every unique donor name in the LA ethics contributions cache into an
industry bucket using a two-pass approach:

  Pass 1  — fast regex heuristics (no API calls)
              • Obvious PACs / political committees → "PAC/Political"
              • Obvious person names                → "Individual"

  Pass 2  — Claude Batch API (claude-haiku-4-5, 50% batch discount)
              • Everything else batched 50 names per request

Output: la_donor_industries.json
  {
    "generated": "YYYY-MM-DD",
    "total_unique_donors": N,
    "industries": {"DONOR NAME": "Industry Bucket", ...}
  }

Industry buckets
────────────────
  Oil & Gas | Law | Real Estate | Healthcare | Construction | Agriculture
  Finance | Labor/Unions | Education | Technology | Retail/Hospitality
  Government | PAC/Political | Individual | Other

Run:
  py -3 build_donor_industries.py

Requires ANTHROPIC_API_KEY in environment (or .env file in same directory).
"""

import gzip, json, os, re, sys, time, pathlib
from collections import Counter
from datetime import date

# ──────────────────────────── config ────────────────────────────
BASE_DIR   = pathlib.Path(__file__).parent
CACHE_DIR  = BASE_DIR / ".la_cache"
OUTPUT     = BASE_DIR / "la_donor_industries.json"
CHECKPOINT = BASE_DIR / "la_donor_industries_checkpoint.json"

MODEL      = "claude-haiku-4-5"
BATCH_SIZE = 50          # donors per Claude request
MAX_TOKENS = 400         # output per request (JSON array of labels)

INDUSTRY_BUCKETS = [
    "Oil & Gas", "Law", "Real Estate", "Healthcare", "Construction",
    "Agriculture", "Finance", "Labor/Unions", "Education", "Technology",
    "Retail/Hospitality", "Government", "PAC/Political", "Individual", "Other"
]

# ──────────────────────── heuristics ────────────────────────────

# Patterns that mark a name as a political committee / PAC.
# Note: also catches names like "ENPAC" where PAC appears mid-word (hence no leading \b).
PAC_RE = re.compile(
    r"(?:^|\b)(PAC|P\.?A\.?C\.?|POLITICAL ACTION|COMMITTEE|CAMPAIGN FUND|VICTORY FUND"
    r"|LEADERSHIP PAC|SUPER PAC|POLITICAL FUND|POLITICAL COMMITTEE"
    r"|PARTY FUND|DEMOCRAT|REPUBLICAN|LIBERTARIAN|GREEN PARTY"
    r"|FOR (GOVERNOR|SENATE|HOUSE|CONGRESS|PRESIDENT|MAYOR|JUDGE|SHERIFF|DISTRICT))\b"
    r"|PAC\b",   # catches ENPAC, CRPPA, etc. — any name ending in PAC
    re.IGNORECASE,
)

# Patterns that strongly suggest a corporate / org entity (send to Claude).
CORP_RE = re.compile(
    r"\b(LLC|L\.L\.C|LLP|L\.L\.P|INC\.?|CORP\.?|CO\.|LTD\.?|COMPANY|CORPORATION"
    r"|ENTERPRISE|ENTERPRISES|ASSOCIATES|GROUP|SERVICES|SOLUTIONS|INDUSTRIES"
    r"|SYSTEMS|PARTNERS|PARTNERSHIP|MANAGEMENT|PROPERTIES|HOLDINGS|TRUST"
    r"|FOUNDATION|ASSOCIATION|SOCIETY|COUNCIL|BUREAU|AUTHORITY|COMMISSION"
    r"|LAW FIRM|ATTORNEYS|ATTORNEY|LEGAL|COUNSEL|P\.C\.|P\.A\."
    r"|HOSPITAL|CLINIC|HEALTH|MEDICAL|CARE|PHARMACY|DENTAL|NURSING"
    r"|CHURCH|TEMPLE|SYNAGOGUE|MOSQUE|PARISH|DIOCESE|MINISTRY|MINISTRIES"
    r"|UNION|LOCAL \d+|BROTHERHOOD|TEAMSTERS|WORKERS|FEDERATION|EMPLOYEES"
    r"|BANK|FINANCIAL|CAPITAL|INVESTMENT|SECURITIES|INSURANCE|MORTGAGE"
    r"|SCHOOL|UNIVERSITY|COLLEGE|ACADEMY|INSTITUTE|BOARD OF|DISTRICT"
    r"|CONSTRUCTION|BUILDERS|CONTRACTORS|REALTY|REALTORS|DEVELOPMENT|DEVELOPERS"
    r"|FARM|FARMERS|AGRICULTURE|RANCHERS|GROWERS|NURSERY"
    r"|STORE|RESTAURANT|HOTEL|CASINO|GAMING|DEALERSHIP|AUTO|CHEVROLET|FORD|TOYOTA"
    r"|FUND|ALLIANCE|COALITION|CAUCUS|NETWORK|LEADERSHIP|SLATE|VENTURES"
    r"|SHIPPING|TRANSPORT|LOGISTICS|PIPELINE|ENERGY|OIL|GAS|PETROLEUM"
    r"|PUBLISHING|MEDIA|BROADCASTING|COMMUNICATIONS)\b",
    re.IGNORECASE,
)

# Looks like a person name — conservative: only 2–3 words, or "LAST, FIRST [M.]" format.
# We intentionally cap at 3 words to avoid classifying org names like "BLUE CROSS BLUE SHIELD"
# as Individual. Four-word person names (LINTON F. BROUSSARD JR) are sent to Claude instead.
NAME_WORD = r"[A-Z][A-Z'\-\.]{0,20}"
SUFFIX    = r"(?:,?\s*(?:JR\.?|SR\.?|II|III|IV|ESQ\.?))?"
PERSON_RE = re.compile(
    r"^"
    r"(?:"
    # "LAST, FIRST [M.]" style — comma required, max 3 words total
    r"(?:" + NAME_WORD + r",\s+" + NAME_WORD + r"(?:\s+" + NAME_WORD + r")?" + SUFFIX + r")"
    r"|"
    # "FIRST [M.] LAST" style — exactly 2–3 space-separated words + optional suffix
    r"(?:" + NAME_WORD + r"(?:\s+" + NAME_WORD + r"){1,2}" + SUFFIX + r")"
    r")"
    r"$",
    re.IGNORECASE,
)


def quick_classify(name: str) -> str | None:
    """Return a bucket without calling Claude, or None if unsure."""
    n = name.strip().upper()

    # Empty / very short / just numbers → Other
    if not n or len(n) < 3:
        return "Other"

    # Obvious PAC / committee
    if PAC_RE.search(n):
        return "PAC/Political"

    # Has corporate / org markers → send to Claude
    if CORP_RE.search(n):
        return None

    # Passes person-name regex and no org markers → Individual
    if PERSON_RE.match(n):
        return "Individual"

    # Anything else → send to Claude
    return None


# ──────────────────────── data collection ───────────────────────

def collect_donors() -> Counter:
    """Read all contributions JSONL.gz files; count occurrences per donor."""
    print("Collecting donor names from cache files…")
    counts: Counter = Counter()
    files = sorted(CACHE_DIR.glob("contributions_yr*.json.gz"))
    if not files:
        sys.exit(f"ERROR: no contribution files found in {CACHE_DIR}")
    for fp in files:
        yr = fp.name.replace("contributions_yr", "").replace(".json.gz", "")
        n = 0
        with gzip.open(fp, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    donor = (rec.get("contributor") or "").strip()
                    if donor:
                        counts[donor] += 1
                        n += 1
                except json.JSONDecodeError:
                    continue
        print(f"  {yr}: {n:,} contributions")
    print(f"Total unique donors: {len(counts):,}")
    return counts


# ───────────────────── batch classification ──────────────────────

SYSTEM_PROMPT = f"""You are a campaign-finance expert classifying donor names into industry buckets.

Given a JSON array of donor names, return a JSON array of the same length where each element is the industry bucket string for the corresponding donor.

Industry buckets (use EXACTLY one of these strings):
{json.dumps(INDUSTRY_BUCKETS, indent=2)}

Rules:
- "Individual" = a human person (not a business, org, or PAC)
- "PAC/Political" = political action committees, campaigns, party organizations
- "Oil & Gas" = oil, gas, energy, pipeline, petroleum companies
- "Law" = law firms, attorneys, legal services
- "Real Estate" = realtors, developers, property management, construction firms primarily real estate
- "Healthcare" = hospitals, clinics, physicians, pharmacies, nursing homes, insurance
- "Construction" = general contractors, builders, engineering firms (non-real-estate)
- "Agriculture" = farms, ranchers, agribusiness, crop/livestock
- "Finance" = banks, investment, insurance (non-healthcare), accounting
- "Labor/Unions" = unions, labor organizations, worker associations
- "Education" = schools, universities, teacher associations
- "Technology" = software, IT, telecom, media companies
- "Retail/Hospitality" = stores, restaurants, hotels, casinos, auto dealers
- "Government" = government entities, elected officials acting in official capacity
- "Other" = everything that doesn't fit above

Return ONLY a JSON array of strings with no explanation."""


def make_request(custom_id: str, donor_chunk: list[str]) -> dict:
    """Build a single batch request dict."""
    user_content = json.dumps(donor_chunk, ensure_ascii=False)
    return {
        "custom_id": custom_id,
        "params": {
            "model": MODEL,
            "max_tokens": MAX_TOKENS,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_content}],
        },
    }


def submit_batch(client, requests: list[dict]):
    """Submit one Anthropic message batch; return batch object."""
    print(f"  Submitting batch with {len(requests):,} requests…")
    batch = client.messages.batches.create(requests=requests)
    print(f"  Batch ID: {batch.id}  status: {batch.processing_status}")
    return batch


def poll_batch(client, batch_id: str, interval: int = 60):
    """Poll until batch processing is complete; return final batch object."""
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        sc = batch.processing_status
        rc = batch.request_counts
        print(
            f"  [{batch_id}] {sc}  "
            f"processing={rc.processing}  succeeded={rc.succeeded}  "
            f"errored={rc.errored}  canceled={rc.canceled}  expired={rc.expired}"
        )
        if sc == "ended":
            return batch
        time.sleep(interval)


def collect_batch_results(client, batch_id: str, chunk_map: dict[str, list[str]]) -> dict[str, str]:
    """Stream batch results; return {donor_name: industry} dict."""
    results: dict[str, str] = {}
    errors = 0
    for item in client.messages.batches.results(batch_id):
        cid = item.custom_id
        donors = chunk_map.get(cid, [])
        if item.result.type == "succeeded":
            raw = item.result.message.content[0].text.strip()
            # Strip markdown code fences if present
            raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.DOTALL).strip()
            try:
                labels = json.loads(raw)
                if isinstance(labels, list) and len(labels) == len(donors):
                    for donor, label in zip(donors, labels):
                        if label in INDUSTRY_BUCKETS:
                            results[donor] = label
                        else:
                            results[donor] = "Other"
                else:
                    # Length mismatch — fall back
                    for donor in donors:
                        results[donor] = "Other"
                    errors += 1
            except json.JSONDecodeError:
                for donor in donors:
                    results[donor] = "Other"
                errors += 1
        else:
            # errored / canceled / expired
            for donor in donors:
                results[donor] = "Other"
            errors += 1
    if errors:
        print(f"  WARNING: {errors} requests had errors or parse failures → labeled 'Other'")
    return results


# ──────────────────────────── main ──────────────────────────────

def main():
    # ── 0. Check API key ──
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        # Try loading from .env in same dir
        env_path = BASE_DIR / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("ANTHROPIC_API_KEY="):
                    api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    os.environ["ANTHROPIC_API_KEY"] = api_key
                    break
    if not api_key:
        sys.exit(
            "ERROR: ANTHROPIC_API_KEY not found.\n"
            "Set it as an environment variable or put it in a .env file:\n"
            "  ANTHROPIC_API_KEY=sk-ant-..."
        )

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    # ── 1. Load checkpoint if run was interrupted ──
    existing: dict[str, str] = {}
    if CHECKPOINT.exists():
        print(f"Loading checkpoint from {CHECKPOINT.name}…")
        cp = json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        existing = cp.get("industries", {})
        print(f"  {len(existing):,} already classified")

    # ── 2. Collect unique donors ──
    donor_counts = collect_donors()
    all_donors = list(donor_counts.keys())
    total = len(all_donors)

    # ── 3. Pass 1: heuristic classification ──
    print("\nPass 1: heuristic classification…")
    classified: dict[str, str] = dict(existing)  # seed from checkpoint
    needs_claude: list[str] = []

    heuristic_stats: Counter = Counter()
    for donor in all_donors:
        if donor in classified:
            continue
        bucket = quick_classify(donor)
        if bucket is not None:
            classified[donor] = bucket
            heuristic_stats[bucket] += 1
        else:
            needs_claude.append(donor)

    print(f"  Heuristic results: {sum(heuristic_stats.values()):,} classified")
    for bucket, n in heuristic_stats.most_common():
        print(f"    {bucket:25s}: {n:>8,}")
    print(f"  Needs Claude:      {len(needs_claude):,}")

    # ── 4. Pass 2: Claude Batch API ──
    if needs_claude:
        print(f"\nPass 2: Claude Batch API ({MODEL}) — {len(needs_claude):,} donors…")

        # Build chunks
        chunks = [needs_claude[i:i+BATCH_SIZE] for i in range(0, len(needs_claude), BATCH_SIZE)]
        print(f"  {len(chunks):,} requests × {BATCH_SIZE} donors each")

        # Estimate cost (approximate)
        est_input_tokens  = len(chunks) * (len(SYSTEM_PROMPT.split()) * 1.3 + BATCH_SIZE * 8)
        est_output_tokens = len(chunks) * BATCH_SIZE * 4
        est_cost = (est_input_tokens / 1e6 * 0.5) + (est_output_tokens / 1e6 * 2.5)
        print(f"  Est. cost (batch discount): ~${est_cost:.2f}")
        print(f"  Proceeding in 5 seconds… (Ctrl-C to abort)")
        time.sleep(5)

        # Build batch request list + chunk map
        chunk_map: dict[str, list[str]] = {}
        batch_requests = []
        for i, chunk in enumerate(chunks):
            cid = f"chunk-{i:06d}"
            chunk_map[cid] = chunk
            batch_requests.append(make_request(cid, chunk))

        # Submit
        batch_obj = submit_batch(client, batch_requests)
        batch_id  = batch_obj.id

        # Save batch_id to checkpoint so we can resume if interrupted
        cp_data = {
            "batch_id": batch_id,
            "industries": classified,
        }
        CHECKPOINT.write_text(json.dumps(cp_data, ensure_ascii=False), encoding="utf-8")
        print(f"  Checkpoint saved (batch_id={batch_id})")

        # Poll
        print("  Polling for completion (updates every 60s)…")
        poll_batch(client, batch_id, interval=60)

        # Collect results
        print("  Collecting results…")
        claude_results = collect_batch_results(client, batch_id, chunk_map)
        classified.update(claude_results)
        print(f"  Claude classified {len(claude_results):,} donors")

    # ── 5. Catch anything still missing ──
    remaining = [d for d in all_donors if d not in classified]
    if remaining:
        print(f"  Fallback 'Other' for {len(remaining):,} uncovered donors")
        for d in remaining:
            classified[d] = "Other"

    # ── 6. Print summary ──
    print("\nFinal industry breakdown:")
    final_counts: Counter = Counter(classified[d] for d in all_donors if d in classified)
    for bucket in INDUSTRY_BUCKETS:
        n = final_counts.get(bucket, 0)
        pct = n / total * 100 if total else 0
        print(f"  {bucket:25s}: {n:>8,}  ({pct:5.1f}%)")

    # ── 7. Write output ──
    output = {
        "generated": str(date.today()),
        "model": MODEL,
        "total_unique_donors": total,
        "industries": {d: classified.get(d, "Other") for d in all_donors},
    }
    OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=None), encoding="utf-8")
    print(f"\nWrote {OUTPUT.name}  ({OUTPUT.stat().st_size / 1e6:.1f} MB)")

    # Clean up checkpoint
    if CHECKPOINT.exists():
        CHECKPOINT.unlink()
        print("Checkpoint removed.")


if __name__ == "__main__":
    main()
