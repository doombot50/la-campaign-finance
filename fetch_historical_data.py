"""
fetch_historical_data.py
────────────────────────
Downloads historical LA Ethics campaign-finance CSVs for cycles not yet
cached locally.  Reuses the server's download_and_cache() function so the
output files are byte-for-byte identical to what the live server produces.

Usage:
    py -3 fetch_historical_data.py

What it fetches (if not already in .la_cache):
    contributions, expenditures, loans  for  2000-2003  and  2004-2007
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

# Import server module WITHOUT starting the HTTP server
# (the server only starts under __main__ guard)
import la_ethics_server as srv

# Make sure the server module knows where to find cache/lookup files
os.chdir(os.path.dirname(__file__))

# Load the politician lookup so _lookup_party() works correctly
print("Loading politician lookup…")
srv._load_politician_lookup()

# Cycles to download (oldest first)
CYCLES = ['2000-2003', '2004-2007']
TYPES  = ['contributions', 'expenditures', 'loans']

for cycle in CYCLES:
    for rtype in TYPES:
        print(f"\n-- {cycle} / {rtype} --")
        key = f'{rtype}_{cycle}'
        if srv.is_cached_fresh(cycle, rtype):
            print(f"  Already cached and fresh — skipping.")
            continue
        try:
            srv.download_and_cache(cycle, rtype)
            print(f"  Done.")
        except Exception as e:
            print(f"  ERROR: {e}")

print("\nAll done. Run 'py -3 build_candidate_index.py' to rebuild the index.")
