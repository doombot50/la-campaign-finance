#!/usr/bin/env python3
"""One-off: re-run party tagging over the cached .la_cache/*.json.gz records using
the freshly-merged lookup, so third-party candidates show up without a full
re-download. Rewrites each cache in place (mtime becomes 'now', which is newer
than the lookup file, so _bust_stale_caches leaves them and the 24h TTL resets).
"""
import gzip, json, os, glob
from collections import Counter
import la_ethics_server as srv

srv._load_politician_lookup()
CACHE = srv.CACHE_DIR

paths = (glob.glob(os.path.join(CACHE, 'contributions_yr*.json.gz')) +
         glob.glob(os.path.join(CACHE, 'expenditures_yr*.json.gz')) +
         glob.glob(os.path.join(CACHE, 'loans_yr*.json.gz')))

grand = Counter()
for path in sorted(paths):
    recs, changed = [], 0
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            old = r.get('party')
            new = srv.lookup_party(r.get('candidate', ''))
            if new != old:
                changed += 1
            r['party'] = new
            grand[new] += 1
            recs.append(r)
    tmp = path + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8', compresslevel=1) as f:
        for r in recs:
            f.write(json.dumps(r, separators=(',', ':')) + '\n')
    os.replace(tmp, path)
    print(f'  {os.path.basename(path):28} {len(recs):>8} recs, {changed:>6} re-tagged')

print('\nParty distribution across all re-tagged records:')
for k, v in grand.most_common():
    print(f'  {k:5} {v:,}')
