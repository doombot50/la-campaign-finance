#!/usr/bin/env python3
"""Shared high-confidence matcher: Ethics filer name -> SoS election result.

Used by both la_ethics_server.py (attach results at parse time) and
measure_winloss.py (dry-run coverage measurement) so the matching logic has a
single source of truth.

High-confidence tiers:
  T1  exact normalized full-name match
  T2  unambiguous first+last match (exactly one SoS candidate has that
      first/last, so middle names / nicknames are recovered without guessing)
Hard exclusions (return None -> no badge):
  - filer looks like a committee / PAC / party org / fund
  - name flagged ambiguous in the scrape (same name, conflicting parties)
  - first+last collides with another distinct SoS candidate
  - matched party conflicts with the filer's independently-assigned party
"""
import json, os, re

_RESULTS = None
_FIRSTLAST = None

_COMMITTEE_RE = re.compile(
    r'\b(PAC|FUND|COMMITTEE|PARTY|ASSOCIATION|ASSN|FRIENDS|CITIZENS|COALITION|'
    r'ALLIANCE|INC|LLC|CORP|POLITICAL|ACTION|LEAGUE|FEDERATION|CAUCUS|FOUNDATION|'
    r'SOCIETY|COUNCIL|BOARD|GROUP|ORGANIZATION|NORPAC|HOSPPAC|HOSTPAC|COMPANY|'
    r'ENERGY|REALTORS|HOSPITAL|RESTAURANT|CHIROPRACTIC|ELECTRIC|STEP UP|TO ELECT)\b',
    re.I)

def _normalize(name):
    name = name.upper()
    name = re.sub(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b', '', name)
    name = re.sub(r'[^A-Z\s]', ' ', name)
    return ' '.join(name.split())

def looks_like_committee(name):
    return bool(_COMMITTEE_RE.search(name)) or bool(re.search(r'\d', name)) or '&' in name

def load(path=None):
    global _RESULTS, _FIRSTLAST
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'la_election_results.json')
    if not os.path.exists(path):
        _RESULTS, _FIRSTLAST = {}, {}
        return _RESULTS
    _RESULTS = json.load(open(path, encoding='utf-8'))
    _FIRSTLAST = {}
    for norm in _RESULTS:
        toks = norm.split()
        if len(toks) >= 2:
            key = (toks[0], toks[-1])
            _FIRSTLAST[key] = None if key in _FIRSTLAST else norm   # None = collision
    return _RESULTS

def match(filer_name, filer_party=None):
    """Return a high-confidence result dict (+ '_tier'), or None."""
    if _RESULTS is None:
        load()
    if not filer_name or looks_like_committee(filer_name):
        return None
    norm = _normalize(filer_name)
    toks = norm.split()
    if len(toks) < 2:
        return None

    entry, tier = None, None
    e = _RESULTS.get(norm)
    if e:
        entry, tier = e, 'T1'
    else:
        cand = _FIRSTLAST.get((toks[0], toks[-1]))
        if cand:                       # exactly one candidate with this first+last
            entry, tier = _RESULTS[cand], 'T2'

    if not entry or entry.get('ambiguous'):
        return None
    if (filer_party and filer_party in ('DEM', 'REP')
            and entry['party'] in ('DEM', 'REP') and filer_party != entry['party']):
        return None
    return {**entry, '_tier': tier}
