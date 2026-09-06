# Social cards

Share-ready 1600×900 data graphics generated from the artifacts this repo
already commits, plus the tweet copy that goes with them (`TWEETS.md`).

Nothing else in the repo depends on this directory — it exists so the cards can
be **regenerated** after a nightly data refresh instead of being re-cut by hand.
Every number in a card is read out of the JSON at build time; none are typed in.

```bash
python3 build_social_cards.py     # -> cards.html
node render_cards.mjs             # -> out/c1.png … out/c4.png  (3200×1800)
```

`cards.html` opens fine in a browser on its own if you just want to look.

## The four cards

| Card | Story | Reads |
|---|---|---|
| `c1` | The biggest spender wins 65% of races — and a 10× advantage only gets to 76% | `la_money_wins.json` → `overall`, `by_advantage` |
| `c2` | North / South / East / West PAC share ~half their donors, every pair | `factions.json` → the four compass nodes + their 6 edges |
| `c3` | The widest upset in the set: $2,458 beat $126,868 | `la_money_wins.json` → `upsets[0]` |
| `c4` | Ranking by money *raised* picks the wrong money candidate 15% of the time | `la_money_wins.json` → `compare` |

## Dependencies

`build_social_cards.py` is **stdlib only**, like everything else here.

`render_cards.mjs` needs `playwright` and a chromium — the one genuinely optional
piece. It is a dev convenience for producing PNGs, never imported by the server
or the dashboard, so the repo's zero-dependency guarantee is unaffected.

## factions.json

Card 2 reads `factions.json` from the **la-donor-factions** repo, auto-found as a
sibling checkout (same convention `build_factions.py` uses for `.la_cache`).
Override with `--factions <path>` or `$LA_FACTIONS`. If it can't be located the
script warns and emits the other three cards, so this still runs from a lone
checkout.

## Design notes

The cards use the dashboard's own type (self-hosted Libre Franklin + IBM Plex
Mono out of `../vendor/fonts/`) and its teal/orange, with two deliberate
adjustments for chart marks:

- **teal `#008a76`** rather than the site's `#14756b`, which sits under the
  OKLCH chroma floor (0.085 vs 0.10) and reads gray at mark size;
- **orange `#c26410`** rather than `#e8833a`, which lands at 2.7:1 against white
  and misses the 3:1 a chart mark needs.

Both clear colour-vision-deficiency separation against each other (ΔE 11.2
protan / 22.5 normal). The four-step teal ramp on card 1
(`#7cc2b6 → #43a696 → #008a76 → #005f4e`) is ordinal — the spending-advantage
buckets are genuinely ordered — and its light end clears 2:1 on white.

Card-1 headline wording ("lose one race in four") is derived from the top
bucket's actual loss rate, so it re-words itself rather than going stale if the
data moves.
