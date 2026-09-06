# Tweet drafts

Four posts, each paired with a rendered card in `out/`. Every figure traces to a
committed artifact — the provenance line under each draft says which.

Pick one and post it standalone; they don't need to run as a thread. If you do
thread them, **2 → 1 → 3 → 4** reads best (hook, then the substance, then the
story, then the craft).

---

## 1 · The money-advantage curve → `out/c1.png`

> Money wins in Louisiana politics — just less than you'd think.
>
> Across 523 contested state races since 2000, the biggest spender won 65% of the time.
>
> Outspend your opponent more than 10-to-1 and you're still only at 76%. The curve flattens fast.

*246 characters.*

**Provenance:** `la_money_wins.json` → `overall` (523 races, 65.4%) and
`by_advantage` (under 2× → 58.3%, over 10× → 75.8%).

**If someone pushes back:** the honest caveat is selection. Candidates below the
Ethics Board's itemization threshold never appear in the finance data, so this
leans toward races where money was actually raised, spent and reported. Say so
rather than defending the number as universal.

---

## 2 · The compass-PAC clique → `out/c2.png`

> I built a graph of which Louisiana political committees draw on the same donors.
>
> Four PACs — North, South, East and West — sit in a perfect little clique. All six pairs share ~half their donors. About $1M runs through each overlap.

*232 characters.*

**Provenance:** `factions.json` → the four compass-named PAC nodes and the six edges among
them (Jaccard 0.47–0.51, 649–723 shared donors, ~$1.0M shared dollars per pair).

**Why it holds up:** Jaccard controls for size, so this isn't "big committee looks
connected to everything." Donor identity is the resolved-entity table (nicknames
folded within last name + ZIP, org spellings merged), and committee-to-committee
transfers are excluded — these are shared *donors*, not money the committees pass
between themselves.

**Worth adding as a reply:** every edge on the site opens its receipts — the top
shared donors ranked by dollars to both sides, each linking back to the finance
portal. That's the part that makes it checkable rather than asserted.

---

## 3 · The $2,458 judge → `out/c3.png`

> Favorite thing in the Louisiana campaign finance data so far:
>
> 2012, 4th Circuit Court of Appeal. One candidate spent $126,868 and took 42.3% of the vote.
>
> The other spent $2,458 and won.
>
> One of 181 races since 2000 where the biggest spender lost.

*248 characters.*

**Provenance:** `la_money_wins.json` → `upsets[0]` (51.6× spending advantage) and
`n_upsets` (181).

**Note:** both are public figures in a public race, and the result reads well for
the winner — but the card names real people, so it's the one draft worth a second
look before posting.

---

## 4 · Raised vs spent → `out/c4.png`

> Something I got wrong at first:
>
> I ranked Louisiana candidates by money raised. But candidates here self-fund with loans that never show up as contributions.
>
> Rank by money spent and the money leader changes in 78 of 506 races. 19% of my "upsets" weren't upsets.

*262 characters.*

**Provenance:** `la_money_wins.json` → `compare.leader_flips` (78 of 506, 15.4%),
`compare.false_upsets` (33 of 173, 19.1%), `compare.by_tier` (Legislative 74.1%
raised vs 67.5% spent).

**Why post this one:** it's the draft that shows judgment rather than a finding.
The Legislative tier gap is the sharpest illustration — a result that looked
solid on fundraising largely dissolves on spending.

---

## 5 · Bonus, no card needed

> Across 11,620 Louisiana elections since 2000 I traced $356M in contributions written before election day.
>
> 68 cents of every dollar went to a candidate who went on to win.

*171 characters.*

**Provenance:** `la_donor_winrates.json` → `baseline` (11,620 valid races,
$355,979,424 in pre-election contributions, 0.682 winner dollar share).

**Keep it to that.** The per-donor leaderboard underneath it (747 donors with 20+
races, the best around 95%) is tempting but needs a caveat the tweet can't carry:
incumbency correlates with win rate at r = 0.46. Restricted to non-incumbent
bets the average only drops from 70% to 66%, so there's something real there —
it's a blog post, not a tweet.
