#!/usr/bin/env python3
"""
build_social_cards.py — render-ready social cards from the committed data
========================================================================
Emits `social/cards.html`: four 1600x900 cards, each a self-contained data
graphic sized for a tweet. Every figure is read out of the shipped artifacts
rather than typed in, so a nightly data refresh flows through to the cards.

  1. money-advantage curve   la_money_wins.json  -> overall + by_advantage
  2. compass-PAC clique      factions.json       -> the four North/South/East/
                                                    West PAC nodes + all 6 edges
  3. biggest upset           la_money_wins.json  -> upsets[0]
  4. raised-vs-spent         la_money_wins.json  -> compare

factions.json lives in the la-donor-factions repo; it is auto-found as a
sibling checkout (override with --factions or $LA_FACTIONS). Card 2 is skipped
with a warning when it can't be located, so this still runs from a lone
checkout.

Fonts come from the repo's own `vendor/fonts/` — the same self-hosted Libre
Franklin + IBM Plex Mono the dashboard uses, so the cards carry the site's
typography with no network fetch.

Render to PNG with `node render_cards.mjs` (needs playwright + a chromium).
Stdlib only.
"""
import argparse
import json
import os
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(BASE)

# Palette. The dashboard's teal/orange, nudged just far enough to clear the
# chroma floor and the 3:1 mark-vs-surface contrast a chart mark needs
# (#14756b reads gray at chart scale; #e8833a lands at 2.7:1 on white).
TEAL = '#008a76'
ORANGE = '#c26410'
RAMP = ['#7cc2b6', '#43a696', '#008a76', '#005f4e']   # ordinal, light -> dark

FINANCE_URL = 'finance.charliestephens.xyz'
FACTIONS_URL = 'factions.charliestephens.xyz'

COMPASS = ('North PAC', 'South PAC', 'East PAC', 'West PAC')


def money(n):
    return '${:,}'.format(int(round(n)))


def load_money_wins(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def find_factions(explicit):
    """Explicit path wins and must exist; otherwise auto-find a sibling checkout."""
    if explicit:
        if not os.path.exists(explicit):
            raise SystemExit('--factions: no such file: {}'.format(explicit))
        return os.path.abspath(explicit)
    for cand in (os.environ.get('LA_FACTIONS'),
                 os.path.join(os.path.dirname(REPO), 'la-donor-factions', 'factions.json'),
                 os.path.join(REPO, '..', 'la-donor-factions', 'factions.json')):
        if cand and os.path.exists(cand):
            return os.path.abspath(cand)
    return None


def card1(mw):
    """Win rate of the biggest spender, by how big the spending edge was."""
    buckets = [{'label': b['bucket'].replace('<2x', 'under 2×')
                                    .replace('2-5x', '2–5×')
                                    .replace('5-10x', '5–10×')
                                    .replace('>10x', 'over 10×'),
                'rate': b['rate'], 'n': b['n'], 'fill': RAMP[i]}
               for i, b in enumerate(mw['by_advantage'])]
    top = mw['by_advantage'][-1]
    # "you still lose one race in N" — N from the top bucket's actual loss rate
    words = {2: 'two', 3: 'three', 4: 'four', 5: 'five', 6: 'six', 7: 'seven'}
    n_lose = round(1 / (1 - top['rate'])) if top['rate'] < 1 else 0
    return {
        'kicker': 'Louisiana · {:,} contested races · {}'.format(
            mw['overall']['n_races'], mw['span']),
        'head': 'Outspend your opponent 10‑to‑1,',
        'head2': 'and you still lose one race in {}.'.format(
            words.get(n_lose, n_lose)),
        'hero': '{:.0f}%'.format(mw['overall']['rate'] * 100),
        'buckets': buckets,
        'source': ('Source: LA Board of Ethics filings × LA Secretary of State results. '
                   'Non‑federal races, ≥2 candidates who spent money. '
                   'Money = spending through the election month.'),
        'url': FINANCE_URL,
    }


def card2(fac, span):
    """The four compass PACs and the six shared-donor edges between them."""
    if not fac:
        return None
    by_name = {n['name']: n for n in fac['nodes']}
    missing = [n for n in COMPASS if n not in by_name]
    if missing:
        print('card 2 skipped: no node for {}'.format(', '.join(missing)), file=sys.stderr)
        return None
    ids = {by_name[n]['id']: n.split()[0] for n in COMPASS}
    pairs = []
    for e in fac['edges']:
        if e['a'] in ids and e['b'] in ids:
            pairs.append({'a': ids[e['a']], 'b': ids[e['b']],
                          'j': e['jaccard'], 'shared': e['shared'],
                          'dollars': e['sharedDollars']})
    if len(pairs) != 6:
        print('card 2 skipped: expected 6 edges among the compass PACs, '
              'found {}'.format(len(pairs)), file=sys.stderr)
        return None
    pairs.sort(key=lambda p: -p['j'])
    avg_dollars = sum(p['dollars'] for p in pairs) / len(pairs)
    return {
        'kicker': 'Shared-donor network · lifetime {}'.format(span),
        'head': 'Four Louisiana PACs named after',
        'head2a': 'compass directions.',
        'head2b': ' Every pair shares',
        'head3': 'about half its donors.',
        'pairs': pairs,
        'dollar_note': 'Roughly ${:.1f}M flows through each overlap.'.format(
            avg_dollars / 1e6),
        'source': ("Overlap = Jaccard index on resolved donor identities — the share of "
                   "two PACs' combined donor list that appears on both. "
                   'Committee‑to‑committee transfers excluded.'),
        'url': FACTIONS_URL,
    }


def _title(name):
    """ELIZABETH A SMITH -> Elizabeth A. Smith."""
    out = []
    for w in name.split():
        if len(w) == 1:
            out.append(w.upper() + '.')
        else:
            out.append(w.capitalize())
    return ' '.join(out)


_MON = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')


def _month_year(mmddyyyy):
    """'11/06/2012' -> 'Nov 2012'."""
    try:
        mm, _dd, yyyy = mmddyyyy.split('/')
        return '{} {}'.format(_MON[int(mm) - 1], yyyy)
    except (ValueError, IndexError):
        return mmddyyyy


def card3(mw):
    """The single widest spend-to-result gap in the set."""
    u = mw['upsets'][0]
    n_upsets = mw['n_upsets']
    office = ' · '.join(p.strip() for p in u['office'].split('--') if p.strip())
    return {
        'kicker': '{} · {}'.format(office, _month_year(u['date'])),
        'head': 'One candidate spent {}.'.format(money(u['lead_spent'])),
        'head2': 'The other spent {} — and won.'.format(money(u['winner_spent'])),
        'loser': {'name': _title(u['name']), 'spent': u['lead_spent'],
                  'vote': u['lead_vote_pct'], 'fill': ORANGE},
        'winner': {'name': _title(u['winner']), 'spent': u['winner_spent'],
                   'vote': u['winner_vote_pct'], 'fill': TEAL},
        'ratio_note': 'A {}× spending advantage.'.format(u['ratio']),
        'vote_note': 'A {:.1f}‑point loss.'.format(
            u['winner_vote_pct'] - u['lead_vote_pct']),
        'source': ('One of <b>{}</b> races since 2000 where the biggest spender lost. '
                   'Spending = campaign expenditures filed in‑cycle through the '
                   'election month.'.format(n_upsets)),
        'url': FINANCE_URL,
    }


def card4(mw):
    """Why the money metric has to be spending, not fundraising."""
    c = mw['compare']
    flips, false_ups = c['leader_flips'], c['false_upsets']
    tiers = [{'tier': t['tier'], 'raised': t['raised_rate'], 'spent': t['spent_rate']}
             for t in c['by_tier']]
    tiers.sort(key=lambda t: -t['raised'])
    return {
        'kicker': 'Methodology · why the metric changes the answer',
        'head_a': 'Ranking candidates by money ',
        'head_b': 'raised',
        'head2': 'picks the wrong “money candidate”',
        'head3': 'in {:.0f}% of Louisiana races.'.format(flips['rate'] * 100),
        'sub_a': ('Louisiana candidates bankroll campaigns with personal loans that never '
                  'appear as contributions received. Rank by what they actually '),
        'sub_b': 'spent',
        'sub_c': ' and a headline finding falls apart:',
        'tiers': tiers,
        'stat1': '{:.0f}%'.format(false_ups['rate'] * 100),
        'stat1_lines': ['of the “upsets” on', 'fundraising were races',
                        'the winner outspent.'],
        'stat2': '{}'.format(flips['n']),
        'stat2_lines': ['of {} races swap'.format(flips['of']),
                        'money leader entirely.'],
        'source': ('Both measures truncated at the election month. {} races with a leader '
                   'under both definitions; {} disagree.'.format(flips['of'], flips['n'])),
        'url': FINANCE_URL,
    }


TEMPLATE = r'''<!doctype html>
<meta charset="utf-8">
<title>LA campaign finance — social cards</title>
<style>
@font-face{font-family:'Libre Franklin';src:url('../vendor/fonts/libre-franklin-latin-400-normal.woff2') format('woff2');font-weight:400;font-display:block}
@font-face{font-family:'Libre Franklin';src:url('../vendor/fonts/libre-franklin-latin-500-normal.woff2') format('woff2');font-weight:500;font-display:block}
@font-face{font-family:'Libre Franklin';src:url('../vendor/fonts/libre-franklin-latin-600-normal.woff2') format('woff2');font-weight:600;font-display:block}
@font-face{font-family:'Libre Franklin';src:url('../vendor/fonts/libre-franklin-latin-700-normal.woff2') format('woff2');font-weight:700;font-display:block}
@font-face{font-family:'Libre Franklin';src:url('../vendor/fonts/libre-franklin-latin-800-normal.woff2') format('woff2');font-weight:800;font-display:block}
@font-face{font-family:'IBM Plex Mono';src:url('../vendor/fonts/ibm-plex-mono-latin-400-normal.woff2') format('woff2');font-weight:400;font-display:block}
@font-face{font-family:'IBM Plex Mono';src:url('../vendor/fonts/ibm-plex-mono-latin-500-normal.woff2') format('woff2');font-weight:500;font-display:block}
@font-face{font-family:'IBM Plex Mono';src:url('../vendor/fonts/ibm-plex-mono-latin-600-normal.woff2') format('woff2');font-weight:600;font-display:block}
:root{--ink:#0f1c1a;--ink-2:#46534f;--muted:#8a948f;--rule:#e2e8e6;--surface:#fff;--teal:__TEAL__;--orange:__ORANGE__}
*{margin:0;padding:0;box-sizing:border-box}
body{background:#5a6663;font-family:'Libre Franklin',system-ui,sans-serif;-webkit-font-smoothing:antialiased}
.card{width:1600px;height:900px;background:var(--surface);color:var(--ink);padding:62px 72px 0;
      display:flex;flex-direction:column;position:relative;overflow:hidden;margin:24px auto}
.kicker{font-family:'IBM Plex Mono',monospace;font-size:19px;font-weight:500;letter-spacing:.13em;
        text-transform:uppercase;color:var(--teal);margin-bottom:22px}
h1{font-size:62px;line-height:1.06;font-weight:800;letter-spacing:-.022em;max-width:1360px}
h1 .lo{font-weight:400;color:var(--ink-2)}
.sub{font-size:25px;line-height:1.42;color:var(--ink-2);margin-top:20px;max-width:1180px}
.foot{margin-top:auto;border-top:1px solid var(--rule);padding:22px 0 30px;display:flex;
      justify-content:space-between;align-items:baseline;font-family:'IBM Plex Mono',monospace;
      font-size:17px;color:var(--muted);letter-spacing:.02em;gap:40px}
.foot b{color:var(--ink);font-weight:600}
.foot span:last-child{white-space:nowrap}
.plot{flex:1;display:flex;align-items:center}
text{font-family:'Libre Franklin',sans-serif}
.legend{display:flex;gap:34px;align-items:center;margin-top:26px}
.lg{display:flex;gap:11px;align-items:center;font-size:22px;color:var(--ink-2);font-weight:500}
.sw{width:16px;height:16px;border-radius:4px;flex:none}
</style>
<div id="cards"></div>
<script>
const DATA = __DATA__;
const NS='http://www.w3.org/2000/svg';
const INK='#0f1c1a', INK2='#46534f', MUTED='#8a948f', RULE='#e2e8e6', MONO='IBM Plex Mono, monospace';
function el(p,t,a,txt){const n=document.createElementNS(NS,t);for(const k in a)n.setAttribute(k,a[k]);
  if(txt!=null)n.textContent=txt;p.appendChild(n);return n;}
/* rounded at the data end only, square on the baseline */
function colPath(x,y,w,h,r){r=Math.min(r,h);return `M${x},${y+h} L${x},${y+r} Q${x},${y} ${x+r},${y} L${x+w-r},${y} Q${x+w},${y} ${x+w},${y+r} L${x+w},${y+h} Z`;}
function barPath(x,y,w,h,r){r=Math.min(r,w);return `M${x},${y} L${x+w-r},${y} Q${x+w},${y} ${x+w},${y+r} L${x+w},${y+h-r} Q${x+w},${y+h} ${x+w-r},${y+h} L${x},${y+h} Z`;}
function card(id,kicker,headHTML,extraHTML,svgH,footHTML,url){
  const d=document.createElement('div');d.className='card';d.id=id;
  d.innerHTML=`<div class="kicker">${kicker}</div><h1>${headHTML}</h1>${extraHTML||''}`
    +`<div class="plot"><svg width="1456" height="${svgH}" viewBox="0 0 1456 ${svgH}"></svg></div>`
    +`<div class="foot"><span>${footHTML}</span><span><b>${url}</b></span></div>`;
  document.getElementById('cards').appendChild(d);
  return d.querySelector('svg');
}

/* ── 1 · money-advantage curve ─────────────────────────────────────────── */
if(DATA.c1){const d=DATA.c1;
  const s=card('c1',d.kicker,`${d.head}<br><span class="lo">${d.head2}</span>`,'',520,d.source,d.url);
  el(s,'text',{x:0,y:130,'font-size':112,'font-weight':800,fill:INK,'letter-spacing':'-.03em'},d.hero);
  el(s,'text',{x:0,y:176,'font-size':21,fill:INK2,'font-weight':500},'of races go to the');
  el(s,'text',{x:0,y:206,'font-size':21,fill:INK2,'font-weight':500},'biggest spender.');
  el(s,'line',{x1:0,y1:240,x2:286,y2:240,stroke:RULE,'stroke-width':1});
  el(s,'text',{x:0,y:274,'font-size':19,fill:MUTED},'But the advantage');
  el(s,'text',{x:0,y:301,'font-size':19,fill:MUTED},'curve flattens fast →');
  const X0=470,BASE=400,TOP=52,H=BASE-TOP,BW=126,GAP=112;
  el(s,'line',{x1:X0-26,y1:BASE,x2:X0+d.buckets.length*(BW+GAP)-GAP+26,y2:BASE,stroke:'#c3c2b7','stroke-width':1});
  d.buckets.forEach((b,i)=>{
    const x=X0+i*(BW+GAP),h=Math.round(b.rate/0.80*H),y=BASE-h;
    el(s,'path',{d:colPath(x,y,BW,h,7),fill:b.fill});
    el(s,'text',{x:x+BW/2,y:y-22,'text-anchor':'middle','font-size':40,'font-weight':800,fill:INK,'letter-spacing':'-.02em'},Math.round(b.rate*100)+'%');
    el(s,'text',{x:x+BW/2,y:BASE+36,'text-anchor':'middle','font-size':23,'font-weight':600,fill:INK},b.label);
    el(s,'text',{x:x+BW/2,y:BASE+64,'text-anchor':'middle','font-size':18,fill:MUTED,'font-family':MONO},b.n+' races');
  });
  el(s,'text',{x:X0-26,y:BASE+106,'font-size':20,fill:MUTED,'font-weight':500},
    'Spending advantage of the race’s biggest spender  →  how often they won');
}

/* ── 2 · compass-PAC clique ────────────────────────────────────────────── */
if(DATA.c2){const d=DATA.c2;
  const s=card('c2',d.kicker,
    `${d.head}<br>${d.head2a}<span class="lo">${d.head2b}<br>${d.head3}</span>`,'',470,d.source,d.url);
  const CX=450,CY=222,R=64,VY=142,HX=286;
  const P={North:{x:CX,y:CY-VY},East:{x:CX+HX,y:CY},South:{x:CX,y:CY+VY},West:{x:CX-HX,y:CY}};
  d.pairs.forEach(p=>{const A=P[p.a],B=P[p.b];
    el(s,'line',{x1:A.x,y1:A.y,x2:B.x,y2:B.y,stroke:'__TEAL__','stroke-width':8,'stroke-opacity':.28,'stroke-linecap':'round'});});
  Object.entries(P).forEach(([t,n])=>{
    el(s,'circle',{cx:n.x,cy:n.y,r:R,fill:'__TEAL__',stroke:'#fff','stroke-width':4});
    el(s,'text',{x:n.x,y:n.y-3,'text-anchor':'middle','font-size':25,'font-weight':700,fill:'#fff'},t);
    el(s,'text',{x:n.x,y:n.y+23,'text-anchor':'middle','font-size':17,'font-weight':500,fill:'#b9e0d8','font-family':MONO},'PAC');
  });
  el(s,'text',{x:CX,y:462,'text-anchor':'middle','font-size':21,fill:MUTED,'font-weight':500},
    'Six possible pairs. All six are linked.');
  const RX=940;
  el(s,'text',{x:RX,y:52,'font-size':22,'font-weight':700,fill:INK},'Donor overlap, every pair');
  d.pairs.forEach((p,i)=>{const y=110+i*46;
    el(s,'text',{x:RX,y:y,'font-size':22,fill:INK2,'font-weight':500},`${p.a} ↔ ${p.b}`);
    el(s,'text',{x:RX+268,y:y,'font-size':22,fill:INK,'font-weight':700,'font-family':MONO,'text-anchor':'end'},Math.round(p.j*100)+'%');
    el(s,'text',{x:RX+300,y:y,'font-size':19,fill:MUTED,'font-family':MONO},p.shared.toLocaleString()+' shared donors');
    el(s,'line',{x1:RX,y1:y+16,x2:RX+506,y2:y+16,stroke:RULE,'stroke-width':1});
  });
  el(s,'text',{x:RX,y:110+d.pairs.length*46+22,'font-size':20,fill:MUTED},d.dollar_note);
}

/* ── 3 · the widest upset ──────────────────────────────────────────────── */
if(DATA.c3){const d=DATA.c3;
  const legend=`<div class="legend">`
    +`<div class="lg"><span class="sw" style="background:${d.loser.fill}"></span>${d.loser.name}</div>`
    +`<div class="lg"><span class="sw" style="background:${d.winner.fill}"></span>${d.winner.name}</div></div>`;
  const s=card('c3',d.kicker,`${d.head}<br><span class="lo">${d.head2}</span>`,legend,350,d.source,d.url);
  const BH=52,GAPB=26,W=430;
  function panel(x,title,rows,note,track){
    el(s,'text',{x:x,y:26,'font-size':20,'font-weight':600,fill:INK,'letter-spacing':'.09em','font-family':MONO},title);
    el(s,'line',{x1:x,y1:48,x2:x+W+180,y2:48,stroke:RULE,'stroke-width':1});
    const max=track||Math.max(...rows.map(r=>r.v));
    rows.forEach((r,i)=>{const y=104+i*(BH+GAPB);
      if(track)el(s,'rect',{x:x,y:y,width:W,height:BH,rx:7,fill:'#f0f4f3'});
      el(s,'path',{d:barPath(x,y,Math.max(5,Math.round(r.v/max*W)),BH,7),fill:r.c});
      el(s,'text',{x:x+W+22,y:y+BH/2+13,'font-size':36,'font-weight':800,fill:INK,'letter-spacing':'-.02em','font-family':MONO},r.l);
    });
    el(s,'text',{x:x,y:104+2*(BH+GAPB)+26,'font-size':21,fill:MUTED},note);
  }
  const fmt$=v=>'$'+v.toLocaleString();
  panel(0,'SPENT',[
    {v:d.loser.spent, c:d.loser.fill, l:fmt$(d.loser.spent)},
    {v:d.winner.spent,c:d.winner.fill,l:fmt$(d.winner.spent)}],d.ratio_note);
  panel(766,'VOTE SHARE',[
    {v:d.loser.vote, c:d.loser.fill, l:d.loser.vote.toFixed(1)+'%'},
    {v:d.winner.vote,c:d.winner.fill,l:d.winner.vote.toFixed(1)+'%'}],d.vote_note,100);
  el(s,'line',{x1:700,y1:0,x2:700,y2:330,stroke:RULE,'stroke-width':1});
}

/* ── 4 · raised vs spent ───────────────────────────────────────────────── */
if(DATA.c4){const d=DATA.c4;
  const head=`${d.head_a}<span style="color:__ORANGE__">${d.head_b}</span><br>${d.head2}`
    +`<br><span class="lo">${d.head3}</span>`;
  const sub=`<div class="sub">${d.sub_a}<b style="font-weight:600;color:__TEAL__">${d.sub_b}</b>${d.sub_c}</div>`;
  const s=card('c4',d.kicker,head,sub,400,d.source,d.url);
  const X0=280,X1=1060,LO=.58,HI=.78,px=v=>X0+(v-LO)/(HI-LO)*(X1-X0);
  el(s,'text',{x:0,y:20,'font-size':21,'font-weight':600,fill:INK},'How often the money leader won, by office tier');
  [.60,.65,.70,.75].forEach(t=>{
    el(s,'line',{x1:px(t),y1:44,x2:px(t),y2:266,stroke:RULE,'stroke-width':1});
    el(s,'text',{x:px(t),y:296,'text-anchor':'middle','font-size':18,fill:MUTED,'font-family':MONO},Math.round(t*100)+'%');
  });
  d.tiers.forEach((r,i)=>{const y=76+i*56;
    el(s,'text',{x:X0-34,y:y+8,'text-anchor':'end','font-size':23,'font-weight':600,fill:INK},r.tier);
    if(Math.abs(r.raised-r.spent)>.001)
      el(s,'line',{x1:px(r.raised),y1:y,x2:px(r.spent),y2:y,stroke:'#c3c2b7','stroke-width':3,'stroke-linecap':'round'});
    else
      el(s,'text',{x:px(r.spent)+26,y:y+7,'font-size':19,fill:MUTED},'identical — dots coincide');
    el(s,'circle',{cx:px(r.raised),cy:y,r:11,fill:'__ORANGE__',stroke:'#fff','stroke-width':2});
    el(s,'circle',{cx:px(r.spent), cy:y,r:11,fill:'__TEAL__',  stroke:'#fff','stroke-width':2});
  });
  /* direct-label only the row the story is about */
  const lead=d.tiers[0];
  el(s,'text',{x:px(lead.raised)+24,y:84,'font-size':22,'font-weight':700,fill:INK,'font-family':MONO},Math.round(lead.raised*100)+'%');
  el(s,'text',{x:px(lead.spent)-24,y:84,'text-anchor':'end','font-size':22,'font-weight':700,fill:INK,'font-family':MONO},Math.round(lead.spent*100)+'%');
  el(s,'circle',{cx:X0+4,cy:340,r:9,fill:'__ORANGE__'});
  el(s,'text',{x:X0+22,y:347,'font-size':20,fill:INK2,'font-weight':500},'ranked by money raised');
  el(s,'circle',{cx:X0+308,cy:340,r:9,fill:'__TEAL__'});
  el(s,'text',{x:X0+326,y:347,'font-size':20,fill:INK2,'font-weight':500},'ranked by money spent');
  /* callout figures stay in ink — a series colour here would read as a series */
  const CX=1190;
  el(s,'line',{x1:CX-46,y1:30,x2:CX-46,y2:330,stroke:RULE,'stroke-width':1});
  el(s,'text',{x:CX,y:74,'font-size':54,'font-weight':800,fill:INK,'letter-spacing':'-.02em'},d.stat1);
  d.stat1_lines.forEach((t,i)=>el(s,'text',{x:CX,y:108+i*26,'font-size':20,fill:INK2,'font-weight':500},t));
  el(s,'text',{x:CX,y:250,'font-size':54,'font-weight':800,fill:INK,'letter-spacing':'-.02em'},d.stat2);
  d.stat2_lines.forEach((t,i)=>el(s,'text',{x:CX,y:284+i*26,'font-size':20,fill:INK2,'font-weight':500},t));
}
</script>
'''


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    ap.add_argument('--money-wins', default=os.path.join(REPO, 'la_money_wins.json'))
    ap.add_argument('--factions', default=None,
                    help='path to la-donor-factions/factions.json (auto-found as a sibling)')
    ap.add_argument('-o', '--out', default=os.path.join(BASE, 'cards.html'))
    args = ap.parse_args()

    mw = load_money_wins(args.money_wins)
    fpath = find_factions(args.factions)
    fac = None
    if fpath:
        with open(fpath, encoding='utf-8') as f:
            fac = json.load(f)
        print('factions: {}'.format(fpath), file=sys.stderr)
    else:
        print('factions.json not found — card 2 will be skipped '
              '(pass --factions or set $LA_FACTIONS)', file=sys.stderr)

    data = {'c1': card1(mw), 'c2': card2(fac, mw['span']),
            'c3': card3(mw), 'c4': card4(mw)}
    data = {k: v for k, v in data.items() if v}

    out = (TEMPLATE
           .replace('__DATA__', json.dumps(data, ensure_ascii=False))
           .replace('__TEAL__', TEAL)
           .replace('__ORANGE__', ORANGE))
    with open(args.out, 'w', encoding='utf-8') as f:
        f.write(out)
    print('wrote {} ({} cards)'.format(args.out, len(data)), file=sys.stderr)


if __name__ == '__main__':
    main()
