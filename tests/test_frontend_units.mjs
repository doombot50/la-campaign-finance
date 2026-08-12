// test_frontend_units.mjs — unit tests for the dashboard's pure helpers.
// These functions live inline in louisiana-campaign-finance.html; we extract
// and exercise them directly (see _extract.mjs). Run: node --test tests/
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { extract } from './_extract.mjs';

const HTML = join(dirname(fileURLToPath(import.meta.url)), '..', 'louisiana-campaign-finance.html');
const fe = extract(HTML, {
  consts: ['PARTY_CSS'],
  fns: [
    '_raceDistrictNum', '_officeRank', '_raceDateSortKey', '_electionDateLabel',
    '_electionDateShort', '_raceAnchorId', '_cmpRaceYear', 'fmtRaisedShort',
    'outcomeClass', 'outcomeLabel', 'escHtml', 'partyBadge',
    '_runLimit', '_recYear',
  ],
});

// ── Office-importance ordering (the Races-tab ranking) ───────────────────────
test('_officeRank: federal offices rank above everything', () => {
  const pres = fe._officeRank('President of the United States')[0];
  const sen  = fe._officeRank('United States Senator')[0];
  const rep  = fe._officeRank('U. S. Representative -- 3rd Congressional District')[0];
  const gov  = fe._officeRank('Governor')[0];
  assert.ok(pres < sen && sen < rep && rep < gov, `${pres} < ${sen} < ${rep} < ${gov}`);
});

test('_officeRank: full importance ordering is monotonic', () => {
  const ordered = [
    'President of the United States',
    'United States Senator',
    'U. S. Representative -- 1st Congressional District',
    'Governor',
    'Lieutenant Governor',
    'Attorney General',
    'Public Service Commissioner -- District 3',
    'Member of BESE -- 2nd District',
    'Justice of the Supreme Court -- 1st District',
    'State Senator -- 5th Senatorial District',
    'State Representative -- 42nd Representative District',
    'Mayor-President',
    'Mayor',
    'Sheriff',
    'District Attorney -- 19th Judicial District',
    'Coroner',
    'Chief of Police -- Town of Delhi',
    'Constable',
    'Council Member -- District 1',
    'Member of School Board -- District 4',
    'Constitutional Amendment No. 1',
    'Proposition - Sales Tax Renewal',
  ];
  const ranks = ordered.map((o) => fe._officeRank(o)[0]);
  for (let i = 1; i < ranks.length; i++) {
    assert.ok(ranks[i] >= ranks[i - 1], `not monotonic at "${ordered[i]}" (${ranks[i]} < ${ranks[i - 1]})`);
  }
  // Ballot measures must sink to the very bottom.
  assert.ok(ranks.at(-1) >= 90 && ranks.at(-2) >= 90);
});

test('_officeRank: district is the secondary sort key (numeric, not lexical)', () => {
  const d1  = fe._officeRank('Council Member -- District 1');
  const d10 = fe._officeRank('Council Member -- District 10');
  assert.equal(d1[0], d10[0]);             // same office tier
  assert.ok(d1[1] < d10[1]);               // District 1 before District 10
});

test('_officeRank: Justice of the Peace is local, not a court', () => {
  const jp    = fe._officeRank('Justice of the Peace -- Ward 3');
  const judge = fe._officeRank('District Judge -- 19th Judicial District');
  assert.ok(jp[0] > judge[0], 'JP should rank below real judgeships');
});

test('_officeRank: presidential nominee/primary ranks at the top', () => {
  assert.equal(fe._officeRank('Presidential Nominee -- Republican Party')[0], 1);
});

test('_raceDistrictNum: parses digits, ordinals, and lettered seats', () => {
  assert.equal(fe._raceDistrictNum('Council -- District 7'), 7);
  assert.equal(fe._raceDistrictNum('U. S. Representative -- 6th Congressional District'), 6);
  assert.equal(fe._raceDistrictNum('Public Service Commissioner -- District E'), 5); // E = 5th letter
  assert.equal(fe._raceDistrictNum('Mayor'), 9999);                                   // unnumbered -> last
});

// ── Date helpers ─────────────────────────────────────────────────────────────
test('_raceDateSortKey: MM/DD/YYYY -> sortable YYYYMMDD', () => {
  assert.equal(fe._raceDateSortKey('11/05/2024'), '20241105');
  assert.equal(fe._raceDateSortKey('1/2/2003'), '20030102');
  assert.equal(fe._raceDateSortKey('garbage'), '00000000');
  // newer date sorts greater
  assert.ok(fe._raceDateSortKey('12/07/2024') > fe._raceDateSortKey('11/05/2024'));
});

test('_electionDateLabel: humanizes the date', () => {
  assert.equal(fe._electionDateLabel('11/05/2024'), 'November 5, 2024');
  assert.equal(fe._electionDateLabel('03/01/2023'), 'March 1, 2023');
  assert.equal(fe._electionDateLabel('bad'), 'bad');
});

test('_electionDateShort: compact date for the career timeline', () => {
  assert.equal(fe._electionDateShort('10/12/2019'), 'Oct 12, 2019');
  assert.equal(fe._electionDateShort('11/05/2024'), 'Nov 5, 2024');
  assert.equal(fe._electionDateShort('bad'), 'bad');
});

test('_cmpRaceYear: extracts the year', () => {
  assert.equal(fe._cmpRaceYear('11/05/2024'), '2024');
  assert.equal(fe._cmpRaceYear('2019-10-12'), '2019');
});

// ── Anchors & formatting ─────────────────────────────────────────────────────
test('_raceAnchorId: stable, URL-safe slug; matches the permalink target', () => {
  const a = fe._raceAnchorId('11/05/2024', 'U. S. Senator');
  assert.equal(a, 'race-11-05-2024-u-s-senator');
  // Same inputs must always yield the same id (career link <-> race card join).
  assert.equal(a, fe._raceAnchorId('11/05/2024', 'U. S. Senator'));
});

test('fmtRaisedShort: compact money formatting', () => {
  assert.equal(fe.fmtRaisedShort(0), '—');
  assert.equal(fe.fmtRaisedShort(950), '$950');
  assert.equal(fe.fmtRaisedShort(1500), '$2K');        // rounds
  assert.equal(fe.fmtRaisedShort(2_500_000), '$2.5M');
});

test('outcomeClass / outcomeLabel: win / loss / pending', () => {
  assert.equal(fe.outcomeClass('Elected'), 'cand-outcome-won');
  assert.equal(fe.outcomeClass('Defeated'), 'cand-outcome-lost');
  assert.equal(fe.outcomeClass('Runoff'), 'cand-outcome-run');
  assert.equal(fe.outcomeLabel('Elected'), '✓ Won');
  assert.equal(fe.outcomeLabel('Lost'), '✗ Lost');
  assert.equal(fe.outcomeLabel(''), 'Pending');
});

test('escHtml: escapes the dangerous characters', () => {
  assert.equal(fe.escHtml('<script>"&"</script>'),
    '&lt;script&gt;&quot;&amp;&quot;&lt;/script&gt;');
  assert.equal(fe.escHtml(null), '');
});

test('partyBadge: maps known parties to their CSS class', () => {
  assert.match(fe.partyBadge('REP'), /party-rep/);
  assert.match(fe.partyBadge('DEM'), /party-dem/);
  assert.match(fe.partyBadge('XYZ'), /party-ind/);   // unknown -> independent styling
  assert.match(fe.partyBadge(''), />\?</);           // empty -> "?"
});

// ── Concurrent cycle loading ──────────────────────────────────────────────────
test('_runLimit: runs every task, never more than `limit` at once', async () => {
  let inFlight = 0, peak = 0;
  const ran = [];
  const tasks = Array.from({ length: 9 }, (_, i) => async () => {
    inFlight++; peak = Math.max(peak, inFlight);
    await new Promise(r => setTimeout(r, 5));
    inFlight--; ran.push(i);
  });
  await fe._runLimit(tasks, 3);
  assert.equal(ran.length, 9);
  assert.equal(new Set(ran).size, 9);   // each task exactly once
  assert.ok(peak <= 3, `peak concurrency ${peak} exceeded limit 3`);
  assert.ok(peak > 1, 'tasks did not actually overlap');
});

test('_runLimit: a throwing task does not abort the others', async () => {
  const done = [];
  const tasks = [
    async () => { done.push(1); },
    async () => { throw new Error('boom'); },
    async () => { done.push(3); },
  ];
  await fe._runLimit(tasks, 2);   // must resolve, not reject
  assert.deepEqual(done.sort(), [1, 3]);
});

test('_recYear: fast ISO path, cache, and odd-format fallback', () => {
  const iso = { date: '2024-05-01' };
  assert.equal(fe._recYear(iso), 2024);
  assert.equal(iso._yr, 2024);                    // cached on the record
  iso.date = '1999-01-01';
  assert.equal(fe._recYear(iso), 2024);           // cache wins — date never mutates in practice
  assert.equal(fe._recYear({ date: '3/5/2024' }), 2024);   // CSV-upload format → Date fallback
  assert.ok(Number.isNaN(fe._recYear({ date: '' })));      // unparseable → NaN (filtered out)
});
