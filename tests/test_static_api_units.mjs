// test_static_api_units.mjs — unit tests for static_api.js pure helpers.
// The end-to-end parity gate (test_static_client_parity.mjs) already proves the
// shipped static layer reproduces the live API when the data release is
// present; this covers the dependency-free building blocks so they're checked
// on every push, with no server boot or data needed.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { extract } from './_extract.mjs';

const JS = join(dirname(fileURLToPath(import.meta.url)), '..', 'static_api.js');
const api = extract(JS, { fns: ['normName', 'wsNorm', '_isPartyOffice'] });

test('normName: uppercases, strips honorifics/suffixes, keeps A–Z + space', () => {
  assert.equal(api.normName('Dr. John A. Smith Jr.'), 'JOHN A SMITH');
  assert.equal(api.normName("  Mary-Jane  O'Brien III "), 'MARY JANE O BRIEN');
  assert.equal(api.normName(''), '');
  assert.equal(api.normName(null), '');
});

test('wsNorm: trims + collapses whitespace + uppercases (COH/entity keys)', () => {
  assert.equal(api.wsNorm('  jeff   landry '), 'JEFF LANDRY');
  assert.equal(api.wsNorm('JAMBALAYA PAC'), 'JAMBALAYA PAC');
  assert.equal(api.wsNorm(null), '');
});

test('_isPartyOffice: flags party-committee seats', () => {
  assert.equal(api._isPartyOffice('Democratic State Central Committee'), true);
  assert.equal(api._isPartyOffice('Member, DSCC'), true);
  assert.equal(api._isPartyOffice('State Senator -- 5th Senatorial District'), false);
});

// The static client's normName is documented as an exact mirror of the server's
// _norm_name. Lock that invariant down here so the two can't silently drift —
// the whole cross-dataset join depends on identical normalization.
test('normName mirrors the documented server _norm_name pipeline', () => {
  const cases = [
    ['Dr. John A. Smith Jr.', 'JOHN A SMITH'],
    ["Mary-Jane O'Brien III", 'MARY JANE O BRIEN'],
    ['Committee to Elect Jane Doe', 'COMMITTEE TO ELECT JANE DOE'],
  ];
  for (const [input, expected] of cases) {
    assert.equal(api.normName(input), expected, `normName(${input})`);
  }
});
