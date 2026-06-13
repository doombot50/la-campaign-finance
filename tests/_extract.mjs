// _extract.mjs — pull named functions/consts out of a source file as live
// values, so the frontend's pure logic can be unit-tested in Node without a
// browser or a build step. We brace-match the declaration text and evaluate the
// collected snippet in one scope. This only works for self-contained helpers
// (no DOM / global dependencies) — that's exactly the layer we test here.
import { readFileSync } from 'node:fs';

// Brace-match a `function NAME(...) { ... }` declaration starting at `from`.
function grabFunction(src, name) {
  // Match the exact name followed by `(` so `foo` doesn't grab `fooHtml`.
  const m = new RegExp(`function\\s+${name}\\s*\\(`).exec(src);
  if (!m) throw new Error(`function not found: ${name}`);
  const start = m.index;
  let depth = 0, started = false, i = start;
  for (; i < src.length; i++) {
    const c = src[i];
    if (c === '{') { depth++; started = true; }
    else if (c === '}') { depth--; if (started && depth === 0) { i++; break; } }
  }
  return src.slice(start, i);
}

// Grab a single-line `const NAME = ...;` declaration.
function grabConst(src, name) {
  const re = new RegExp(`const ${name}\\s*=.*?;`, 's');
  const m = src.match(re);
  if (!m) throw new Error(`const not found: ${name}`);
  return m[0];
}

/**
 * extract(filePath, { fns: [...], consts: [...] }) -> { name: value, ... }
 * Consts are evaluated before functions so functions can close over them.
 */
export function extract(filePath, { fns = [], consts = [] } = {}) {
  const src = readFileSync(filePath, 'utf8');
  const pieces = [
    ...consts.map((c) => grabConst(src, c)),
    ...fns.map((f) => grabFunction(src, f)),
  ];
  const names = [...consts, ...fns];
  // new Function avoids strict-mode eval scoping quirks and hands back the values.
  const factory = new Function(`${pieces.join('\n')}\nreturn { ${names.join(', ')} };`);
  return factory();
}
