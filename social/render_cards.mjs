/**
 * render_cards.mjs — screenshot social/cards.html into out/<id>.png
 *
 *   python3 build_social_cards.py && node render_cards.mjs
 *
 * Needs playwright and a chromium. Nothing else in the repo depends on this —
 * it exists so the cards can be regenerated after a data refresh rather than
 * re-cut by hand. Outputs 3200x1800 (2x of the 1600x900 card), which is what
 * Twitter/X wants for a 16:9 image.
 */
import { chromium } from 'playwright';
import { mkdirSync } from 'node:fs';
import path from 'node:path';

const here = path.dirname(new URL(import.meta.url).pathname);
mkdirSync(path.join(here, 'out'), { recursive: true });

const browser = await chromium.launch();
const page = await browser.newPage({
  viewport: { width: 1700, height: 1000 },
  deviceScaleFactor: 2,
});
await page.goto('file://' + path.join(here, 'cards.html'));
await page.waitForFunction(() => document.fonts.status === 'loaded');
await page.waitForTimeout(300);

const ids = await page.$$eval('.card', els => els.map(e => e.id));
for (const id of ids) {
  await page.locator('#' + id).screenshot({ path: path.join(here, 'out', `${id}.png`) });
  console.error('wrote out/' + id + '.png');
}
await browser.close();
