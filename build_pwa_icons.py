#!/usr/bin/env python3
"""
build_pwa_icons.py — generate the PWA / home-screen icons
==========================================================
Writes icon-192.png, icon-512.png and apple-touch-icon.png (180px) to the
repo root. The icons are committed; re-run only if the design changes.

Design: three rising bars in the Mardi Gras colors (purple, gold, green —
the dashboard's brand stripe) on the dashboard's dark background, drawn
inside the maskable safe zone (inner 80%).

Stdlib only — the PNGs are assembled by hand (zlib-compressed raw
scanlines), no Pillow required.
"""
import struct
import sys
import zlib

BG     = (0x0d, 0x11, 0x17)   # --bg
PURPLE = (0x7b, 0x2d, 0x8b)
GOLD   = (0xc8, 0xa9, 0x51)
GREEN  = (0x2d, 0x8a, 0x45)


def _chunk(tag, payload):
    return (struct.pack('>I', len(payload)) + tag + payload +
            struct.pack('>I', zlib.crc32(tag + payload) & 0xffffffff))


def write_png(path, size):
    # Bar geometry, as fractions of the icon size. Bars sit on a common
    # baseline at 78% height and stay inside the maskable inner 80%.
    bar_w = 0.16
    gap = 0.06
    baseline = 0.78
    bars = [          # (left, top, color)
        (0.5 - 1.5 * bar_w - gap,       0.46, PURPLE),
        (0.5 - 0.5 * bar_w,             0.22, GOLD),
        (0.5 + 0.5 * bar_w + gap,       0.34, GREEN),
    ]
    px = [[BG] * size for _ in range(size)]
    for left, top, color in bars:
        x0, x1 = int(left * size), int((left + bar_w) * size)
        y0, y1 = int(top * size), int(baseline * size)
        for y in range(y0, y1):
            for x in range(x0, x1):
                px[y][x] = color

    raw = b''.join(b'\x00' + bytes(c for p in row for c in p) for row in px)
    ihdr = struct.pack('>IIBBBBB', size, size, 8, 2, 0, 0, 0)  # 8-bit RGB
    with open(path, 'wb') as fh:
        fh.write(b'\x89PNG\r\n\x1a\n')
        fh.write(_chunk(b'IHDR', ihdr))
        fh.write(_chunk(b'IDAT', zlib.compress(raw, 9)))
        fh.write(_chunk(b'IEND', b''))
    print(f'wrote {path} ({size}x{size})')


def main():
    write_png('icon-192.png', 192)
    write_png('icon-512.png', 512)
    write_png('apple-touch-icon.png', 180)
    return 0


if __name__ == '__main__':
    sys.exit(main())
