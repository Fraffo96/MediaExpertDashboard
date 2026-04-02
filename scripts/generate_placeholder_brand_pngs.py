"""Genera PNG segnaposto (tinta unita neutra) per ogni brand_id in ADMIN_BRANDS.

Usare per sviluppo locale senza bucket GCS. Non sono loghi di marca: in produzione
caricare PNG reali su GCS (BRAND_LOGOS_PUBLIC_BASE) o in static/img/brands/.
I loghi non risiedono in Firestore (solo brand_id sull'utente).
"""
from __future__ import annotations

import struct
import zlib
from pathlib import Path

# Esegui dalla root del repo: python scripts/generate_placeholder_brand_pngs.py
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.constants import ADMIN_BRANDS


def _write_solid_png(path: Path, width: int, height: int, r: int, g: int, b: int) -> None:
    raw = bytearray()
    row = bytes([0] + [r, g, b] * width)
    for _ in range(height):
        raw.extend(row)
    compressed = zlib.compress(bytes(raw), 9)

    def chunk(tag: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack("!I", len(data)) + tag + data + struct.pack("!I", crc)

    ihdr = struct.pack("!2I5B", width, height, 8, 2, 0, 0, 0)
    out = b"\x89PNG\r\n\x1a\n"
    out += chunk(b"IHDR", ihdr)
    out += chunk(b"IDAT", compressed)
    out += chunk(b"IEND", b"")
    path.write_bytes(out)


def _rgb_for_id(brand_id: int) -> tuple[int, int, int]:
    """Grigio scuro leggermente variabile (evita rettangoli verdi/accesi da hash)."""
    base = 42 + (brand_id * 13) % 24  # ~42–65
    return base, base + 3, base + 6


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    out_dir = root / "app" / "static" / "img" / "brands"
    out_dir.mkdir(parents=True, exist_ok=True)
    w, h = 280, 80
    for row in ADMIN_BRANDS:
        bid = int(row["brand_id"])
        rgb = _rgb_for_id(bid)
        _write_solid_png(out_dir / f"{bid}.png", w, h, *rgb)
        print(f"wrote {bid}.png")
    print(f"OK -> {out_dir} ({len(ADMIN_BRANDS)} files)")


if __name__ == "__main__":
    main()
