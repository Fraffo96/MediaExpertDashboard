"""
Importa loghi da una cartella locale (es. Downloads\\Loghi), rinomina come {brand_id}.png
e carica su GCS (richiede gsutil e GOOGLE_APPLICATION_CREDENTIALS / gcloud auth).

SVG: pip install svglib reportlab

Esempio:
  python scripts/import_brand_logos_from_folder.py "C:\\Users\\...\\Loghi" gs://bucket/brands
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Nome file (esatto) nella cartella sorgente -> brand_id (da app/constants.py ADMIN_BRANDS)
LOGHI_TO_BRAND_ID: dict[str, int] = {
    "0-2324_acer-logo-png-clipart.png": 20,
    "Amica_Wronki_(Unternehmen)_logo.svg": 34,
    "Apple-Logo.png": 8,
    "ASUS_Corporate_Logo.svg.png": 19,
    "Beats-Logo.png": 45,
    "Beko_2014_logo.png": 32,
    "Bosch_logo.png": 29,
    "Bose-logo.png": 42,
    "Braun_(company)-Logo.wine.png": 47,
    "canon-inc.-logo-black-and-white.png": 49,
    "Dell_Logo.svg.png": 16,
    "DeLonghi-Logo.png": 37,
    "DJI_Innovations_logo.svg.png": 52,
    "Dyson_(Unternehmen)_logo.svg": 36,
    "Electrolux-logo.png": 33,
    "Fitbit_logo16.svg.png": 55,
    "Garmin_logo_2006.svg.png": 15,
    "Google-Logo.wine.png": 39,
    "GoPro_logo.svg.png": 51,
    "Hisense.svg": 6,
    "HP_logo_2025.svg.png": 17,
    "HyperX_Logo.svg.png": 28,
    "JBL-Logo.wine.png": 43,
    "Lenovo_Global_Corporate_Logo.png": 18,
    "LG_logo_(2014).svg.png": 2,
    "logo-whiting.png": 54,
    "Logo_of_the_TCL_Corporation.svg.png": 5,
    "Marshall_Amplification-Logo.wine.png": 44,
    "Microsoft_logo_(2012).svg.png": 24,
    "Motorola_logo.svg.png": 13,
    "MSI-Logo.png": 21,
    "Nikon_Logo.svg.png": 50,
    "Nintendo.svg.png": 25,
    "oneplus.png": 14,
    "Oppo-Logo.wine.png": 10,
    "Oral-B-logo.png": 48,
    "Panasonic_logo_(Blue).svg.png": 7,
    "Philips_logo_new.svg.png": 4,
    "png-transparent-krups-logo.png": 38,
    "Razer-logo.png": 26,
    "Realme_logo.png": 11,
    "Remington_logo.png": 53,
    "Ring_logo.svg.png": 41,
    "Samsung_old_logo_before_year_2015.svg.png": 1,
    "Sennheiser-Logo.svg.png": 46,
    "Siemens-logo.svg.png": 30,
    "Sony-logo.png": 3,
    "Steelseries-logo.png": 27,
    "Tefal_logo.svg": 35,
    "TPLINK_Logo_2.svg.png": 23,
    "Whirlpool_Corporation_Logo.png": 31,
    "woodland-gardening-amazon-png-logo-vector-8.png": 40,
    "Xiaomi_logo_(2021-).svg.png": 9,
}


def _svg_to_png(src: Path, dest: Path) -> bool:
    try:
        from reportlab.graphics import renderPM
        from svglib.svglib import svg2rlg
    except ImportError:
        print("Installa: pip install svglib reportlab", file=sys.stderr)
        return False
    drawing = svg2rlg(str(src))
    if drawing is None:
        return False
    ow, oh = float(drawing.width), float(drawing.height)
    max_side = 560.0
    if ow and oh and max(ow, oh) > max_side:
        s = max_side / max(ow, oh)
        drawing.width, drawing.height = ow * s, oh * s
        drawing.scale(s, s)
    renderPM.drawToFile(drawing, str(dest), fmt="PNG", dpi=144)
    return True


def _prepare_staging(src_dir: Path, stage: Path) -> list[Path]:
    out: list[Path] = []
    for fname, bid in LOGHI_TO_BRAND_ID.items():
        p = src_dir / fname
        if not p.is_file():
            print(f"missing skip: {fname}")
            continue
        dest = stage / f"{bid}.png"
        suf = p.suffix.lower()
        if suf == ".svg":
            if _svg_to_png(p, dest):
                print(f"svg  -> {dest.name}")
                out.append(dest)
            else:
                print(f"svg fail: {fname}")
        else:
            shutil.copy2(p, dest)
            print(f"copy -> {dest.name}")
            out.append(dest)
    return out


def _gsutil() -> str:
    for name in ("gsutil", "gsutil.cmd"):
        p = shutil.which(name)
        if p:
            return p
    sys.exit("gsutil non trovato nel PATH (installa Google Cloud SDK).")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("source_dir", type=Path, help="Cartella con i loghi (es. Downloads\\Loghi)")
    ap.add_argument(
        "gs_destination",
        nargs="?",
        default="gs://mediaexpertdashboard-brand-logos/brands",
        help="Path gsutil destinazione",
    )
    args = ap.parse_args()
    src = args.source_dir.expanduser().resolve()
    if not src.is_dir():
        sys.exit(f"Cartella non trovata: {src}")

    with tempfile.TemporaryDirectory() as td:
        stage = Path(td)
        _prepare_staging(src, stage)
        files = sorted(stage.glob("*.png"))
        if not files:
            sys.exit("Nessun file preparato.")

        dest = args.gs_destination.rstrip("/") + "/"
        g = _gsutil()
        subprocess.run([g, "-m", "cp", *[str(f) for f in files], dest], check=True)


if __name__ == "__main__":
    main()
