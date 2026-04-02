"""Home: img brand deve usare /brand-logo/ (proxy GCS), non /static/."""
import os
import subprocess
import sys
from pathlib import Path


def test_landing_html_uses_gcs_brand_logo():
    root = Path(__file__).resolve().parent.parent
    env = {**os.environ, "PYTHONPATH": str(root)}
    r = subprocess.run(
        [sys.executable, str(root / "scripts" / "check_landing_logo_html.py")],
        cwd=str(root),
        env=env,
    )
    assert r.returncode == 0
