"""
Build one viewer/<market>/index.json per market (doge, hype, eth, bnb)
so the per-market GitHub Pages viewer stays in sync with committed CSVs.
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CSV_ROOT = ROOT / "csv"
VIEWER_ROOT = Path(__file__).resolve().parent

MARKETS = ["doge", "hype", "eth", "bnb"]


def extract_winner(csv_path):
    try:
        with open(csv_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            chunk = min(size, 4096)
            f.seek(-chunk, os.SEEK_END)
            tail = f.read().decode("utf-8", errors="replace")
        for line in reversed(tail.splitlines()):
            if "# RESULT" in line and "winner=" in line:
                m = re.search(r"winner=(\w+)", line)
                if m:
                    return m.group(1)
    except Exception:
        return None
    return None


def slug_to_epoch(slug):
    m = re.search(r"-(\d{10,})$", slug)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    return 0


def build_for(market):
    csv_dir = CSV_ROOT / market
    viewer_dir = VIEWER_ROOT / market
    viewer_dir.mkdir(parents=True, exist_ok=True)

    if not csv_dir.exists():
        print(f"  [{market}] no csv dir, skipping")
        return 0

    files = sorted(csv_dir.glob(f"{market}-updown-5m-*.csv"))
    windows = []
    for p in files:
        windows.append({
            "slug": p.stem,
            "filename": p.name,
            "winner": extract_winner(p),
            "epoch": slug_to_epoch(p.stem),
        })
    windows.sort(key=lambda w: w["epoch"], reverse=True)

    idx = {
        "market": market,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "count": len(windows),
        "windows": windows,
    }
    (viewer_dir / "index.json").write_text(json.dumps(idx, indent=2))
    print(f"  [{market}] wrote {len(windows)} windows -> viewer/{market}/index.json")
    return len(windows)


def main():
    total = 0
    for m in MARKETS:
        total += build_for(m)
    print(f"Built indexes for {len(MARKETS)} markets, {total} total windows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
