#!/usr/bin/env python3
"""altcoin_symbols_to_kline.py

For one (exchange, market):

  1. Load the altcoin "except" list (``altcoin_symbols_except.json``).
  2. List every symbol directory under
     ``$TRADE_DATA/<exchange>/<market>/aggTrades/``.
  3. Drop the excepted symbols and run the Rust ``agg_kline`` CLI on the
     remainder, writing klines into ``aggTrades_kline/<symbol>/...``.

The agg_kline binary is incremental — it skips up-to-date interval files
per year — so re-running daily is cheap after the initial pass.

Stdout / stderr are mirrored into
``report/running_log/altcoin_symbols_to_kline.{UTC}.log``.

Usage:
    python altcoin_symbols_to_kline.py --exchange binance --market spot
    python altcoin_symbols_to_kline.py --exchange binance --market um
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parent.parent
TRADE_DATA = Path(os.environ.get("TRADE_DATA", "/ndata/trade/data"))
LOG_DIR = REPO_ROOT / "report" / "running_log"
EXCEPT_JSON = Path(__file__).resolve().parent / "altcoin_symbols_except.json"

MARKET_PATHS: dict[str, Path] = {
    "spot": Path("spot"),
    "um": Path("future") / "um",
}


# ---------- logging --------------------------------------------------------

class _Tee:
    """Write to multiple text streams simultaneously (terminal + log file)."""

    def __init__(self, *streams):
        self._streams = streams

    def write(self, data: str) -> int:
        for s in self._streams:
            s.write(data)
            s.flush()
        return len(data)

    def flush(self) -> None:
        for s in self._streams:
            s.flush()

    def isatty(self) -> bool:
        return False


def setup_logging() -> tuple[Path, "object"]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(ZoneInfo("UTC")).strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"altcoin_symbols_to_kline.{stamp}.log"
    log_file = open(log_path, "w", buffering=1, encoding="utf-8")
    sys.stdout = _Tee(sys.__stdout__, log_file)
    sys.stderr = _Tee(sys.__stderr__, log_file)
    return log_path, log_file


def log(msg: str) -> None:
    ts = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------- symbol selection ---------------------------------------------

def load_except_list() -> list[str]:
    if not EXCEPT_JSON.is_file():
        log(f"WARN: except list not found at {EXCEPT_JSON} — using empty")
        return []
    return json.loads(EXCEPT_JSON.read_text())


def select_symbols(exchange: str, market: str,
                   except_set: set[str]) -> list[str]:
    market_path = MARKET_PATHS[market]
    src = TRADE_DATA / exchange / market_path / "aggTrades"
    if not src.is_dir():
        raise SystemExit(f"source not found: {src}")
    all_dirs = sorted(
        p.name for p in src.iterdir()
        if p.is_dir() and not p.name.startswith("_")
    )
    selected = [s for s in all_dirs if s not in except_set]
    skipped = sorted(s for s in all_dirs if s in except_set)
    log(f"    src: {src}")
    log(f"    all dirs: {len(all_dirs)} | "
        f"excluded: {len(skipped)} -> {skipped} | "
        f"selected: {len(selected)}")
    return selected


# ---------- runner -------------------------------------------------------

def run_kline(market: str, symbols: list[str]) -> int:
    if not symbols:
        log("    no symbols selected — nothing to do")
        return 0

    bin_path = REPO_ROOT / "target" / "release" / "agg_kline"
    if bin_path.is_file():
        cmd = [str(bin_path)]
    else:
        cmd = ["cargo", "run", "--release", "--bin", "agg_kline", "--"]

    market_arg = "spot" if market == "spot" else "um"
    cmd += ["-m", market_arg]
    # agg_kline's `-s` is repeat-style (no `num_args = 1..`), so each
    # symbol needs its own flag.
    for s in symbols:
        cmd += ["-s", s]

    log(f"running: agg_kline -m {market_arg} -s ... ({len(symbols)} symbols)")
    return _run_streamed(cmd, cwd=str(REPO_ROOT))


def _run_streamed(cmd: list[str], cwd: str) -> int:
    proc = subprocess.Popen(
        cmd, cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in iter(proc.stdout.readline, ""):
        sys.stdout.write(line)
    proc.wait()
    return proc.returncode


# ---------- main ---------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--exchange", required=True, choices=["binance"])
    ap.add_argument("--market", required=True,
                    choices=sorted(MARKET_PATHS))
    args = ap.parse_args()

    log_path, _ = setup_logging()
    log(f"logging to {log_path}")
    log(f"exchange={args.exchange} market={args.market}")

    except_list = load_except_list()
    log(f"except list ({len(except_list)}): {except_list}")

    symbols = select_symbols(args.exchange, args.market, set(except_list))
    rc = run_kline(args.market, symbols)
    log(f"agg_kline exited rc={rc}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
