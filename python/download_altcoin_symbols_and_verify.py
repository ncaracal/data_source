#!/usr/bin/env python3
"""download_altcoin_symbols_and_verify.py

End-to-end altcoin pipeline for one (exchange, market):

  1. Refresh the latest exchange_info JSON from the exchange.
  2. Pick every TRADING USDT-quoted symbol minus the major-coin "except"
     list (``download_altcoin_symbols_and_verify_except.json``) and run
     the Rust ``data_source`` CLI to download / convert them to parquet.
     Cargo's own progress output is streamed live and tee'd to the log.
  3. Call ``verify_all_symbols_id.run(...)`` to verify id continuity over
     every parquet now on disk.

Stdout / stderr are mirrored into
``report/running_log/download_altcoin_symbols_and_verify.{UTC}.log``.

Usage:
    python download_altcoin_symbols_and_verify.py --exchange binance --market spot
    python download_altcoin_symbols_and_verify.py --exchange binance --market um
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Local import — verify module lives next to this script.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import verify_all_symbols_id as verifier  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parent.parent
TRADE_DATA = Path(os.environ.get("TRADE_DATA", "/ndata/trade/data"))
LOG_DIR = REPO_ROOT / "report" / "running_log"
EXCEPT_JSON = Path(__file__).resolve().parent \
    / "download_altcoin_symbols_and_verify_except.json"

# (exchange, market) -> (info URL, on-disk JSON path)
EXCHANGE_INFO: dict[tuple[str, str], tuple[str, Path]] = {
    ("binance", "spot"): (
        "https://api.binance.com/api/v3/exchangeInfo",
        TRADE_DATA / "binance" / "exchange_info_spot.json",
    ),
    ("binance", "um"): (
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        TRADE_DATA / "binance" / "exchange_info_um.json",
    ),
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
    log_path = LOG_DIR / f"download_altcoin_symbols_and_verify.{stamp}.log"
    log_file = open(log_path, "w", buffering=1, encoding="utf-8")
    sys.stdout = _Tee(sys.__stdout__, log_file)
    sys.stderr = _Tee(sys.__stderr__, log_file)
    return log_path, log_file


def log(msg: str) -> None:
    ts = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------- step 1: refresh exchange_info ---------------------------------

def refresh_exchange_info(exchange: str, market: str) -> Path:
    url, out_path = EXCHANGE_INFO[(exchange, market)]
    log(f"step 1: GET {url}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as resp:
        body = resp.read()
    info = json.loads(body)
    if not info.get("symbols"):
        raise RuntimeError(f"empty/invalid exchange_info from {url}")
    # write atomically: temp file + replace
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_bytes(body)
    tmp.replace(out_path)
    log(f"    wrote {out_path} ({len(body):,} bytes, "
        f"{len(info['symbols'])} symbols)")
    return out_path


# ---------- step 2: select + download -------------------------------------

def load_except_list() -> list[str]:
    if not EXCEPT_JSON.is_file():
        log(f"WARN: except list not found at {EXCEPT_JSON} — using empty")
        return []
    return json.loads(EXCEPT_JSON.read_text())


def select_symbols(exchange: str, market: str,
                   except_set: set[str]) -> list[str]:
    _, info_path = EXCHANGE_INFO[(exchange, market)]
    info = json.loads(info_path.read_text())
    syms_in = info.get("symbols", [])

    # Filter to TRADING + USDT-quoted (consistent with data_source's typical
    # backfill universe; the except list trims out the heavy majors).
    eligible = [
        s["symbol"] for s in syms_in
        if s.get("status") == "TRADING"
        and s.get("quoteAsset") == "USDT"
    ]
    selected = sorted(s for s in eligible if s not in except_set)
    skipped = sorted(s for s in eligible if s in except_set)
    log(f"    eligible TRADING/USDT: {len(eligible)} | "
        f"excluded (in except list): {len(skipped)} -> {skipped} | "
        f"selected: {len(selected)}")
    return selected


def download_symbols(market: str, symbols: list[str]) -> int:
    """Run the Rust CLI to download aggTrades for ``symbols``. Returns rc."""
    if not symbols:
        log("    no symbols to download")
        return 0

    # write the symbols-json side file inside the repo so it's easy to
    # rerun manually with the same input
    sj_dir = REPO_ROOT / "python" / "_run"
    sj_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(ZoneInfo("UTC")).strftime("%Y%m%d_%H%M%S")
    sj_path = sj_dir / f"altcoin_{market}_{stamp}.json"
    sj_path.write_text(json.dumps(symbols))
    log(f"    wrote symbols-json -> {sj_path}")

    market_args = (["-m", "spot"] if market == "spot"
                   else ["-m", "future", "--market-sub", "um"])
    bin_path = REPO_ROOT / "target" / "release" / "data_source"
    if bin_path.is_file():
        cmd = [str(bin_path)]
    else:
        cmd = ["cargo", "run", "--release", "--"]
    cmd += [
        "--symbols-json", str(sj_path),
        *market_args,
        "-d", "aggTrades",
    ]
    log(f"step 2: running download — {' '.join(cmd)}")
    return _run_streamed(cmd, cwd=str(REPO_ROOT))


def _run_streamed(cmd: list[str], cwd: str) -> int:
    """Run ``cmd``, line-stream stdout+stderr to our (tee'd) stdout."""
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


# ---------- step 3: verify -----------------------------------------------

def run_verify(exchange: str, market: str) -> int:
    log(f"step 3: verify id continuity — exchange={exchange} market={market}")
    return verifier.run(exchange, market)


# ---------- main ---------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--exchange", required=True,
                    choices=sorted({e for e, _ in EXCHANGE_INFO}))
    ap.add_argument("--market", required=True,
                    choices=sorted({m for _, m in EXCHANGE_INFO}))
    args = ap.parse_args()

    log_path, _ = setup_logging()
    log(f"logging to {log_path}")
    log(f"exchange={args.exchange} market={args.market}")

    except_list = load_except_list()
    except_set = set(except_list)
    log(f"except list ({len(except_list)}): {except_list}")

    refresh_exchange_info(args.exchange, args.market)
    symbols = select_symbols(args.exchange, args.market, except_set)

    rc_dl = download_symbols(args.market, symbols)
    log(f"download exited rc={rc_dl}")
    if rc_dl != 0:
        log("download step failed — skipping verify")
        return rc_dl

    rc_v = run_verify(args.exchange, args.market)
    log(f"verify exited rc={rc_v}")
    return rc_v


if __name__ == "__main__":
    sys.exit(main())
