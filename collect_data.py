#!/usr/bin/env python3
"""
GRAVESTONE — Data Collector
Fetches daily, 4H, and 1H klines for all Binance USDT perpetuals.
Pushes CSV files to a GitHub repo for persistent storage.

ENV VARS:
  GITHUB_TOKEN  — personal access token with repo scope
  GITHUB_REPO   — owner/repo (e.g., myuser/gravestone-data)
"""

import json
import os
import sys
import time
import base64
import urllib.request
import urllib.error
from datetime import datetime, timezone

BIN_BASE = "https://fapi.binance.com"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
GITHUB_API = "https://api.github.com"

SKIP_SYMBOLS = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "EURUSDT", "GBPUSDT"
}

TIMEFRAMES = {
    "1d": {"candles": 800, "label": "daily"},
    "4h": {"candles": 4500, "label": "4H"},
    "1h": {"candles": 4500, "label": "1H"},
}

# ══════════════════════════════════════════════════════════════
#  API HELPERS
# ══════════════════════════════════════════════════════════════

def fetch_json(url, retries=3, delay=1.0):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "GravestoneCollector/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                raise

def get_symbols():
    data = fetch_json(f"{BIN_BASE}/fapi/v1/exchangeInfo")
    symbols = []
    for s in data["symbols"]:
        if (s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
            and s["contractType"] == "PERPETUAL" and s["symbol"] not in SKIP_SYMBOLS):
            symbols.append(s["symbol"])
    return symbols

def get_klines_chunk(symbol, timeframe, limit, end_time=None):
    url = f"{BIN_BASE}/fapi/v1/klines?symbol={symbol}&interval={timeframe}&limit={limit}"
    if end_time:
        url += f"&endTime={end_time}"
    data = fetch_json(url)
    return data  # raw arrays

def get_klines_extended(symbol, timeframe, total_candles):
    all_candles = []
    end_time = None
    remaining = total_candles
    while remaining > 0:
        chunk_size = min(remaining, 1500)
        chunk = get_klines_chunk(symbol, timeframe, chunk_size, end_time)
        if not chunk:
            break
        all_candles = chunk + all_candles
        end_time = int(chunk[0][0]) - 1
        remaining -= len(chunk)
        if len(chunk) < chunk_size:
            break
        time.sleep(0.15)
    return all_candles

# ══════════════════════════════════════════════════════════════
#  CSV CONVERSION
# ══════════════════════════════════════════════════════════════

def klines_to_csv(raw_klines):
    """Convert raw Binance kline arrays to CSV string."""
    lines = ["timestamp,open,high,low,close,volume,quote_volume,trades,close_time"]
    for k in raw_klines:
        lines.append(f"{k[0]},{k[1]},{k[2]},{k[3]},{k[4]},{k[5]},{k[7]},{k[8]},{k[6]}")
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════
#  GITHUB API
# ══════════════════════════════════════════════════════════════

def github_request(method, path, data=None):
    """Make a GitHub API request."""
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "GravestoneCollector/1.0",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        if e.code == 404:
            return None  # file doesn't exist yet
        if e.code == 422 and "sha" in error_body:
            return {"error": "sha_conflict"}
        print(f"      GitHub API error {e.code}: {error_body[:200]}")
        return {"error": str(e.code)}

def get_file_sha(filepath):
    """Get the SHA of an existing file (needed for updates)."""
    result = github_request("GET", f"contents/{filepath}")
    if result and "sha" in result:
        return result["sha"]
    return None

def push_file(filepath, content, message):
    """Create or update a file in the GitHub repo."""
    encoded = base64.b64encode(content.encode()).decode()
    sha = get_file_sha(filepath)

    data = {
        "message": message,
        "content": encoded,
    }
    if sha:
        data["sha"] = sha

    result = github_request("PUT", f"contents/{filepath}", data)
    if result and "content" in result:
        return True
    if result and result.get("error") == "sha_conflict":
        # Retry with fresh SHA
        sha = get_file_sha(filepath)
        if sha:
            data["sha"] = sha
            result = github_request("PUT", f"contents/{filepath}", data)
            return result and "content" in result
    return False

# ══════════════════════════════════════════════════════════════
#  MAIN COLLECTOR
# ══════════════════════════════════════════════════════════════

def main():
    if not GITHUB_TOKEN:
        print("ERROR: GITHUB_TOKEN not set")
        sys.exit(1)
    if not GITHUB_REPO:
        print("ERROR: GITHUB_REPO not set")
        sys.exit(1)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*60}")
    print(f"  GRAVESTONE DATA COLLECTOR")
    print(f"  {now}")
    print(f"  Repo: {GITHUB_REPO}")
    print(f"{'='*60}\n")

    # Get all symbols
    print("[1/3] Fetching symbols...")
    symbols = get_symbols()
    print(f"      Found {len(symbols)} perpetual pairs")

    # Push a manifest file
    manifest = {
        "last_updated": now,
        "symbols": len(symbols),
        "timeframes": list(TIMEFRAMES.keys()),
        "symbol_list": symbols,
    }
    push_file("manifest.json", json.dumps(manifest, indent=2), f"Update manifest {now}")
    print("      Pushed manifest.json")

    # Collect data for each symbol
    print(f"\n[2/3] Collecting kline data...")
    total = len(symbols)
    errors = 0
    skipped = 0

    for idx, sym in enumerate(symbols):
        print(f"\n  [{idx+1}/{total}] {sym}")

        for tf, config in TIMEFRAMES.items():
            filepath = f"data/{tf}/{sym}.csv"
            label = config["label"]
            candles = config["candles"]

            print(f"    {label:5s} ", end="", flush=True)

            try:
                raw = get_klines_extended(sym, tf, candles)
                if not raw or len(raw) < 10:
                    print(f"skip ({len(raw) if raw else 0} candles)")
                    skipped += 1
                    continue

                csv_content = klines_to_csv(raw)
                success = push_file(filepath, csv_content, f"{sym} {tf} data {now}")

                if success:
                    print(f"ok ({len(raw)} candles, pushed)")
                else:
                    print(f"PUSH FAILED ({len(raw)} candles fetched)")
                    errors += 1

                time.sleep(0.5)  # rate limit GitHub API

            except Exception as e:
                print(f"ERROR: {e}")
                errors += 1

            time.sleep(0.2)  # rate limit Binance

    # Summary
    print(f"\n{'='*60}")
    print(f"  COLLECTION COMPLETE")
    print(f"  Symbols: {total}")
    print(f"  Files pushed: {total * len(TIMEFRAMES) - errors - skipped}")
    print(f"  Errors: {errors}")
    print(f"  Skipped: {skipped}")
    print(f"{'='*60}\n")

    # Push completion marker
    summary = {
        "completed": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "symbols": total,
        "errors": errors,
        "skipped": skipped,
        "files": total * len(TIMEFRAMES) - errors - skipped,
    }
    push_file("collection_status.json", json.dumps(summary, indent=2), f"Collection complete {now}")

    return summary


if __name__ == "__main__":
    main()
