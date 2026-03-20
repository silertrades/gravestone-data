#!/usr/bin/env python3
"""
GRAVESTONE — Funding Rate & Open Interest History Collector
Fetches max history for funding rates (~2.7 years) and OI at 4H + 1D.
Pushes CSV files to the gravestone-data GitHub repo.

ENV VARS:
  GITHUB_TOKEN  — personal access token with repo scope
  GITHUB_REPO   — owner/repo
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
    return [s["symbol"] for s in data["symbols"]
            if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
            and s["contractType"] == "PERPETUAL" and s["symbol"] not in SKIP_SYMBOLS]

# ══════════════════════════════════════════════════════════════
#  FUNDING RATE HISTORY (~2.7 years at 3000 records)
# ══════════════════════════════════════════════════════════════

def get_funding_history(symbol, total=3000):
    """Fetch historical funding rates. Every 8h, paginate backwards."""
    all_rates = []
    end_time = None
    remaining = total
    while remaining > 0:
        limit = min(remaining, 1000)
        url = f"{BIN_BASE}/fapi/v1/fundingRate?symbol={symbol}&limit={limit}"
        if end_time:
            url += f"&endTime={end_time}"
        data = fetch_json(url)
        if not data:
            break
        all_rates = data + all_rates
        end_time = data[0]["fundingTime"] - 1
        remaining -= len(data)
        if len(data) < limit:
            break
        time.sleep(0.15)
    return all_rates

def funding_to_csv(rates):
    lines = ["timestamp,symbol,funding_rate"]
    for r in rates:
        lines.append(f"{r['fundingTime']},{r['symbol']},{r['fundingRate']}")
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════
#  OPEN INTEREST HISTORY (paginate for max depth)
# ══════════════════════════════════════════════════════════════

def get_oi_history(symbol, period="4h", max_pages=20):
    """Fetch OI history, paginate backwards for max depth."""
    all_data = []
    end_time = None
    for page in range(max_pages):
        url = f"{BIN_BASE}/futures/data/openInterestHist?symbol={symbol}&period={period}&limit=500"
        if end_time:
            url += f"&endTime={end_time}"
        try:
            data = fetch_json(url)
        except:
            break
        if not data:
            break
        all_data = data + all_data
        end_time = int(data[0]["timestamp"]) - 1
        if len(data) < 500:
            break
        time.sleep(0.2)
    return all_data

def oi_to_csv(data):
    lines = ["timestamp,symbol,sum_open_interest,sum_open_interest_value"]
    for r in data:
        lines.append(f"{r['timestamp']},{r['symbol']},{r['sumOpenInterest']},{r['sumOpenInterestValue']}")
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════
#  GITHUB
# ══════════════════════════════════════════════════════════════

def github_request(method, path, data=None):
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
            return None
        if e.code == 422 and "sha" in error_body:
            return {"error": "sha_conflict"}
        print(f"      GitHub error {e.code}: {error_body[:200]}")
        return {"error": str(e.code)}

def get_file_sha(filepath):
    result = github_request("GET", f"contents/{filepath}")
    if result and "sha" in result:
        return result["sha"]
    return None

def push_file(filepath, content, message):
    encoded = base64.b64encode(content.encode()).decode()
    sha = get_file_sha(filepath)
    data = {"message": message, "content": encoded}
    if sha:
        data["sha"] = sha
    result = github_request("PUT", f"contents/{filepath}", data)
    if result and "content" in result:
        return True
    if result and result.get("error") == "sha_conflict":
        sha = get_file_sha(filepath)
        if sha:
            data["sha"] = sha
            result = github_request("PUT", f"contents/{filepath}", data)
            return result and "content" in result
    return False

# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    if not GITHUB_TOKEN:
        print("ERROR: GITHUB_TOKEN not set"); sys.exit(1)
    if not GITHUB_REPO:
        print("ERROR: GITHUB_REPO not set"); sys.exit(1)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*60}")
    print(f"  GRAVESTONE — FUNDING RATE & OI COLLECTOR")
    print(f"  Funding: 3000 records (~2.7 years)")
    print(f"  OI: 4H + 1D periods (max available history)")
    print(f"  {now}")
    print(f"  Repo: {GITHUB_REPO}")
    print(f"{'='*60}\n")

    print("[1/2] Fetching symbols...")
    symbols = get_symbols()
    print(f"      Found {len(symbols)} perpetual pairs\n")

    total = len(symbols)
    stats = {"fr_ok": 0, "fr_err": 0, "fr_skip": 0,
             "oi4h_ok": 0, "oi4h_err": 0, "oi4h_skip": 0,
             "oi1d_ok": 0, "oi1d_err": 0, "oi1d_skip": 0}

    print("[2/2] Collecting data...")

    for idx, sym in enumerate(symbols):
        print(f"\n  [{idx+1}/{total}] {sym}")

        # ── Funding Rates ──
        print(f"    FR     ", end="", flush=True)
        try:
            rates = get_funding_history(sym, total=3000)
            if rates and len(rates) > 5:
                csv = funding_to_csv(rates)
                ok = push_file(f"data/funding/{sym}.csv", csv, f"{sym} funding {now}")
                if ok:
                    print(f"ok ({len(rates)} rates)")
                    stats["fr_ok"] += 1
                else:
                    print(f"PUSH FAILED ({len(rates)} fetched)")
                    stats["fr_err"] += 1
            else:
                print(f"skip ({len(rates) if rates else 0})")
                stats["fr_skip"] += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"ERROR: {e}")
            stats["fr_err"] += 1

        # ── OI 4H ──
        print(f"    OI-4H  ", end="", flush=True)
        try:
            oi = get_oi_history(sym, period="4h", max_pages=20)
            if oi and len(oi) > 5:
                csv = oi_to_csv(oi)
                ok = push_file(f"data/oi_4h/{sym}.csv", csv, f"{sym} OI-4H {now}")
                if ok:
                    # Calculate date range
                    first_ts = int(oi[0]["timestamp"])
                    last_ts = int(oi[-1]["timestamp"])
                    days = (last_ts - first_ts) / 86400000
                    print(f"ok ({len(oi)} periods, {days:.0f} days)")
                    stats["oi4h_ok"] += 1
                else:
                    print(f"PUSH FAILED ({len(oi)} fetched)")
                    stats["oi4h_err"] += 1
            else:
                print(f"skip ({len(oi) if oi else 0})")
                stats["oi4h_skip"] += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"ERROR: {e}")
            stats["oi4h_err"] += 1

        # ── OI 1D ──
        print(f"    OI-1D  ", end="", flush=True)
        try:
            oi = get_oi_history(sym, period="1d", max_pages=20)
            if oi and len(oi) > 5:
                csv = oi_to_csv(oi)
                ok = push_file(f"data/oi_1d/{sym}.csv", csv, f"{sym} OI-1D {now}")
                if ok:
                    first_ts = int(oi[0]["timestamp"])
                    last_ts = int(oi[-1]["timestamp"])
                    days = (last_ts - first_ts) / 86400000
                    print(f"ok ({len(oi)} periods, {days:.0f} days)")
                    stats["oi1d_ok"] += 1
                else:
                    print(f"PUSH FAILED ({len(oi)} fetched)")
                    stats["oi1d_err"] += 1
            else:
                print(f"skip ({len(oi) if oi else 0})")
                stats["oi1d_skip"] += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"ERROR: {e}")
            stats["oi1d_err"] += 1

        time.sleep(0.2)

    # Summary
    print(f"\n{'='*60}")
    print(f"  COLLECTION COMPLETE")
    print(f"  Funding rates:  {stats['fr_ok']} ok / {stats['fr_err']} err / {stats['fr_skip']} skip")
    print(f"  OI (4H):        {stats['oi4h_ok']} ok / {stats['oi4h_err']} err / {stats['oi4h_skip']} skip")
    print(f"  OI (1D):        {stats['oi1d_ok']} ok / {stats['oi1d_err']} err / {stats['oi1d_skip']} skip")
    print(f"{'='*60}\n")

    summary = {
        "completed": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "type": "funding_rate_and_oi",
        "symbols": total,
        **stats,
    }
    push_file("collection_status_fr_oi.json", json.dumps(summary, indent=2), f"FR+OI complete {now}")


if __name__ == "__main__":
    main()
