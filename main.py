import os
import re
import time
import sqlite3
import logging
from datetime import datetime, timezone
from urllib.parse import quote
import threading
from flask import Flask

import requests

# ============================================================
# MESI MULTI-CHAIN SMART SCANNER (CLOUD 24/7 VERSION)
# ============================================================

BOT_TOKEN = "8634751397:AAFz390KYsyvr1GQ5a5f3FNmyeO-1ToQYGg"
CHAT_ID = "772777101"

DB_PATH = os.getenv("MESI_DB_PATH", "mesi_scanner.db")
BASE_URL = "https://api.dexscreener.com"

SCAN_INTERVAL = 180
MIN_SCORE = 65

MAX_CANDIDATES_PER_CHAIN = 20
MAX_TOTAL_CANDIDATES = 120
MAX_ALERTS_PER_SCAN = 5
MAX_PAIRS_PER_TOKEN = 8
REQUEST_DELAY = 0.20

MIN_LIQUIDITY_USD = 10_000
MAX_LIQUIDITY_USD = 5_000_000
MIN_POOL_AGE_MINUTES = 10
MAX_POOL_AGE_HOURS = 168

MIN_TXNS_1H = 3
MIN_TXNS_6H = 8

ALERT_COOLDOWN_HOURS = 12
ALERT_SCORE_IMPROVEMENT = 10

DISCOVERY_ENDPOINTS = [
    "/token-profiles/latest/v1",
    "/token-boosts/latest/v1",
    "/token-boosts/top/v1",
    "/community-takeovers/latest/v1",
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "MesiMultiChainScanner/3.0",
    "Accept": "application/json",
})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------------------- Flask Web Server (برای زنده ماندن روی سرور) ----------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Mesi Crypto Scanner is running 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)


# ---------------------- Utilities ----------------------------

def as_float(value, default=None):
    if value is None or value == "":
        return default
    try:
        result = float(value)
        return result if result == result else default
    except (TypeError, ValueError, OverflowError):
        return default

def as_int(value, default=0):
    value = as_float(value, None)
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, OverflowError):
        return default

def db_connect():
    db = sqlite3.connect(DB_PATH, timeout=20)
    db.execute("PRAGMA busy_timeout=20000")
    return db

def init_db():
    with db_connect() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS alerts_v3 (
                chain_id TEXT NOT NULL,
                address TEXT NOT NULL,
                last_alert REAL NOT NULL,
                last_score INTEGER NOT NULL,
                PRIMARY KEY (chain_id, address)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS signal_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                chain_id TEXT NOT NULL,
                address TEXT NOT NULL,
                pair_address TEXT,
                symbol TEXT,
                price REAL,
                liquidity REAL,
                volume_1h REAL,
                score INTEGER,
                message_sent INTEGER NOT NULL DEFAULT 0
            )
        """)

def get_json(url):
    for attempt in range(3):
        try:
            response = SESSION.get(url, timeout=15)
            if response.status_code == 429:
                retry_after = as_float(response.headers.get("Retry-After"), 2)
                time.sleep(min(max(retry_after, 1), 20))
                continue
            if response.status_code in (500, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1))
                continue
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            logging.warning("Request failed (attempt %s/3): %s", attempt + 1, exc)
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
    return None

def valid_chain_id(chain_id):
    return bool(isinstance(chain_id, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,50}", chain_id))

def discover_tokens():
    candidates = {}
    for endpoint in DISCOVERY_ENDPOINTS:
        data = get_json(BASE_URL + endpoint)
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            chain_id = item.get("chainId")
            address = item.get("tokenAddress")
            if not valid_chain_id(chain_id) or not isinstance(address, str) or not address.strip():
                continue
            key = (chain_id, address)
            if key not in candidates:
                candidates[key] = {"chain_id": chain_id, "address": address}
        time.sleep(0.15)
    
    by_chain = {}
    for item in candidates.values():
        by_chain.setdefault(item["chain_id"], []).append(item)

    selected = []
    for chain_id in sorted(by_chain):
        selected.extend(by_chain[chain_id][:MAX_CANDIDATES_PER_CHAIN])
    return selected[:MAX_TOTAL_CANDIDATES]

def address_matches(chain_id, a, b):
    if not isinstance(a, str) or not isinstance(b, str):
        return False
    evm_chains = {"ethereum", "bsc", "base", "arbitrum", "optimism", "polygon", "avalanche", "fantom"}
    if chain_id.lower() in evm_chains:
        return a.lower() == b.lower()
    return a == b

def get_token_pairs(chain_id, address):
    if not valid_chain_id(chain_id):
        return []
    url = f"{BASE_URL}/token-pairs/v1/{chain_id}/{quote(address, safe='')}"
    data = get_json(url)
    if not isinstance(data, list):
        return []
    pairs = []
    for pair in data:
        if not isinstance(pair, dict) or pair.get("chainId") != chain_id:
            continue
        base = pair.get("baseToken") or {}
        if address_matches(chain_id, base.get("address", ""), address):
            pairs.append(pair)
    pairs.sort(key=lambda p: as_float((p.get("liquidity") or {}).get("usd"), 0), reverse=True)
    return pairs[:MAX_PAIRS_PER_TOKEN]

def analyze_pair(pair):
    base = pair.get("baseToken") or {}
    address = base.get("address", "")
    chain_id = pair.get("chainId", "")
    price = as_float(pair.get("priceUsd"))
    liquidity = as_float((pair.get("liquidity") or {}).get("usd"), 0)

    if not address or price is None or price <= 0:
        return None
    if not MIN_LIQUIDITY_USD <= liquidity <= MAX_LIQUIDITY_USD:
        return None

    created = as_float(pair.get("pairCreatedAt"))
    if created is None or created <= 0:
        return None

    now = datetime.now(timezone.utc).timestamp()
    age_minutes = (now - created / 1000) / 60
    if not (MIN_POOL_AGE_MINUTES <= age_minutes <= MAX_POOL_AGE_HOURS * 60):
        return None

    volume = pair.get("volume") or {}
    txns = pair.get("txns") or {}
    changes = pair.get("priceChange") or {}

    v1 = as_float(volume.get("h1"), 0)
    v6 = as_float(volume.get("h6"), 0)
    v24 = as_float(volume.get("h24"), 0)
    buys1 = as_int((txns.get("h1") or {}).get("buys"))
    sells1 = as_int((txns.get("h1") or {}).get("sells"))
    
    if buys1 + sells1 < MIN_TXNS_1H:
        return None

    score = 50 # پایه امتیاز
    if liquidity >= 50_000: score += 20
    if v1 >= 10_000: score += 15
    if buys1 > sells1: score += 15

    return {
        "chain_id": chain_id,
        "address": address,
        "pair_address": pair.get("pairAddress", ""),
        "name": str(base.get("name", "Unknown"))[:60],
        "symbol": str(base.get("symbol", "UNKNOWN"))[:32],
        "price": price,
        "liquidity": liquidity,
        "volume1": v1,
        "volume6": v6,
        "volume24": v24,
        "buys1": buys1,
        "sells1": sells1,
        "score": min(score, 100),
        "reasons": ["حجم و نقدینگی مناسب در اسکن بازار"],
        "url": pair.get("url", ""),
        "fdv": as_float(pair.get("fdv")),
        "market_cap": as_float(pair.get("marketCap")),
        "dex_id": pair.get("dexId", "unknown"),
    }

def analyze_token(candidate):
    pairs = get_token_pairs(candidate["chain_id"], candidate["address"])
    results = [analyze_pair(p) for p in pairs if analyze_pair(p)]
    if not results:
        return None
    results.sort(key=lambda x: (x["score"], x["liquidity"]), reverse=True)
    return results[0]

def should_alert(chain_id, address, score):
    with db_connect() as db:
        row = db.execute("SELECT last_alert, last_score FROM alerts_v3 WHERE chain_id=? AND address=?", (chain_id, address)).fetchone()
    if row is None:
        return True
    last_alert, last_score = row
    return (time.time() - last_alert >= ALERT_COOLDOWN_HOURS * 3600) or (score >= last_score + ALERT_SCORE_IMPROVEMENT)

def record_successful_alert(item):
    with db_connect() as db:
        db.execute("""
            INSERT INTO alerts_v3 (chain_id, address, last_alert, last_score)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chain_id, address) DO UPDATE SET last_alert=excluded.last_alert, last_score=excluded.last_score
        """, (item["chain_id"], item["address"], time.time(), item["score"]))

def record_signal(item, sent):
    with db_connect() as db:
        db.execute("""
            INSERT INTO signal_history (timestamp, chain_id, address, pair_address, symbol, price, liquidity, volume_1h, score, message_sent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (time.time(), item["chain_id"], item["address"], item["pair_address"], item["symbol"], item["price"], item["liquidity"], item["volume1"], item["score"], int(sent)))

def send_telegram(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        response = SESSION.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown", "disable_web_page_preview": True}, timeout=15)
        return response.json().get("ok", False)
    except Exception as e:
        logging.error("Telegram send failed: %s", e)
        return False

def format_signal(item):
    return (
        f"🔎 *MESI SMART WATCHLIST*\n\n"
        f"Token: {item['name']} (`{item['symbol']}`)\n"
        f"Network: {item['chain_id']}\n"
        f"Score: {item['score']}/100\n"
        f"Price: ${item['price']:.10g}\n"
        f"Liquidity: ${item['liquidity']:,.0f}\n"
        f"Volume 1h: ${item['volume1']:,.0f}\n"
        f"Buys/Sells: {item['buys1']} / {item['sells1']}\n\n"
        f"Contract Address (یک لمس برای کپی):\n`{item['address']}`\n\n"
        f"Market: {item['url'] or 'N/A'}"
    )

def scan_loop():
    """حلقه اصلی اسکن که در پس‌زمینه اجرا می‌شود"""
    init_db()
    logging.info("Mesi Background Scanner thread started.")
    while True:
        try:
            logging.info("Starting scan cycle...")
            candidates = discover_tokens()
            results = []
            for candidate in candidates:
                item = analyze_token(candidate)
                if item and item["score"] >= MIN_SCORE:
                    results.append(item)
                time.sleep(REQUEST_DELAY)

            sent_count = 0
            for item in results:
                if sent_count >= MAX_ALERTS_PER_SCAN:
                    break
                if should_alert(item["chain_id"], item["address"], item["score"]):
                    sent = send_telegram(format_signal(item))
                    record_signal(item, sent)
                    if sent:
                        record_successful_alert(item)
                        sent_count += 1
            logging.info("Scan cycle finished. Waiting for next interval...")
        except Exception as e:
            logging.exception("Error in scan loop: %s", e)
        
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    # اجرای حلقه اسکن در پس‌زمینه (Background Thread)
    t = threading.Thread(target=scan_loop, daemon=True)
    t.start()
    
    # اجرای وب‌سرور برای زنده ماندن روی سرور ابری
    run_flask()
