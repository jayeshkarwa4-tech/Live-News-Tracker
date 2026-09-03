#!/usr/bin/env python3
"""
Global Monitor — Finance / Geopolitics / Supply Chain, in one Python app.

This is a real Python application (Streamlit), not an HTML file. It pulls
live data at runtime from:
  - Yahoo Finance (via the `yfinance` package)      -> market prices
  - Public RSS feeds (via `feedparser`)              -> news, categorized
  - YouTube's public /live channel page (via `requests`) -> current live video ID
    for each broadcaster, then handed to Streamlit's native st.video player
  - Optionally, NewsAPI.org or GNews.io (via `requests`) -> a second, often
    faster-updating news layer, merged and deduped with the RSS feed

No API keys are required to run this — RSS + Yahoo Finance + YouTube cover
everything with zero config. Entering a free NewsAPI.org or GNews.io key in
the sidebar layers that provider's articles on top of RSS (never replacing
it), since RSS keeps working even if the API key is missing, invalid, or
rate-limited. If you later get a market-data key (e.g. Alpha Vantage,
Finnhub) instead of Yahoo Finance, that would go in fetch_quotes().

------------------------------------------------------------------------------
SETUP

    pip install streamlit yfinance feedparser plotly pandas requests
    # optional, gives smooth auto-refresh instead of a manual button:
    pip install streamlit-autorefresh

RUN

    streamlit run global_monitor_app.py

Then open the local URL it prints (usually http://localhost:8501).
------------------------------------------------------------------------------
"""

import re
import time
import calendar
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
import streamlit as st
import plotly.graph_objects as go

try:
    import feedparser
except ImportError:
    st.error("Missing dependency: run `pip install feedparser`")
    st.stop()

try:
    import yfinance as yf
except ImportError:
    st.error("Missing dependency: run `pip install yfinance`")
    st.stop()

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False


# ============================================================================
# CONFIG / STATIC DATA
# ============================================================================

st.set_page_config(
    page_title="Global Monitor",
    page_icon="\U0001F310",
    layout="wide",
    initial_sidebar_state="expanded",
)

REFRESH_SECONDS = 45         # RSS refresh cadence — short, so the feed feels live
API_REFRESH_SECONDS = 180    # news-API refresh cadence — longer, to respect free-tier quotas
QUOTE_REFRESH_SECONDS = 60   # market quotes refresh a bit slower than news
TV_REFRESH_SECONDS = 300     # how often we re-check which video is "live"
ENTRIES_PER_FEED = 15        # how many items to pull from each RSS source per cycle

# Search queries used against NewsAPI.org / GNews.io, one per dashboard category.
# RSS feeds are already pre-categorized by source; API providers are query-based
# instead, so this is how the same three buckets get populated from them.
CATEGORY_QUERIES = {
    "Finance": "markets OR stocks OR economy OR inflation OR earnings OR \"federal reserve\"",
    "Geopolitics": "geopolitics OR conflict OR sanctions OR diplomacy OR war",
    "Supply Chain": "\"supply chain\" OR shipping OR logistics OR freight OR \"port congestion\"",
}

NEWS_FEEDS = [
    # ---- Finance / Business ----
    {"name": "CNBC Markets",      "cat": "Finance", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html"},
    {"name": "CNBC Business",     "cat": "Finance", "url": "https://www.cnbc.com/id/10001147/device/rss/rss.html"},
    {"name": "NYT Business",      "cat": "Finance", "url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml"},
    {"name": "MarketWatch",       "cat": "Finance", "url": "https://feeds.content.dowjones.io/public/rss/mw_topstories"},
    {"name": "Investing.com",     "cat": "Finance", "url": "https://www.investing.com/rss/news.rss"},
    {"name": "Yahoo Finance",     "cat": "Finance", "url": "https://finance.yahoo.com/news/rssindex"},
    {"name": "Business Insider",  "cat": "Finance", "url": "https://www.businessinsider.com/rss"},
    {"name": "Forbes Business",   "cat": "Finance", "url": "https://www.forbes.com/business/feed/"},
    {"name": "Financial Times",   "cat": "Finance", "url": "https://www.ft.com/rss/home"},
    # ---- Geopolitics ----
    {"name": "BBC World",         "cat": "Geopolitics", "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "Al Jazeera",        "cat": "Geopolitics", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    {"name": "Guardian World",    "cat": "Geopolitics", "url": "https://www.theguardian.com/world/rss"},
    {"name": "NYT World",         "cat": "Geopolitics", "url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"},
    {"name": "Foreign Policy",    "cat": "Geopolitics", "url": "https://foreignpolicy.com/feed/"},
    {"name": "Politico",          "cat": "Geopolitics", "url": "https://www.politico.com/rss/politicopicks.xml"},
    {"name": "SCMP",              "cat": "Geopolitics", "url": "https://www.scmp.com/rss/91/feed"},
    # ---- Supply Chain / Logistics ----
    {"name": "gCaptain",          "cat": "Supply Chain", "url": "https://gcaptain.com/feed/"},
    {"name": "FreightWaves",      "cat": "Supply Chain", "url": "https://www.freightwaves.com/news/feed"},
    {"name": "The Loadstar",      "cat": "Supply Chain", "url": "https://theloadstar.com/feed/"},
    {"name": "Supply Chain Dive", "cat": "Supply Chain", "url": "https://www.supplychaindive.com/feeds/news/"},
    {"name": "Supply Chain Brain","cat": "Supply Chain", "url": "https://www.supplychainbrain.com/rss/articles"},
]

MARKET_TICKERS = [
    {"sym": "^GSPC",    "name": "S&P 500"},
    {"sym": "^DJI",     "name": "Dow Jones"},
    {"sym": "^IXIC",    "name": "Nasdaq"},
    {"sym": "^FTSE",    "name": "FTSE 100"},
    {"sym": "^N225",    "name": "Nikkei 225"},
    {"sym": "^HSI",     "name": "Hang Seng"},
    {"sym": "CL=F",     "name": "Crude Oil WTI"},
    {"sym": "GC=F",     "name": "Gold"},
    {"sym": "BTC-USD",  "name": "Bitcoin"},
    {"sym": "EURUSD=X", "name": "EUR/USD"},
]

TV_CHANNELS = [
    {"name": "Bloomberg Originals", "channel_id": "UCIALMKvObZNtJ6AmdCLP7Lg"},
    {"name": "Sky News",            "channel_id": "UCoMdktPbSTixAyNGwb-UYkQ"},
    {"name": "DW News",             "channel_id": "UCknLrEdhRCp1aegoMqRaCZg"},
    {"name": "France 24 English",   "channel_id": "UCQfwfsi5VrQ8yKZ-UWmAEFg"},
    {"name": "Al Jazeera English",  "channel_id": "UCNye-wNBqNL5ZzHSJj3l8Bg"},
    {"name": "NBC News NOW",        "channel_id": "UCeY0bbntWzzVIaj2z3QigXg"},
]

FINANCE_HUBS = [
    {"name": "New York (NYSE / NASDAQ)", "lat": 40.7069, "lon": -74.0113, "desc": "World's largest equity markets by market cap."},
    {"name": "London (LSE)",             "lat": 51.5155, "lon": -0.0922,  "desc": "Major European financial center, FX trading hub."},
    {"name": "Tokyo (TSE)",              "lat": 35.6813, "lon": 139.7671, "desc": "Largest exchange in Asia by market cap."},
    {"name": "Shanghai (SSE)",           "lat": 31.2304, "lon": 121.4737, "desc": "Mainland China's primary equity exchange."},
    {"name": "Hong Kong (HKEX)",         "lat": 22.2793, "lon": 114.1628, "desc": "Gateway exchange for China-linked capital flows."},
    {"name": "Frankfurt (Deutsche B\u00f6rse)", "lat": 50.1155, "lon": 8.6842, "desc": "Germany's primary exchange, ECB proximity."},
    {"name": "Singapore (SGX)",          "lat": 1.2839,  "lon": 103.8512, "desc": "Southeast Asia's commodities & derivatives hub."},
    {"name": "Mumbai (BSE / NSE)",       "lat": 18.9256, "lon": 72.8242,  "desc": "India's primary equity markets."},
    {"name": "Dubai (DFM)",              "lat": 25.2532, "lon": 55.2895,  "desc": "Gulf financial hub, energy capital flows."},
    {"name": "Sydney (ASX)",             "lat": -33.8688,"lon": 151.2093, "desc": "Australia's primary exchange, commodities-linked."},
]

CHOKEPOINTS = [
    {"name": "Strait of Hormuz",   "lat": 26.50, "lon": 56.25,  "desc": "~20% of global oil consumption transits this strait."},
    {"name": "Suez Canal",         "lat": 30.50, "lon": 32.35,  "desc": "Key Asia-Europe shipping shortcut, ~12% of global trade."},
    {"name": "Panama Canal",       "lat": 9.08,  "lon": -79.68, "desc": "Pacific-Atlantic shortcut, sensitive to drought/water levels."},
    {"name": "Strait of Malacca",  "lat": 2.50,  "lon": 101.50, "desc": "Busiest strait by volume, links Indian & Pacific Oceans."},
    {"name": "Bosphorus Strait",   "lat": 41.00, "lon": 29.00,  "desc": "Black Sea grain & energy export corridor."},
    {"name": "Strait of Gibraltar","lat": 35.95, "lon": -5.60,  "desc": "Atlantic-Mediterranean gateway."},
    {"name": "Bab-el-Mandeb Strait","lat": 12.50,"lon": 43.30,  "desc": "Red Sea approach, links to Suez-bound traffic."},
    {"name": "Taiwan Strait",      "lat": 24.50, "lon": 119.50, "desc": "Critical for semiconductor supply chain logistics."},
]

HOTSPOTS = [
    {"name": "Eastern Europe",             "lat": 48.38, "lon": 31.17,  "desc": "Long-running conflict zone with global energy & grain market impact."},
    {"name": "Middle East (Levant)",       "lat": 31.50, "lon": 34.47,  "desc": "Persistent regional tension affecting shipping & energy routes."},
    {"name": "Iran / Gulf",                "lat": 32.43, "lon": 53.69,  "desc": "Watched for Hormuz-linked energy market impact."},
    {"name": "Taiwan / South China Sea",   "lat": 22.50, "lon": 118.50, "desc": "Watched for shipping-lane and semiconductor-supply risk."},
    {"name": "Korean Peninsula",           "lat": 38.00, "lon": 127.50, "desc": "Long-standing geopolitical flashpoint in East Asia."},
    {"name": "Sudan / Horn of Africa",     "lat": 12.86, "lon": 30.22,  "desc": "Conflict affecting regional stability and trade routes."},
    {"name": "Sahel Region",               "lat": 14.50, "lon": -2.50,  "desc": "Watched for instability across West/Central Africa."},
]


# ============================================================================
# DATA FETCHERS (cached, so repeated Streamlit reruns don't hammer sources)
# ============================================================================

def make_fp(title: str) -> str:
    """Fingerprint used to dedupe the same story picked up by multiple sources."""
    return hashlib.md5(re.sub(r"[^a-z0-9]", "", title.lower())[:80].encode()).hexdigest()


def _parse_iso_ts(iso_string):
    """Parse a NewsAPI/GNews-style 'YYYY-MM-DDTHH:MM:SSZ' timestamp to epoch seconds (UTC)."""
    if not iso_string:
        return time.time()
    try:
        dt = datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%SZ")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return time.time()


def dedupe_and_sort(rows):
    """Merge item lists from multiple sources: newest first, one copy per story."""
    rows = sorted(rows, key=lambda r: r["ts"], reverse=True)
    seen_fp, deduped = set(), []
    for r in rows:
        if r["fp"] in seen_fp:
            continue
        seen_fp.add(r["fp"])
        deduped.append(r)
    return deduped


def _fetch_one_feed(feed):
    """Fetch + parse a single RSS source. Runs inside a worker thread."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; GlobalMonitor/1.0)"}
        resp = requests.get(feed["url"], headers=headers, timeout=8)
        parsed = feedparser.parse(resp.content)
        if not parsed.entries:
            return feed["name"], "error", []

        items = []
        for idx, entry in enumerate(parsed.entries[:ENTRIES_PER_FEED]):
            published = entry.get("published_parsed") or entry.get("updated_parsed")
            if published:
                # published_parsed is a UTC struct_time — timegm (not mktime,
                # which assumes local time) is what keeps "X ago" accurate
                # regardless of the machine's timezone.
                ts = calendar.timegm(published)
            else:
                # Feed didn't supply a timestamp — assume feed order is newest-first
                # and space entries a minute apart so they still sort sensibly.
                ts = time.time() - idx * 60
            title = entry.get("title", "(untitled)").strip()
            link = entry.get("link", "")
            items.append({
                "source": feed["name"],
                "cat": feed["cat"],
                "title": title,
                "link": link,
                "ts": ts,
                "fp": make_fp(title),
            })
        return feed["name"], "ok", items
    except Exception:
        return feed["name"], "error", []


def _fetch_newsapi_org(api_key):
    """
    Query NewsAPI.org's /v2/everything once per category. Requires a free key
    from https://newsapi.org — note their free "Developer" tier is meant for
    local/non-production use and has a daily request cap, so this polls on a
    longer cadence (API_REFRESH_SECONDS) than the RSS feeds do.
    """
    rows = []
    for cat, query in CATEGORY_QUERIES.items():
        try:
            resp = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": query,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 20,
                    "apiKey": api_key,
                },
                timeout=8,
            )
            data = resp.json()
            if data.get("status") != "ok":
                continue
            for a in data.get("articles", []):
                title = (a.get("title") or "").strip()
                if not title or title.lower() == "[removed]":
                    continue
                rows.append({
                    "source": (a.get("source") or {}).get("name") or "NewsAPI.org",
                    "cat": cat,
                    "title": title,
                    "link": a.get("url") or "",
                    "ts": _parse_iso_ts(a.get("publishedAt")),
                    "fp": make_fp(title),
                })
        except Exception:
            continue
    return rows


def _fetch_gnews(api_key):
    """
    Query GNews.io's /v4/search once per category. Requires a free key from
    https://gnews.io — free tier has a daily request cap, so this also polls
    on the longer API_REFRESH_SECONDS cadence.
    """
    rows = []
    for cat, query in CATEGORY_QUERIES.items():
        try:
            resp = requests.get(
                "https://gnews.io/api/v4/search",
                params={
                    "q": query,
                    "lang": "en",
                    "max": 20,
                    "sortby": "publishedAt",
                    "token": api_key,
                },
                timeout=8,
            )
            data = resp.json()
            for a in data.get("articles", []):
                title = (a.get("title") or "").strip()
                if not title:
                    continue
                rows.append({
                    "source": (a.get("source") or {}).get("name") or "GNews",
                    "cat": cat,
                    "title": title,
                    "link": a.get("url") or "",
                    "ts": _parse_iso_ts(a.get("publishedAt")),
                    "fp": make_fp(title),
                })
        except Exception:
            continue
    return rows


@st.cache_data(ttl=API_REFRESH_SECONDS, show_spinner=False)
def fetch_newsapi_org(api_key):
    return _fetch_newsapi_org(api_key)


@st.cache_data(ttl=API_REFRESH_SECONDS, show_spinner=False)
def fetch_gnews(api_key):
    return _fetch_gnews(api_key)


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def fetch_rss_news():
    """Poll every RSS source concurrently, dedupe, and return one sorted list."""
    rows = []
    status = {}
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(_fetch_one_feed, feed) for feed in NEWS_FEEDS]
        for future in as_completed(futures):
            name, state, items = future.result()
            status[name] = state
            rows.extend(items)

    return dedupe_and_sort(rows), status


@st.cache_data(ttl=QUOTE_REFRESH_SECONDS, show_spinner=False)
def fetch_quotes():
    """Pull live-ish quotes for every configured ticker via yfinance."""
    out = []
    for t in MARKET_TICKERS:
        row = {"name": t["name"], "sym": t["sym"], "ok": False}
        try:
            info = yf.Ticker(t["sym"]).fast_info
            price = info.get("last_price") or info.get("lastPrice")
            prev_close = info.get("previous_close") or info.get("previousClose")
            if price is not None and prev_close:
                chg = price - prev_close
                pct = (chg / prev_close) * 100
                row.update({"price": price, "chg": chg, "pct": pct, "ok": True})
        except Exception:
            pass
        out.append(row)
    return out


@st.cache_data(ttl=TV_REFRESH_SECONDS, show_spinner=False)
def get_live_video_id(channel_id: str):
    """Scrape a channel's /live page for the currently-live videoId (best effort)."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = requests.get(f"https://www.youtube.com/channel/{channel_id}/live",
                             headers=headers, timeout=6)
        match = re.search(r'"videoId":"(.*?)"', resp.text)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None


def build_map_dataframe(show_finance, show_choke, show_hotspot):
    rows = []
    if show_finance:
        for h in FINANCE_HUBS:
            rows.append({**h, "type": "Finance Hub", "color": "#6fe7c9"})
    if show_choke:
        for c in CHOKEPOINTS:
            rows.append({**c, "type": "Supply Chokepoint", "color": "#6fa8ff"})
    if show_hotspot:
        for h in HOTSPOTS:
            rows.append({**h, "type": "Geopolitical Flashpoint", "color": "#ff6b6b"})
    return pd.DataFrame(rows)


def time_ago(ts):
    if not ts:
        return "?"
    diff = time.time() - ts
    if diff < 60:
        return f"{int(diff)}s ago"
    if diff < 3600:
        return f"{int(diff // 60)}m ago"
    if diff < 86400:
        return f"{int(diff // 3600)}h ago"
    return f"{int(diff // 86400)}d ago"


# ============================================================================
# SIDEBAR
# ============================================================================

st.sidebar.title("\U0001F310 Global Monitor")
st.sidebar.caption("Finance \u00b7 Geopolitics \u00b7 Supply Chain")

st.sidebar.subheader("Map layers")
show_finance = st.sidebar.checkbox("Finance hubs", value=True)
show_choke = st.sidebar.checkbox("Supply chokepoints", value=True)
show_hotspot = st.sidebar.checkbox("Geopolitical flashpoints", value=True)

st.sidebar.subheader("News filter")
cat_choice = st.sidebar.radio(
    "Category", ["All", "Finance", "Geopolitics", "Supply Chain"], index=0
)

st.sidebar.subheader("Live news API (optional)")
api_provider = st.sidebar.selectbox(
    "Provider",
    ["RSS only (no key needed)", "RSS + NewsAPI.org", "RSS + GNews.io"],
    index=0,
    help="RSS alone needs nothing and never breaks. Adding a free API key "
    "layers in a second, typically faster-updating source on top of it — "
    "RSS keeps running underneath either way, so this never goes to zero results.",
)
api_key = ""
if api_provider != "RSS only (no key needed)":
    api_key = st.sidebar.text_input(
        "API key",
        type="password",
        help="Free keys: newsapi.org/register or gnews.io/register. "
        "Nothing is sent anywhere except directly to that provider's API.",
    )
    if not api_key:
        st.sidebar.caption("No key entered yet — showing RSS only for now.")
    st.sidebar.caption(f"API layer refreshes every {API_REFRESH_SECONDS}s (free-tier request caps are the reason this is slower than RSS).")

st.sidebar.subheader("Refresh")
if HAS_AUTOREFRESH:
    st_autorefresh(interval=REFRESH_SECONDS * 1000, key="auto_refresh")
    st.sidebar.caption(f"Auto-refreshing every {REFRESH_SECONDS}s \u2014 new headlines get a \U0001F7E2 NEW tag and a toast.")
else:
    st.sidebar.caption("Auto-refresh needs `pip install streamlit-autorefresh` (see requirements.txt).")
    if st.sidebar.button("\u21bb Refresh now"):
        fetch_news.clear()
        fetch_quotes.clear()
        get_live_video_id.clear()

st.sidebar.markdown("---")
st.sidebar.caption(
    f"News is polled from {len(NEWS_FEEDS)} RSS sources across Finance, Geopolitics "
    "and Supply Chain, optionally merged with a live API layer above, "
    "deduplicated across all of them, and sorted by publish time. "
    "Map markers are static reference points, not a computed risk score. "
    "Market quotes come from Yahoo Finance via `yfinance` and may run on a "
    "short delay. Use for situational awareness, not trading decisions."
)


# ============================================================================
# HEADER + MARKET STRIP
# ============================================================================

col_title, col_clock = st.columns([3, 1])
with col_title:
    st.markdown("## \U0001F310 GLOBAL MONITOR")
    st.caption("Live finance, geopolitics & supply-chain situational dashboard")
with col_clock:
    st.markdown(
        f"<div style='text-align:right; font-family:monospace; padding-top:18px;'>"
        f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}<br>"
        f"<span style='color:gray;font-size:12px;'>{datetime.now(timezone.utc).strftime('%a %d %b %Y')}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )

quotes = fetch_quotes()
qcols = st.columns(len(quotes))
for col, q in zip(qcols, quotes):
    with col:
        if q["ok"]:
            st.metric(q["name"], f"{q['price']:,.2f}", f"{q['chg']:+.2f} ({q['pct']:+.2f}%)")
        else:
            st.metric(q["name"], "\u2014", "no data this cycle")

st.markdown("---")


# ============================================================================
# MAP
# ============================================================================

st.subheader("Situation Map")
df = build_map_dataframe(show_finance, show_choke, show_hotspot)

if df.empty:
    st.info("No layers selected — turn one on in the sidebar.")
else:
    fig = go.Figure()
    for marker_type, group in df.groupby("type"):
        fig.add_trace(go.Scattergeo(
            lon=group["lon"], lat=group["lat"],
            text=group.apply(lambda r: f"<b>{r['name']}</b><br>{r['desc']}", axis=1),
            hoverinfo="text",
            mode="markers",
            marker=dict(size=9, color=group["color"].iloc[0], line=dict(width=1, color="#0b0f14")),
            name=marker_type,
        ))
    fig.update_geos(
        projection_type="natural earth",
        showland=True, landcolor="#12161d",
        showocean=True, oceancolor="#05070a",
        showcountries=True, countrycolor="#1b2530",
        showcoastlines=True, coastlinecolor="#1b2530",
        bgcolor="#05070a",
    )
    fig.update_layout(
        paper_bgcolor="#05070a",
        plot_bgcolor="#05070a",
        font_color="#c9d1d9",
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=-0.05, x=0.5, xanchor="center"),
        height=480,
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# NEWS + LIVE TV TABS
# ============================================================================

tab_news, tab_tv = st.tabs(["\U0001F4F0 Live News", "\U0001F4FA Live TV"])

with tab_news:
    rss_items, feed_status = fetch_rss_news()

    api_items = []
    api_error = None
    if api_provider == "RSS + NewsAPI.org" and api_key:
        try:
            api_items = fetch_newsapi_org(api_key)
            if not api_items:
                api_error = "NewsAPI.org returned no articles this cycle (check the key, or it may be rate-limited)."
        except Exception as e:
            api_error = f"NewsAPI.org fetch failed: {e}"
    elif api_provider == "RSS + GNews.io" and api_key:
        try:
            api_items = fetch_gnews(api_key)
            if not api_items:
                api_error = "GNews.io returned no articles this cycle (check the key, or it may be rate-limited)."
        except Exception as e:
            api_error = f"GNews.io fetch failed: {e}"

    news_items = dedupe_and_sort(rss_items + api_items) if api_items else rss_items

    if api_error:
        st.warning(f"{api_error} Showing RSS results in the meantime.")
    elif api_items:
        st.caption(f"\u2705 {len(api_items)} articles from the live API layer merged in with RSS this cycle.")

    # ---- track which headlines are new since the last poll (this session) ----
    if "seen_fps" not in st.session_state:
        st.session_state.seen_fps = set()
        st.session_state.is_first_load = True
    else:
        st.session_state.is_first_load = False

    current_fps = {i["fp"] for i in news_items}
    new_fps = set() if st.session_state.is_first_load else (current_fps - st.session_state.seen_fps)
    if new_fps:
        st.toast(f"{len(new_fps)} new headline{'s' if len(new_fps) != 1 else ''} just landed", icon="\U0001F7E2")
    st.session_state.seen_fps |= current_fps

    bad_feeds = [n for n, s in feed_status.items() if s == "error"]
    ok_count = sum(1 for s in feed_status.values() if s == "ok")
    source_note = f" \u00b7 + {api_provider.split('+ ')[1]}" if api_items else ""
    st.caption(
        f"Polling {len(NEWS_FEEDS)} RSS sources every {REFRESH_SECONDS}s{source_note} \u00b7 "
        f"{ok_count}/{len(NEWS_FEEDS)} RSS feeds responded this cycle"
        + (f" \u00b7 quiet: {', '.join(bad_feeds)}" if bad_feeds else "")
    )

    filtered = news_items if cat_choice == "All" else [n for n in news_items if n["cat"] == cat_choice]

    if not filtered:
        st.info("No headlines yet — waiting on the wire.")
    else:
        for item in filtered[:60]:
            badge_color = {"Finance": "green", "Geopolitics": "red", "Supply Chain": "blue"}.get(item["cat"], "gray")
            new_tag = " \U0001F7E2 **NEW**" if item["fp"] in new_fps else ""
            st.markdown(
                f":{badge_color}[**{item['cat']}**]  ·  *{item['source']}*  ·  "
                f"<span style='color:gray;font-size:12px'>{time_ago(item['ts'])}</span>{new_tag}",
                unsafe_allow_html=True,
            )
            st.markdown(f"**[{item['title']}]({item['link']})**")
            st.markdown("")

with tab_tv:
    st.caption("Official 24/7 live streams, resolved from each broadcaster's channel page.")
    names = [c["name"] for c in TV_CHANNELS]
    picked = st.selectbox("Channel", names, index=0)
    channel = next(c for c in TV_CHANNELS if c["name"] == picked)

    with st.spinner(f"Finding {channel['name']}'s current live stream..."):
        video_id = get_live_video_id(channel["channel_id"])

    if video_id:
        st.video(f"https://www.youtube.com/watch?v={video_id}")
    else:
        st.warning(
            f"Couldn't resolve a live stream for {channel['name']} right now — "
            "it may be between broadcasts. Try another channel or refresh in a bit."
        )

st.markdown("---")
st.caption(
    "Sources: Yahoo Finance (yfinance) \u00b7 public RSS wires \u00b7 YouTube live"
    + (" \u00b7 " + api_provider.split("+ ")[1] if api_items else "")
    + f" \u00b7 last app refresh: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
)
