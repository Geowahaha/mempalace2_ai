"""
market/stock_universe.py - Global Stock Universe
Covers: US (S&P500), EU (FTSE/DAX/CAC), Asia (Nikkei/HangSeng/SET), and more.
All tickers use Yahoo Finance format.
"""
from datetime import datetime, time as dt_time, timezone
from zoneinfo import ZoneInfo

# ─────────────────────────────────────────────────────────────────────────────
#  UNITED STATES — S&P 500 Top 50 + NASDAQ Leaders + Sector ETFs
# ─────────────────────────────────────────────────────────────────────────────
US_MEGA_CAP = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B", "TSLA",
    "AVGO", "JPM", "LLY", "UNH", "V", "XOM", "MA", "JNJ", "PG",
    "HD", "COST", "MRK", "ABBV", "CVX", "NFLX", "KO", "PEP",
    "ADBE", "WMT", "BAC", "TMO", "CRM", "ACN", "MCD", "LIN",
    "CSCO", "ABT", "TXN", "AMD", "ORCL", "NEE", "DHR", "PM",
    "MS", "GS", "WFC", "RTX", "CAT", "NOW", "INTC", "IBM", "QCOM",
]

US_SECTOR_ETFS = [
    "SPY",   # S&P 500
    "QQQ",   # NASDAQ 100
    "IWM",   # Russell 2000 (Small Cap)
    "DIA",   # Dow Jones 30
    "XLF",   # Financials
    "XLK",   # Technology
    "XLE",   # Energy
    "XLV",   # Healthcare
    "XLI",   # Industrials
    "XLB",   # Materials
    "ARKK",  # ARK Innovation
    "SOXX",  # Semiconductors
    "XBI",   # Biotech
    "GLD",   # Gold ETF
    "TLT",   # Long-Term Treasuries
]

# ─────────────────────────────────────────────────────────────────────────────
#  UNITED KINGDOM — FTSE 100 Top 30
# ─────────────────────────────────────────────────────────────────────────────
UK_FTSE100 = [
    "SHEL.L",  # Shell
    "AZN.L",   # AstraZeneca
    "HSBA.L",  # HSBC
    "BP.L",    # BP
    "ULVR.L",  # Unilever
    "GSK.L",   # GlaxoSmithKline
    "DGE.L",   # Diageo
    "REL.L",   # RELX
    "NG.L",    # National Grid
    "RIO.L",   # Rio Tinto
    "BHP.L",   # BHP Group
    "LLOY.L",  # Lloyds Banking
    "BARC.L",  # Barclays
    "VOD.L",   # Vodafone
    "PRU.L",   # Prudential
    "IAG.L",   # International Airlines
    "BA.L",    # BAE Systems
    "EXPN.L",  # Experian
    "STAN.L",  # Standard Chartered
    "IMB.L",   # Imperial Brands
]

# ─────────────────────────────────────────────────────────────────────────────
#  GERMANY — DAX 40 Top 20
# ─────────────────────────────────────────────────────────────────────────────
DE_DAX40 = [
    "SAP.DE",   # SAP
    "SIE.DE",   # Siemens
    "ALV.DE",   # Allianz
    "DTE.DE",   # Deutsche Telekom
    "MUV2.DE",  # Munich Re
    "BMW.DE",   # BMW
    "VOW3.DE",  # Volkswagen
    "ADS.DE",   # Adidas
    "BAYN.DE",  # Bayer
    "BAS.DE",   # BASF
    "DB1.DE",   # Deutsche Boerse
    "RWE.DE",   # RWE
    "MBG.DE",   # Mercedes-Benz
    "IFX.DE",   # Infineon
    "EOAN.DE",  # E.ON
    "HEN3.DE",  # Henkel
    "LIN.DE",   # Linde (German listing)
    "MRK.DE",   # Merck Germany
    "DHER.DE",  # Delivery Hero
    "ZAL.DE",   # Zalando
]

# ─────────────────────────────────────────────────────────────────────────────
#  FRANCE — CAC 40 Top 15
# ─────────────────────────────────────────────────────────────────────────────
FR_CAC40 = [
    "MC.PA",    # LVMH
    "OR.PA",    # L'Oreal
    "TTE.PA",   # TotalEnergies
    "SAN.PA",   # Sanofi
    "AIR.PA",   # Airbus
    "BNP.PA",   # BNP Paribas
    "KER.PA",   # Kering
    "CS.PA",    # AXA
    "DG.PA",    # Vinci
    "RI.PA",    # Pernod Ricard
    "DSY.PA",   # Dassault Systèmes
    "HO.PA",    # Thales
    "LR.PA",    # Legrand
    "ENGI.PA",  # Engie
    "VIE.PA",   # Veolia
]

# ─────────────────────────────────────────────────────────────────────────────
#  JAPAN — Nikkei 225 Top 20
# ─────────────────────────────────────────────────────────────────────────────
JP_NIKKEI = [
    "7203.T",   # Toyota
    "6758.T",   # Sony
    "9984.T",   # SoftBank
    "6861.T",   # Keyence
    "8306.T",   # Mitsubishi UFJ
    "6367.T",   # Daikin
    "7267.T",   # Honda
    "4519.T",   # Chugai Pharma
    "9432.T",   # NTT
    "6098.T",   # Recruit Holdings
    "9983.T",   # Fast Retailing (Uniqlo)
    "6954.T",   # Fanuc
    "8316.T",   # Sumitomo Mitsui
    "7741.T",   # Hoya
    "6501.T",   # Hitachi
    "4063.T",   # Shin-Etsu Chemical
    "6702.T",   # Fujitsu
    "7751.T",   # Canon
    "9433.T",   # KDDI
    "4661.T",   # Oriental Land
]

# ─────────────────────────────────────────────────────────────────────────────
#  HONG KONG — Hang Seng Top 20
# ─────────────────────────────────────────────────────────────────────────────
HK_HANGSENG = [
    "0700.HK",  # Tencent
    "9988.HK",  # Alibaba
    "0941.HK",  # China Mobile
    "3690.HK",  # Meituan
    "1299.HK",  # AIA Group
    "0005.HK",  # HSBC Holdings
    "2318.HK",  # Ping An Insurance
    "0939.HK",  # China Construction Bank
    "1398.HK",  # ICBC
    "0388.HK",  # HK Exchanges
    "2382.HK",  # Sunny Optical
    "9618.HK",  # JD.com
    "1810.HK",  # Xiaomi
    "0883.HK",  # CNOOC
    "2020.HK",  # ANTA Sports
    "0027.HK",  # Galaxy Entertainment
    "0011.HK",  # Hang Seng Bank
    "1177.HK",  # Sino Biopharm
    "0175.HK",  # Geely Auto
    "9999.HK",  # NetEase
]

# ─────────────────────────────────────────────────────────────────────────────
#  CHINA — A-Share / H-Share / ADRs
# ─────────────────────────────────────────────────────────────────────────────
CN_STOCKS = [
    "BABA",     # Alibaba ADR
    "TCEHY",    # Tencent ADR
    "PDD",      # PDD Holdings ADR
    "JD",       # JD.com ADR
    "BIDU",     # Baidu ADR
    "NIO",      # NIO ADR
    "XPEV",     # XPeng ADR
    "LI",       # Li Auto ADR
    "NTES",     # NetEase ADR
    "KWEB",     # China Internet ETF
]

# ─────────────────────────────────────────────────────────────────────────────
#  THAILAND — SET 50 Top Stocks
# ─────────────────────────────────────────────────────────────────────────────
TH_SET50 = [
    "PTT.BK",    # PTT (Oil & Gas)
    "KBANK.BK",  # Kasikorn Bank
    "SCB.BK",    # Siam Commercial Bank
    "BBL.BK",    # Bangkok Bank
    "KTB.BK",    # Krungthai Bank
    "AOT.BK",    # Airports of Thailand
    "GULF.BK",   # Gulf Energy
    "MINT.BK",   # Minor International
    "CPALL.BK",  # CP All (7-Eleven Thailand)
    "AWC.BK",    # Asset World Corp
    "TRUE.BK",   # True Corporation
    "DTAC.BK",   # DTAC
    "ADVANC.BK", # Advanced Info Service (AIS)
    "INTUCH.BK", # Intouch Holdings
    "BH.BK",     # Bumrungrad Hospital
    "BDMS.BK",   # Bangkok Dusit Medical
    "CENTRAL.BK",# Central Retail
    "CRC.BK",    # Central Retail Corp
    "CPN.BK",    # Central Pattana
    "HMPRO.BK",  # HomePro
    "SCC.BK",    # Siam Cement Group
    "PTTGC.BK",  # PTT Global Chemical
    "IVL.BK",    # Indorama Ventures
    "IRPC.BK",   # IRPC
    "TOP.BK",    # Thai Oil
    "RATCH.BK",  # Ratchaburi Electricity
    "EGCO.BK",   # Electricity Generating
    "EA.BK",     # Energy Absolute
    "BGRIM.BK",  # B.Grimm Power
    "WHA.BK",    # WHA Corporation
]

# ─────────────────────────────────────────────────────────────────────────────
#  SINGAPORE — STI Top 15
# ─────────────────────────────────────────────────────────────────────────────
SG_STI = [
    "D05.SI",   # DBS Group
    "O39.SI",   # OCBC
    "U11.SI",   # UOB
    "C6L.SI",   # Singapore Airlines
    "Z74.SI",   # SingTel
    "BN4.SI",   # Keppel Corp
    "U96.SI",   # Sembcorp Industries
    "G13.SI",   # Genting Singapore
    "C31.SI",   # CapitaLand
    "ME8U.SI",  # Mapletree Logistics Trust
    "S68.SI",   # SGX
    "CC3.SI",   # StarHub
    "V03.SI",   # Venture Corp
    "S58.SI",   # SATS
    "M44U.SI",  # Mapletree Industrial Trust
]

# ─────────────────────────────────────────────────────────────────────────────
#  INDIA — NIFTY 50 Top 20
# ─────────────────────────────────────────────────────────────────────────────
IN_NIFTY = [
    "RELIANCE.NS",  # Reliance Industries
    "TCS.NS",       # TCS
    "HDFCBANK.NS",  # HDFC Bank
    "INFY.NS",      # Infosys
    "ICICIBANK.NS", # ICICI Bank
    "HINDUNILVR.NS",# Hindustan Unilever
    "ITC.NS",       # ITC
    "SBIN.NS",      # State Bank of India
    "BHARTIARTL.NS",# Bharti Airtel
    "KOTAKBANK.NS", # Kotak Mahindra Bank
    "LT.NS",        # Larsen & Toubro
    "BAJFINANCE.NS",# Bajaj Finance
    "WIPRO.NS",     # Wipro
    "HCLTECH.NS",   # HCL Tech
    "ASIANPAINT.NS",# Asian Paints
    "MARUTI.NS",    # Maruti Suzuki
    "AXISBANK.NS",  # Axis Bank
    "TITAN.NS",     # Titan Company
    "SUNPHARMA.NS", # Sun Pharma
    "ADANIENT.NS",  # Adani Enterprises
]

# ─────────────────────────────────────────────────────────────────────────────
#  AUSTRALIA — ASX Top 15
# ─────────────────────────────────────────────────────────────────────────────
AU_ASX = [
    "BHP.AX",   # BHP Group
    "CBA.AX",   # Commonwealth Bank
    "CSL.AX",   # CSL Limited
    "NAB.AX",   # National Australia Bank
    "WBC.AX",   # Westpac
    "ANZ.AX",   # ANZ Bank
    "WES.AX",   # Wesfarmers
    "RIO.AX",   # Rio Tinto
    "WOW.AX",   # Woolworths
    "GMG.AX",   # Goodman Group
    "MQG.AX",   # Macquarie Group
    "TCL.AX",   # Transurban
    "NCM.AX",   # Newcrest Mining
    "FMG.AX",   # Fortescue Metals
    "COL.AX",   # Coles Group
]

# ─────────────────────────────────────────────────────────────────────────────
#  GLOBAL INDICES (via ETFs and index futures)
# ─────────────────────────────────────────────────────────────────────────────
GLOBAL_INDICES = [
    "^GSPC",    # S&P 500 Index
    "^IXIC",    # NASDAQ Composite
    "^DJI",     # Dow Jones Industrial
    "^FTSE",    # FTSE 100
    "^GDAXI",   # DAX 40
    "^FCHI",    # CAC 40
    "^N225",    # Nikkei 225
    "^HSI",     # Hang Seng
    "^STI",     # Straits Times Index
    "^BSESN",   # BSE Sensex
    "^SET.BK",  # SET Index Thailand
    "^AXJO",    # ASX 200
]

# ─────────────────────────────────────────────────────────────────────────────
#  CURATED MARKET GROUPS
# ─────────────────────────────────────────────────────────────────────────────

MARKET_GROUPS = {
    "US_MEGA_CAP":    US_MEGA_CAP,
    "US_ETFS":        US_SECTOR_ETFS,
    "UK_FTSE100":     UK_FTSE100,
    "DE_DAX40":       DE_DAX40,
    "FR_CAC40":       FR_CAC40,
    "JP_NIKKEI":      JP_NIKKEI,
    "HK_HANGSENG":    HK_HANGSENG,
    "CN_STOCKS":      CN_STOCKS,
    "TH_SET50":       TH_SET50,        # 🇹🇭 Thailand (your home!)
    "SG_STI":         SG_STI,
    "IN_NIFTY":       IN_NIFTY,
    "AU_ASX":         AU_ASX,
    "GLOBAL_INDICES": GLOBAL_INDICES,
}

# Priority list for quick scan (most liquid global names)
PRIORITY_STOCKS = (
    ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"] +  # US
    ["SHEL.L", "AZN.L", "SAP.DE", "MC.PA"] +                           # EU
    ["7203.T", "0700.HK", "BABA"] +                                     # Asia
    ["PTT.BK", "KBANK.BK", "AOT.BK", "ADVANC.BK"] +                    # Thailand
    ["D05.SI", "RELIANCE.NS"]                                            # SG + IN
)

# Trading hours by market (UTC)
MARKET_HOURS = {
    "US":        {"open": "13:30", "close": "20:00", "tz": "EST"},
    "UK":        {"open": "08:00", "close": "16:30", "tz": "GMT"},
    "DE":        {"open": "08:00", "close": "16:30", "tz": "CET"},
    "FR":        {"open": "08:00", "close": "16:30", "tz": "CET"},
    "JP":        {"open": "00:00", "close": "06:00",  "tz": "JST"},
    "HK":        {"open": "01:30", "close": "08:00",  "tz": "HKT"},
    "TH":        {"open": "03:30", "close": "10:00",  "tz": "ICT"},
    "SG":        {"open": "01:00", "close": "09:00",  "tz": "SGT"},
    "IN":        {"open": "03:45", "close": "10:00",  "tz": "IST"},
    "AU":        {"open": "23:00", "close": "05:00",  "tz": "AEST"},
}

def get_all_stocks() -> list[str]:
    """Return deduplicated list of all global stocks."""
    all_tickers = []
    for group in MARKET_GROUPS.values():
        all_tickers.extend(group)
    return list(dict.fromkeys(all_tickers))  # preserve order, remove dups

def get_active_markets_now(utc_hour_min: str) -> list[str]:
    """Return markets currently open given UTC time (HH:MM)."""
    active = []
    for market, hours in MARKET_HOURS.items():
        if market == "US":
            # DST-aware US equities hours (09:30-16:00 America/New_York).
            ny_now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            if ny_now.weekday() < 5 and dt_time(9, 30) <= ny_now.time() <= dt_time(16, 0):
                active.append("US")
            continue

        o = hours["open"]
        c = hours["close"]
        if o <= c:  # normal hours
            if o <= utc_hour_min <= c:
                active.append(market)
        else:  # crosses midnight
            if utc_hour_min >= o or utc_hour_min <= c:
                active.append(market)
    return active
