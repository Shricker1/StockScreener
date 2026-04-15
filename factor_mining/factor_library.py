"""标准因子库：估值/成长/质量/动量/情绪/技术。"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from data.fetcher import DataFetcher
from reports.data_collector import collect_stock_report_data


def _safe_float(v: Any) -> float | None:
    try:
        if v in (None, "N/A", ""):
            return None
        return float(v)
    except Exception:
        return None


def _snapshot(stock_code: str) -> dict[str, Any]:
    """取单股快照（复用现有报告收集逻辑）。"""
    try:
        return collect_stock_report_data(stock_code)
    except Exception:
        return {}


def _daily_df(stock_code: str, days: int = 260) -> pd.DataFrame:
    end = datetime.now()
    start = end - timedelta(days=max(days * 2, 400))
    fetcher = DataFetcher()
    return fetcher.get_daily(
        stock_code,
        start.strftime("%Y%m%d"),
        end.strftime("%Y%m%d"),
    )


# ========= 估值因子 =========
def calc_factor_pe(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("pe"))


def calc_factor_pb(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("pb"))


def calc_factor_ps(stock_code: str, date: str) -> float | None:
    rev = _safe_float(_snapshot(stock_code).get("revenue_q_yoy"))
    return None if rev is None else max(0.1, 100.0 / (abs(rev) + 1.0))


def calc_factor_peg(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("peg"))


def calc_factor_dividend_yield(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("dividend_yield"))


# ========= 成长因子 =========
def calc_factor_eps_yoy(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("eps_q_yoy"))


def calc_factor_revenue_yoy(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("revenue_q_yoy"))


def calc_factor_roe(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("roe"))


# ========= 质量因子 =========
def calc_factor_gross_margin(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("gross_margin"))


def calc_factor_net_margin(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("net_margin"))


def calc_factor_debt_ratio(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("debt_ratio"))


# ========= 动量因子 =========
def calc_factor_rsi(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("rsi"))


def calc_factor_return_1m(stock_code: str, date: str) -> float | None:
    df = _daily_df(stock_code, days=80)
    if df is None or df.empty or len(df) < 22:
        return None
    c = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(c) < 22:
        return None
    return float(c.iloc[-1] / c.iloc[-22] - 1.0)


def calc_factor_return_3m(stock_code: str, date: str) -> float | None:
    df = _daily_df(stock_code, days=160)
    if df is None or df.empty or len(df) < 66:
        return None
    c = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(c) < 66:
        return None
    return float(c.iloc[-1] / c.iloc[-66] - 1.0)


def calc_factor_return_6m(stock_code: str, date: str) -> float | None:
    df = _daily_df(stock_code, days=260)
    if df is None or df.empty or len(df) < 126:
        return None
    c = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(c) < 126:
        return None
    return float(c.iloc[-1] / c.iloc[-126] - 1.0)


def calc_factor_ma_bias(stock_code: str, date: str) -> float | None:
    snap = _snapshot(stock_code)
    close = _safe_float(snap.get("close"))
    ma50 = _safe_float(snap.get("ma50"))
    if close is None or ma50 in (None, 0):
        return None
    return float((close - ma50) / ma50)


# ========= 情绪因子 =========
def calc_factor_northbound_change(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("northbound_change"))


def calc_factor_inst_holding_change(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("inst_change"))


def calc_factor_news_sentiment(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("news_sentiment"))


# ========= 技术因子 =========
def calc_factor_volume_ratio(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("vol_ratio"))


def calc_factor_turnover_rate(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("turnover_rate"))


def calc_factor_atr_volatility(stock_code: str, date: str) -> float | None:
    return _safe_float(_snapshot(stock_code).get("atr"))


FACTOR_FUNCTIONS = {
    "pe": calc_factor_pe,
    "pb": calc_factor_pb,
    "ps": calc_factor_ps,
    "peg": calc_factor_peg,
    "dividend_yield": calc_factor_dividend_yield,
    "eps_yoy": calc_factor_eps_yoy,
    "revenue_yoy": calc_factor_revenue_yoy,
    "roe": calc_factor_roe,
    "gross_margin": calc_factor_gross_margin,
    "net_margin": calc_factor_net_margin,
    "debt_ratio": calc_factor_debt_ratio,
    "rsi": calc_factor_rsi,
    "return_1m": calc_factor_return_1m,
    "return_3m": calc_factor_return_3m,
    "return_6m": calc_factor_return_6m,
    "ma_bias": calc_factor_ma_bias,
    "northbound_change": calc_factor_northbound_change,
    "inst_holding_change": calc_factor_inst_holding_change,
    "news_sentiment": calc_factor_news_sentiment,
    "volume_ratio": calc_factor_volume_ratio,
    "turnover_rate": calc_factor_turnover_rate,
    "atr_volatility": calc_factor_atr_volatility,
}


def extract_factor_values_from_report(raw_data: dict[str, Any]) -> dict[str, float | None]:
    """从单股报告扁平数据中提取可落库因子值。"""
    return {
        "pe": _safe_float(raw_data.get("pe")),
        "pb": _safe_float(raw_data.get("pb")),
        "peg": _safe_float(raw_data.get("peg")),
        "eps_yoy": _safe_float(raw_data.get("eps_q_yoy")),
        "revenue_yoy": _safe_float(raw_data.get("revenue_q_yoy")),
        "roe": _safe_float(raw_data.get("roe")),
        "rsi": _safe_float(raw_data.get("rsi")),
        "news_sentiment": _safe_float(raw_data.get("news_sentiment")),
        "volume_ratio": _safe_float(raw_data.get("vol_ratio")),
        "northbound_change": _safe_float(raw_data.get("northbound_change")),
    }

