"""
安全边际估值过滤器
-----------------
实现：
1) calc_pe_percentile(stock_code, years=5)
2) calc_pb_percentile(stock_code, years=5)
3) calc_peg(stock_code)
4) get_valuation_data(stock_code)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from config import CONFIG
from data.fetcher import DataFetcher


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.safety_margin")
    if logger.handlers:
        return logger

    log_path = Path(CONFIG.get("output", {}).get("log_file", "logs/app.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


LOGGER = _build_logger()


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _percentile_of_current(series: pd.Series) -> float | None:
    """
    计算“当前值在历史中的分位数（0~100）”：
    分位数越低，代表当前估值越便宜。
    """
    s = pd.to_numeric(series, errors="coerce").dropna()
    s = s[s > 0]
    if s.empty:
        return None

    current = float(s.iloc[-1])
    pct = float((s <= current).mean() * 100.0)
    return pct


def _get_valuation_history(stock_code: str, years: int = 5) -> pd.DataFrame:
    """
    获取估值历史，尽量产出包含 pe/pb 列的 DataFrame。
    - 优先遵循 CONFIG["data_source"]
    - 若主路径失败，会尝试 AkShare 兜底
    """
    fetcher = DataFetcher()
    source = str(CONFIG.get("data_source", "akshare")).lower()
    start_date = (datetime.now() - timedelta(days=365 * years + 30)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")

    # 主路径：AkShare
    if source == "akshare":
        try:
            import akshare as ak

            df = ak.stock_a_lg_indicator(symbol=stock_code)
            # 常见列：trade_date, pe, pb, pe_ttm
            if isinstance(df, pd.DataFrame) and not df.empty:
                date_col = _find_column(df, ["trade_date", "日期", "date"])
                if date_col:
                    d = df.copy()
                    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
                    d = d[d[date_col] >= pd.to_datetime(start_date)]
                    d = d.sort_values(date_col).reset_index(drop=True)
                    return d
                return df
        except Exception as exc:
            LOGGER.warning("AkShare 估值历史拉取失败: %s", exc)

    # 主路径：Tushare
    if source == "tushare":
        try:
            # 使用 fetcher 内部的 pro，避免重复鉴权
            df = fetcher.pro.daily_basic(
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,pe_ttm,pb",
            )
            if isinstance(df, pd.DataFrame) and not df.empty:
                d = df.copy()
                d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce")
                d = d.sort_values("trade_date").reset_index(drop=True)
                return d
        except Exception as exc:
            LOGGER.warning("Tushare 估值历史拉取失败: %s", exc)

    # 兜底：直接尝试 AkShare
    try:
        import akshare as ak

        df = ak.stock_a_lg_indicator(symbol=stock_code)
        if isinstance(df, pd.DataFrame) and not df.empty:
            date_col = _find_column(df, ["trade_date", "日期", "date"])
            if date_col:
                d = df.copy()
                d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
                d = d[d[date_col] >= pd.to_datetime(start_date)]
                d = d.sort_values(date_col).reset_index(drop=True)
                return d
            return df
    except Exception as exc:
        LOGGER.warning("估值历史兜底拉取失败: %s", exc)

    return pd.DataFrame()


def calc_pe_percentile(stock_code: str, years: int = 5) -> float | None:
    """计算当前 PE 在近 years 年历史中的分位数（0~100）。"""
    df = _get_valuation_history(stock_code, years=years)
    if df.empty:
        return None

    pe_col = _find_column(df, ["pe_ttm", "pe", "市盈率", "PE"])
    if pe_col is None:
        return None
    pct = _percentile_of_current(df[pe_col])
    LOGGER.info("PE percentile: %s -> %s", stock_code, pct)
    return pct


def calc_current_pe(stock_code: str, years: int = 1) -> float | None:
    """返回当前 PE。"""
    df = _get_valuation_history(stock_code, years=years)
    if df.empty:
        return None
    pe_col = _find_column(df, ["pe_ttm", "pe", "市盈率", "PE"])
    if pe_col is None:
        return None
    pe_series = pd.to_numeric(df[pe_col], errors="coerce").dropna()
    pe_series = pe_series[pe_series > 0]
    return float(pe_series.iloc[-1]) if not pe_series.empty else None


def calc_pb_percentile(stock_code: str, years: int = 5) -> float | None:
    """计算当前 PB 在近 years 年历史中的分位数（0~100）。"""
    df = _get_valuation_history(stock_code, years=years)
    if df.empty:
        return None

    pb_col = _find_column(df, ["pb", "市净率", "PB"])
    if pb_col is None:
        return None
    pct = _percentile_of_current(df[pb_col])
    LOGGER.info("PB percentile: %s -> %s", stock_code, pct)
    return pct


def calc_current_pb(stock_code: str, years: int = 1) -> float | None:
    """返回当前 PB。"""
    df = _get_valuation_history(stock_code, years=years)
    if df.empty:
        return None
    pb_col = _find_column(df, ["pb", "市净率", "PB"])
    if pb_col is None:
        return None
    pb_series = pd.to_numeric(df[pb_col], errors="coerce").dropna()
    pb_series = pb_series[pb_series > 0]
    return float(pb_series.iloc[-1]) if not pb_series.empty else None


def calc_peg(stock_code: str) -> float | None:
    """
    计算 PEG（Price/Earnings to Growth）。
    近似公式：PEG = 当前 PE / EPS增长率（百分比口径）
    """
    fetcher = DataFetcher()

    # 当前 PE：优先从估值历史的最新值取
    val_df = _get_valuation_history(stock_code, years=1)
    pe_col = _find_column(val_df, ["pe_ttm", "pe", "市盈率", "PE"]) if not val_df.empty else None
    if pe_col is None:
        return None
    pe_series = pd.to_numeric(val_df[pe_col], errors="coerce").dropna()
    pe_series = pe_series[pe_series > 0]
    if pe_series.empty:
        return None
    pe = float(pe_series.iloc[-1])

    # EPS 增长率：优先季度/年度 EPS 同比字段
    fin_df = fetcher.get_financial(stock_code)
    if fin_df is None or fin_df.empty:
        return None

    growth_col = _find_column(
        fin_df,
        ["eps_yoy", "q_eps_yoy", "ann_eps_yoy", "dt_eps_yoy", "netprofit_yoy", "净利润同比增长率"],
    )
    if growth_col is None:
        return None

    growth = pd.to_numeric(fin_df[growth_col], errors="coerce").dropna()
    if growth.empty:
        return None
    growth_val = float(growth.iloc[0])
    if growth_val <= 0:
        return None

    peg = pe / growth_val
    LOGGER.info("PEG: %s -> pe=%.4f, growth=%.4f, peg=%.4f", stock_code, pe, growth_val, peg)
    return float(peg)


def get_valuation_data(stock_code: str) -> dict[str, Any]:
    """整合估值数据（无打分）。"""
    pe = calc_current_pe(stock_code)
    pb = calc_current_pb(stock_code)
    pe_pct = calc_pe_percentile(stock_code, years=5)
    pb_pct = calc_pb_percentile(stock_code, years=5)
    peg = calc_peg(stock_code)
    return {
        "stock_code": stock_code,
        "pe": None if pe is None else round(pe, 4),
        "pe_percentile": None if pe_pct is None else round(pe_pct, 2),
        "pb": None if pb is None else round(pb, 4),
        "pb_percentile": None if pb_pct is None else round(pb_pct, 2),
        "peg": None if peg is None else round(peg, 4),
    }
