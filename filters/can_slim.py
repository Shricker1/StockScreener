"""CAN SLIM 数据采集模块（无评分）。"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from analysis.indicators import rsi
from config import CONFIG
from data.cleaner import clean_daily_data, standardize_columns
from data.fetcher import DataFetcher


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.can_slim")
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
    """按候选列名查找第一个存在的列。"""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _latest_numeric(df: pd.DataFrame, col: str) -> float | None:
    """获取某列最新有效数值。"""
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.iloc[0]) if len(series) > 0 else None


def _get_daily_clean(fetcher: DataFetcher, symbol: str) -> pd.DataFrame:
    """统一获取并清洗日线数据。"""
    source = str(CONFIG.get("data_source", "akshare")).lower()
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=500)).strftime("%Y%m%d")
    raw = fetcher.get_daily(symbol=symbol, start_date=start_date, end_date=end_date)
    std = standardize_columns(raw, source=source)
    return clean_daily_data(std)


def get_c_data(stock_code: str) -> dict[str, Any]:
    """返回 C 项原始数据：当季EPS同比、当季营收同比。"""
    fetcher = DataFetcher()
    fin_df = fetcher.get_financial(stock_code)
    if fin_df is None or fin_df.empty:
        return {"eps_q_yoy": None, "revenue_q_yoy": None}
    eps_col = _find_column(fin_df, ["q_eps_yoy", "eps_yoy", "dt_eps_yoy"])
    rev_col = _find_column(fin_df, ["q_sales_yoy", "revenue_yoy", "tr_yoy"])
    return {
        "eps_q_yoy": _latest_numeric(fin_df, eps_col) if eps_col else None,
        "revenue_q_yoy": _latest_numeric(fin_df, rev_col) if rev_col else None,
    }


def get_a_data(stock_code: str) -> dict[str, Any]:
    """返回 A 项原始数据：3年EPS复合增速、最新ROE。"""
    fetcher = DataFetcher()
    fin_df = fetcher.get_financial(stock_code)
    if fin_df is None or fin_df.empty:
        return {"eps_cagr_3y": None, "roe": None}
    cagr_col = _find_column(
        fin_df,
        ["eps_cagr", "annual_eps_cagr", "ann_eps_cagr", "profit_cagr", "ann_eps_yoy"],
    )
    roe_col = _find_column(fin_df, ["roe", "roe_dt", "weighted_roe", "净资产收益率"])
    return {
        "eps_cagr_3y": _latest_numeric(fin_df, cagr_col) if cagr_col else None,
        "roe": _latest_numeric(fin_df, roe_col) if roe_col else None,
    }


def get_n_data(stock_code: str) -> dict[str, Any]:
    """返回 N 项原始数据：近3个月新闻关键词列表。"""
    try:
        import akshare as ak
    except Exception as exc:
        LOGGER.warning("get_n_data: akshare 不可用, stock=%s, err=%s", stock_code, exc)
        return {"news_keywords": None}

    news_df = pd.DataFrame()
    for func_name in ("stock_news_em", "stock_js_news"):
        try:
            func = getattr(ak, func_name, None)
            if func is None:
                continue
            news_df = func(symbol=stock_code)
            if isinstance(news_df, pd.DataFrame) and not news_df.empty:
                break
        except Exception:
            continue

    if news_df is None or news_df.empty:
        return {"news_keywords": None}
    time_col = _find_column(news_df, ["发布时间", "时间", "publish_time", "datetime", "pub_time"])
    title_col = _find_column(news_df, ["标题", "title", "新闻标题"])
    if title_col is None:
        return {"news_keywords": None}

    df = news_df.copy()
    if time_col:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        cutoff = datetime.now() - timedelta(days=90)
        df = df[df[time_col] >= cutoff]
    if df.empty:
        return {"news_keywords": None}
    titles = df[title_col].fillna("").astype(str).str.lower()
    keywords = ["新产品", "新品", "新技术", "技术突破", "管理层", "高管", "董事长", "ceo"]
    hit_words = [kw for kw in keywords if titles.str.contains(kw, regex=False).any()]
    return {"news_keywords": hit_words or None}


def get_s_data(stock_code: str) -> dict[str, Any]:
    """返回 S 项原始数据：量比、股东户数变化率。"""
    fetcher = DataFetcher()
    vol_ratio = None
    holder_change_rate = None
    try:
        daily_df = _get_daily_clean(fetcher, stock_code)
        if not daily_df.empty and len(daily_df) >= 60:
            vol = pd.to_numeric(daily_df["volume"], errors="coerce")
            vol_ma20 = float(vol.tail(20).mean())
            vol_ma60 = float(vol.tail(60).mean())
            if pd.notna(vol_ma20) and pd.notna(vol_ma60) and vol_ma60 != 0:
                vol_ratio = float(vol_ma20 / vol_ma60)
    except Exception as exc:
        LOGGER.warning("get_s_data(volume) 异常: stock=%s, err=%s", stock_code, exc)
    try:
        fin_df = fetcher.get_financial(stock_code)
        holder_col = _find_column(
            fin_df,
            ["holder_num", "holders", "holder_count", "股东户数", "股东人数"],
        )
        if holder_col and fin_df is not None and not fin_df.empty:
            holder_series = pd.to_numeric(fin_df[holder_col], errors="coerce").dropna()
            if len(holder_series) >= 2:
                latest_holder = float(holder_series.iloc[0])
                prev_holder = float(holder_series.iloc[1])
                if prev_holder != 0:
                    holder_change_rate = float((latest_holder - prev_holder) / prev_holder)
    except Exception as exc:
        LOGGER.warning("get_s_data(holder) 异常: stock=%s, err=%s", stock_code, exc)
    return {"vol_ratio": vol_ratio, "holder_change_rate": holder_change_rate}


def get_l_data(stock_code: str) -> dict[str, Any]:
    """返回 L 项原始数据：RSI、近6月个股涨幅、行业涨幅。"""
    fetcher = DataFetcher()
    latest_rsi = None
    stock_return_6m = None
    industry_return_6m = None
    stock_df = _get_daily_clean(fetcher, stock_code)
    if not stock_df.empty and len(stock_df) >= 126:
        rsi_series = rsi(stock_df, period=14)
        rsi_val = pd.to_numeric(rsi_series, errors="coerce").iloc[-1]
        latest_rsi = float(rsi_val) if pd.notna(rsi_val) else None
        close = pd.to_numeric(stock_df["close"], errors="coerce").dropna()
        if len(close) >= 126:
            stock_return_6m = float(close.iloc[-1] / close.iloc[-126] - 1.0)

    try:
        import akshare as ak
        industry_name = None
        info_df = pd.DataFrame()
        try:
            info_df = ak.stock_individual_info_em(symbol=stock_code)
        except Exception:
            info_df = pd.DataFrame()
        if isinstance(info_df, pd.DataFrame) and not info_df.empty:
            key_col = _find_column(info_df, ["item", "项目", "指标"])
            val_col = _find_column(info_df, ["value", "值", "内容"])
            if key_col and val_col:
                mask = info_df[key_col].astype(str).str.contains("行业", regex=False)
                if mask.any():
                    industry_name = str(info_df.loc[mask, val_col].iloc[0]).strip()
        if industry_name:
            ind_hist = pd.DataFrame()
            for func_name in ("stock_board_industry_hist_em", "stock_board_concept_hist_em"):
                try:
                    func = getattr(ak, func_name, None)
                    if func is None:
                        continue
                    ind_hist = func(symbol=industry_name)
                    if isinstance(ind_hist, pd.DataFrame) and not ind_hist.empty:
                        break
                except Exception:
                    continue
            if not ind_hist.empty:
                ind_std = standardize_columns(ind_hist, source="akshare")
                ind_clean = clean_daily_data(ind_std)
                ind_close = pd.to_numeric(ind_clean["close"], errors="coerce").dropna()
                if len(ind_close) >= 126:
                    industry_return_6m = float(ind_close.iloc[-1] / ind_close.iloc[-126] - 1.0)
    except Exception as exc:
        LOGGER.warning("get_l_data(industry) 异常: stock=%s, err=%s", stock_code, exc)
    return {
        "rsi": latest_rsi,
        "stock_return_6m": stock_return_6m,
        "industry_return_6m": industry_return_6m,
    }


def get_i_data(stock_code: str) -> dict[str, Any]:
    """返回 I 项原始数据：机构持股比例及变化、北向持股及变化。"""
    fetcher = DataFetcher()
    inst_holding = None
    inst_change = None
    northbound_holding = None
    northbound_change = None
    try:
        fin_df = fetcher.get_financial(stock_code)
        ratio_col = _find_column(
            fin_df,
            [
                "inst_holding_ratio",
                "holder_ratio",
                "fund_holding_ratio",
                "机构持仓比例",
                "机构持股比例",
            ],
        )
        if ratio_col and fin_df is not None and not fin_df.empty:
            ratio_series = pd.to_numeric(fin_df[ratio_col], errors="coerce").dropna()
            if len(ratio_series) >= 2:
                inst_holding = float(ratio_series.iloc[0])
                prev_ratio = float(ratio_series.iloc[1])
                if prev_ratio != 0:
                    inst_change = float((inst_holding - prev_ratio) / prev_ratio)
    except Exception as exc:
        LOGGER.warning("get_i_data(inst) 异常: stock=%s, err=%s", stock_code, exc)
    try:
        import akshare as ak

        north_df = pd.DataFrame()
        for func_name in ("stock_hsgt_individual_em", "stock_hsgt_stock_statistics_em"):
            try:
                func = getattr(ak, func_name, None)
                if func is None:
                    continue
                north_df = func(symbol=stock_code)
                if isinstance(north_df, pd.DataFrame) and not north_df.empty:
                    break
            except Exception:
                continue

        if north_df is None or north_df.empty:
            return {
                "inst_holding": inst_holding,
                "inst_change": inst_change,
                "northbound_holding": None,
                "northbound_change": None,
            }
        net_col = _find_column(
            north_df,
            [
                "净流入",
                "净买入",
                "今日增持估计-市值",
                "增持估计-市值",
                "持股市值变化",
                "net_inflow",
            ],
        )
        hold_col = _find_column(
            north_df,
            ["持股数量", "持股数", "持股量", "shareholding", "hold_shares"],
        )
        if hold_col:
            hold_series = pd.to_numeric(north_df[hold_col], errors="coerce").dropna()
            if not hold_series.empty:
                northbound_holding = float(hold_series.iloc[-1])
                if len(hold_series) >= 2 and hold_series.iloc[-2] != 0:
                    northbound_change = float(
                        (hold_series.iloc[-1] - hold_series.iloc[-2]) / hold_series.iloc[-2]
                    )
        if net_col:
            net_series = pd.to_numeric(north_df[net_col], errors="coerce").dropna()
            if not net_series.empty:
                northbound_change = float(net_series.tail(20).sum())
    except Exception as exc:
        LOGGER.warning("get_i_data(north) 异常: stock=%s, err=%s", stock_code, exc)
    return {
        "inst_holding": inst_holding,
        "inst_change": inst_change,
        "northbound_holding": northbound_holding,
        "northbound_change": northbound_change,
    }
