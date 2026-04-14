"""Collect per-stock data for markdown reports."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from analysis.indicators import (
    is_bearish_engulfing,
    is_bullish_engulfing,
    is_doji,
    is_hammer,
    is_morning_star,
    macd,
    rsi,
    sma,
)
from analysis.sentiment import analyze_news_sentiment
from config import CONFIG
from data.cleaner import clean_daily_data, standardize_columns
from data.fetcher import DataFetcher
from data.news_fetcher import get_market_sentiment, get_stock_news
from filters.can_slim import get_a_data, get_c_data, get_i_data, get_l_data, get_n_data, get_s_data
from filters.safety_margin import get_valuation_data


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.report_collector")
    if logger.handlers:
        return logger
    log_path = Path(CONFIG.get("output", {}).get("log_file", "logs/app.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    return logger


LOGGER = _build_logger()


def _na_fill(data: dict[str, Any]) -> dict[str, Any]:
    return {k: ("N/A" if v is None else v) for k, v in data.items()}


def _macd_status(df: pd.DataFrame) -> str:
    m = macd(df)
    if m.empty:
        return "N/A"
    line = float(m["macd"].iloc[-1])
    signal = float(m["signal"].iloc[-1])
    prev_line = float(m["macd"].iloc[-2]) if len(m) > 1 else line
    prev_signal = float(m["signal"].iloc[-2]) if len(m) > 1 else signal
    if line > signal and prev_line <= prev_signal:
        return "金叉"
    if line < signal and prev_line >= prev_signal:
        return "死叉"
    return "多头" if line > signal else "空头"


def _latest_candle_pattern(df: pd.DataFrame) -> str:
    patterns = {
        "十字星": is_doji(df),
        "锤子线": is_hammer(df),
        "看涨吞没": is_bullish_engulfing(df),
        "看跌吞没": is_bearish_engulfing(df),
        "启明星": is_morning_star(df),
    }
    names = [name for name, s in patterns.items() if not s.empty and bool(s.iloc[-1])]
    return "、".join(names) if names else "无明显形态"


def _extract_chip_metrics(stock_code: str, current_price: float | None) -> dict[str, Any]:
    """提取筹码分布关键指标；失败时返回空值。"""
    out = {
        "chip_profit_ratio": None,
        "chip_avg_cost": None,
        "chip_concentration": None,
    }
    try:
        import akshare as ak

        cyq = ak.stock_cyq_em(symbol=stock_code)
        if cyq is None or cyq.empty:
            return out
        row = cyq.iloc[-1]
        cols = {str(c): c for c in cyq.columns}

        def _num(candidates: list[str]) -> float | None:
            for name in candidates:
                key = cols.get(name)
                if key is not None:
                    val = pd.to_numeric(pd.Series([row.get(key)]), errors="coerce").iloc[0]
                    if pd.notna(val):
                        return float(val)
            return None

        profit_ratio = _num(["获利比例", "获利盘比例", "profit_ratio"])
        if profit_ratio is not None:
            out["chip_profit_ratio"] = round(profit_ratio, 2)
        avg_cost = _num(["平均成本", "avg_cost", "cost_avg", "平均持仓成本"])
        if avg_cost is not None:
            out["chip_avg_cost"] = round(avg_cost, 3)
        # 用 90%成本上限-下限 近似集中度宽度，再除以当前价
        p90_low = _num(["90成本-低", "90成本下限", "cost_90_low"])
        p90_high = _num(["90成本-高", "90成本上限", "cost_90_high"])
        if p90_low is not None and p90_high is not None and current_price and current_price > 0:
            out["chip_concentration"] = round((p90_high - p90_low) / current_price, 4)
    except Exception as exc:
        LOGGER.warning("筹码分布获取失败: %s, err=%s", stock_code, exc)
    return out


def _extract_bid_ask_ratio(stock_code: str) -> float | None:
    """从实时盘口快照计算委比(%)。"""
    try:
        import akshare as ak

        spot = ak.stock_zh_a_spot_em()
        if spot is None or spot.empty:
            return None
        code_col = "代码" if "代码" in spot.columns else None
        if code_col is None:
            return None
        base = spot[spot[code_col].astype(str) == str(stock_code)]
        if base.empty:
            return None
        row = base.iloc[0]
        buy = pd.to_numeric(pd.Series([row.get("委买", row.get("买盘", None))]), errors="coerce").iloc[0]
        sell = pd.to_numeric(pd.Series([row.get("委卖", row.get("卖盘", None))]), errors="coerce").iloc[0]
        if pd.isna(buy) or pd.isna(sell):
            # 回退：若存在直接委比列则直接用
            direct = pd.to_numeric(pd.Series([row.get("委比")]), errors="coerce").iloc[0]
            return round(float(direct), 2) if pd.notna(direct) else None
        denom = float(buy + sell)
        if denom == 0:
            return None
        return round(float((buy - sell) / denom * 100), 2)
    except Exception as exc:
        LOGGER.warning("委比获取失败: %s, err=%s", stock_code, exc)
        return None


def _build_recent_k_pattern(df: pd.DataFrame, lookback: int = 10) -> tuple[str, str]:
    """生成近N日K线形态描述与Markdown表格。"""
    if df is None or df.empty:
        return "N/A", "N/A"
    try:
        use = df.tail(lookback).copy()
        for c in ("open", "high", "low", "close", "volume"):
            if c in use.columns:
                use[c] = pd.to_numeric(use[c], errors="coerce")
        use = use.dropna(subset=["open", "high", "low", "close"], how="any")
        if use.empty:
            return "N/A", "N/A"
        close = use["close"]
        up_days = int((close.diff() > 0).sum())
        down_days = int((close.diff() < 0).sum())
        last = use.iloc[-1]
        prev = use.iloc[-2] if len(use) >= 2 else last
        desc_parts: list[str] = []
        if up_days >= 3 and down_days <= 1:
            desc_parts.append("近10日偏强，存在连阳特征")
        elif down_days >= 3 and up_days <= 1:
            desc_parts.append("近10日偏弱，存在连阴特征")
        if float(last["close"]) > float(prev["close"]) and float(last["high"]) >= float(use["high"].tail(5).max()):
            desc_parts.append("短线放量/突破迹象")
        if "volume" in use.columns:
            vol = pd.to_numeric(use["volume"], errors="coerce")
            if vol.notna().sum() >= 6:
                if float(vol.iloc[-1]) > float(vol.tail(5).mean() * 1.5):
                    desc_parts.append("量能放大")
                elif float(vol.iloc[-1]) < float(vol.tail(5).mean() * 0.7):
                    desc_parts.append("量能收缩")
        desc = "；".join(desc_parts) if desc_parts else "近10日震荡为主"
        table_cols = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in use.columns]
        table_df = use[table_cols].copy()
        if "date" in table_df.columns:
            table_df["date"] = pd.to_datetime(table_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        headers = "| " + " | ".join(table_cols) + " |"
        sep = "| " + " | ".join(["---"] * len(table_cols)) + " |"
        rows: list[str] = [headers, sep]
        for _, row in table_df.iterrows():
            vals = [str(row.get(c, "")) for c in table_cols]
            rows.append("| " + " | ".join(vals) + " |")
        return desc, "\n".join(rows)
    except Exception as exc:
        LOGGER.warning("构建滚动K线描述失败: err=%s", exc)
        return "N/A", "N/A"


def collect_stock_report_data(stock_code: str) -> dict[str, Any]:
    fetcher = DataFetcher()
    source = str(CONFIG.get("data_source", "akshare"))
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
    result: dict[str, Any] = {"code": stock_code}

    try:
        raw_df = fetcher.get_daily(stock_code, start_date, end_date)
        std_df = standardize_columns(raw_df, source=source)
        df = clean_daily_data(std_df)
    except Exception as exc:
        LOGGER.warning("日线获取失败: %s, err=%s", stock_code, exc)
        df = pd.DataFrame()

    latest_close: float | None = None
    try:
        if not df.empty and len(df) >= 220:
            df["ma50"] = sma(df, 50)
            df["ma150"] = sma(df, 150)
            df["ma200"] = sma(df, 200)
            close = float(df["close"].iloc[-1])
            ma50 = float(df["ma50"].iloc[-1])
            ma150 = float(df["ma150"].iloc[-1])
            ma200 = float(df["ma200"].iloc[-1])
            high_52w = float(df["close"].tail(252).max())
            low_52w = float(df["close"].tail(252).min())
            ma200_prev = float(df["ma200"].iloc[-21]) if len(df) > 220 else ma200
            rs_df = clean_daily_data(
                standardize_columns(
                    fetcher.get_daily("000300", start_date, end_date),
                    source=source,
                )
            )
            rs_trend = "N/A"
            if not rs_df.empty and len(rs_df) >= 30:
                stock_ret_6w = float(df["close"].iloc[-1] / df["close"].iloc[-30] - 1.0)
                idx_ret_6w = float(rs_df["close"].iloc[-1] / rs_df["close"].iloc[-30] - 1.0)
                rs_trend = "上升" if stock_ret_6w > idx_ret_6w else "下降"
            result.update(
                {
                    "close": close,
                    "ma50": ma50,
                    "ma150": ma150,
                    "ma200": ma200,
                    "high_52w": high_52w,
                    "low_52w": low_52w,
                    "pct_from_high": round((close / high_52w - 1) * 100, 2) if high_52w else None,
                    "pct_from_low": round((close / low_52w - 1) * 100, 2) if low_52w else None,
                    "ma200_change": round((ma200 / ma200_prev - 1) * 100, 2) if ma200_prev else None,
                    "rs_trend_6w": rs_trend,
                    "rsi": round(float(rsi(df, 14).iloc[-1]), 2),
                    "macd_status": _macd_status(df),
                    "candle_pattern": _latest_candle_pattern(df),
                }
            )
            latest_close = close
    except Exception as exc:
        LOGGER.warning("技术指标计算失败: %s, err=%s", stock_code, exc)

    # 技术顾问专用字段（筹码/委比/日涨幅/K线形态）
    try:
        if latest_close is None and not df.empty and "close" in df.columns:
            latest_close = float(pd.to_numeric(df["close"], errors="coerce").dropna().iloc[-1])
    except Exception:
        latest_close = None
    result.update(_extract_chip_metrics(stock_code, latest_close))
    result["bid_ask_ratio"] = _extract_bid_ask_ratio(stock_code)
    try:
        if not df.empty and len(df) >= 2:
            close_ser = pd.to_numeric(df["close"], errors="coerce")
            high_ser = pd.to_numeric(df["high"], errors="coerce")
            low_ser = pd.to_numeric(df["low"], errors="coerce")
            prev_close = float(close_ser.iloc[-2]) if pd.notna(close_ser.iloc[-2]) else None
            now_close = float(close_ser.iloc[-1]) if pd.notna(close_ser.iloc[-1]) else None
            now_high = float(high_ser.iloc[-1]) if pd.notna(high_ser.iloc[-1]) else None
            now_low = float(low_ser.iloc[-1]) if pd.notna(low_ser.iloc[-1]) else None
            if prev_close and now_close is not None:
                result["daily_pct_change"] = round((now_close / prev_close - 1) * 100, 2)
            else:
                result["daily_pct_change"] = None
            if prev_close and now_high is not None and now_low is not None:
                result["daily_amplitude"] = round((now_high - now_low) / prev_close * 100, 2)
            else:
                result["daily_amplitude"] = None
        else:
            result["daily_pct_change"] = None
            result["daily_amplitude"] = None
    except Exception as exc:
        LOGGER.warning("日涨幅/振幅计算失败: %s, err=%s", stock_code, exc)
        result["daily_pct_change"] = None
        result["daily_amplitude"] = None
    recent_desc, recent_table = _build_recent_k_pattern(df, lookback=10)
    result["recent_k_pattern"] = recent_desc
    result["recent_k_table_md"] = recent_table

    try:
        c_data = get_c_data(stock_code)
        a_data = get_a_data(stock_code)
        n_data = get_n_data(stock_code)
        s_data = get_s_data(stock_code)
        l_data = get_l_data(stock_code)
        i_data = get_i_data(stock_code)
        valuation = get_valuation_data(stock_code)
        result.update(c_data)
        result.update(a_data)
        result.update(n_data)
        result.update(s_data)
        result.update(l_data)
        result.update(i_data)
        result.update(valuation)
    except Exception as exc:
        LOGGER.warning("CAN SLIM/估值数据汇总失败: %s, err=%s", stock_code, exc)

    try:
        news_list = get_stock_news(stock_code, days=30)
        sent = analyze_news_sentiment(news_list)
        market = get_market_sentiment()
        headlines = [str(x.get("title", "")).strip() for x in news_list[:5] if str(x.get("title", "")).strip()]
        up = market.get("up_count", 0) or 0
        down = market.get("down_count", 0) or 0
        up_down_ratio = f"{int(up)}/{int(down)}"
        result.update(
            {
                "news_headlines": "；".join(headlines) if headlines else "N/A",
                "news_sentiment": sent.get("sentiment_score", 0),
                "market_up_down_ratio": up_down_ratio,
                "northbound_net_flow": market.get("northbound_net_flow", 0.0),
            }
        )
    except Exception as exc:
        LOGGER.warning("新闻与情绪数据获取失败: %s, err=%s", stock_code, exc)
        result.update(
            {
                "news_headlines": "N/A",
                "news_sentiment": 0,
                "market_up_down_ratio": "N/A",
                "northbound_net_flow": 0,
            }
        )

    try:
        all_list = fetcher.get_all_stock_list()
        if not all_list.empty:
            code_col = "code" if "code" in all_list.columns else "symbol"
            base = all_list[all_list[code_col].astype(str) == stock_code]
            if not base.empty:
                row = base.iloc[0]
                result["name"] = row.get("name", "N/A")
                result["industry"] = row.get("industry", "N/A")
                result["total_mv"] = row.get("总市值", "N/A")
                result["circ_mv"] = row.get("流通市值", "N/A")
    except Exception as exc:
        LOGGER.warning("基础信息获取失败: %s, err=%s", stock_code, exc)

    result.setdefault("name", "N/A")
    result.setdefault("industry", "N/A")
    result.setdefault("total_mv", "N/A")
    result.setdefault("circ_mv", "N/A")
    return _na_fill(result)

