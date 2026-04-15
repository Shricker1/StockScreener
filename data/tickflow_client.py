"""
TickFlow 可选数据源（与 Tushare 协作）
------------------------------------
- K 线：在 Tushare 日线/指数线为空时回退，或用于主要指数补充
- 标的池行情：用于全市场涨跌家数等统计（需套餐支持 universe 查询）

文档: https://docs.tickflow.org
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd

from config import CONFIG, TICKFLOW_API_KEY, TICKFLOW_BASE_URL

if TYPE_CHECKING:
    pass

LOGGER = logging.getLogger("stock_screener.tickflow")

_tickflow_singleton: Any = None


def get_tickflow_client() -> Any:
    """懒加载 TickFlow 同步客户端；未配置 API Key 时返回 None。"""
    global _tickflow_singleton
    if not TICKFLOW_API_KEY:
        return None
    if _tickflow_singleton is not None:
        return _tickflow_singleton
    try:
        from tickflow import TickFlow

        base = (TICKFLOW_BASE_URL or "").strip() or None
        _tickflow_singleton = TickFlow(api_key=TICKFLOW_API_KEY, base_url=base)
        LOGGER.info("TickFlow 客户端初始化成功")
    except ImportError:
        LOGGER.warning("未安装 tickflow 包，请执行: pip install tickflow")
        _tickflow_singleton = False
    except Exception as exc:
        LOGGER.warning("TickFlow 初始化失败: %s", exc)
        _tickflow_singleton = False
    return _tickflow_singleton if _tickflow_singleton is not False else None


def klines_to_tushare_like_daily(df: pd.DataFrame) -> pd.DataFrame:
    """将 TickFlow 日 K DataFrame 转为与 Tushare daily/index_daily 接近的列名。"""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "trade_date" in out.columns:
        td = pd.to_datetime(out["trade_date"], errors="coerce")
        out["trade_date"] = td.dt.strftime("%Y%m%d")
    vol_src = "volume" if "volume" in out.columns else None
    if vol_src and "vol" not in out.columns:
        out["vol"] = pd.to_numeric(out[vol_src], errors="coerce")
    elif "vol" in out.columns:
        out["vol"] = pd.to_numeric(out["vol"], errors="coerce")
    for col in ("open", "high", "low", "close"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "amount" in out.columns:
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    out = out.dropna(subset=["trade_date"], how="any")
    out = out.sort_values("trade_date").reset_index(drop=True)
    return out


def fetch_daily_klines(
    ts_code: str,
    start_date: str,
    end_date: str,
    *,
    adjust: str = "forward",
) -> pd.DataFrame:
    """
    拉取日 K 并返回 Tushare 风格的列（trade_date 为 YYYYMMDD 字符串）。
    ts_code 示例：600519.SH、000300.SH
    """
    client = get_tickflow_client()
    if client is None:
        return pd.DataFrame()
    if not CONFIG.get("tickflow_use_kline_fallback", True):
        return pd.DataFrame()
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        end_dt = end_dt.replace(hour=23, minute=59, second=59)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        raw = client.klines.get(
            ts_code,
            period="1d",
            start_time=start_ms,
            end_time=end_ms,
            adjust=adjust,
            as_dataframe=True,
        )
    except Exception as exc:
        LOGGER.warning("TickFlow K线请求失败 %s: %s", ts_code, exc)
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()
    return klines_to_tushare_like_daily(raw)


def enrich_market_stats_from_universe(stats: dict[str, Any]) -> dict[str, Any]:
    """
    使用标的池全市场行情补充涨跌家数、涨跌停家数（AkShare 不可用或统计为 0 时）。
    """
    if not CONFIG.get("tickflow_use_universe_market_stats", True):
        return stats
    client = get_tickflow_client()
    if client is None:
        return stats
    universe = str(CONFIG.get("tickflow_market_universe", "CN_Equity_A") or "CN_Equity_A")
    try:
        df = client.quotes.get(universes=[universe], as_dataframe=True)
    except Exception as exc:
        LOGGER.warning("TickFlow 标的池行情失败 universe=%s: %s", universe, exc)
        return stats
    if df is None or df.empty:
        return stats
    chg_col = None
    for c in ("ext.change_pct", "change_pct"):
        if c in df.columns:
            chg_col = c
            break
    if chg_col is None:
        return stats
    chg = pd.to_numeric(df[chg_col], errors="coerce").dropna()
    if chg.empty:
        return stats
    stats = dict(stats)
    stats["up_count"] = int((chg > 0).sum())
    stats["down_count"] = int((chg < 0).sum())
    stats["limit_up_count"] = int((chg >= 9.5).sum())
    stats["limit_down_count"] = int((chg <= -9.5).sum())
    total = max(1, stats["up_count"] + stats["down_count"])
    stats["market_sentiment_score"] = round(
        (stats["up_count"] - stats["down_count"]) / total, 4
    )
    stats["market_stats_source"] = "tickflow_universe"
    return stats
