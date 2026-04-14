"""News and market sentiment data fetchers."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config import CONFIG
from data.fetcher import DataFetcher
from data.tickflow_client import enrich_market_stats_from_universe


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.news_fetcher")
    if logger.handlers:
        return logger
    log_path = Path(CONFIG.get("output", {}).get("log_file", "logs/app.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(fh)
    logger.addHandler(logging.StreamHandler())
    return logger


LOGGER = _build_logger()


def get_stock_news(stock_code: str, days: int = 30) -> list[dict]:
    news_items: list[dict] = []
    cutoff = datetime.now() - timedelta(days=days)
    try:
        import akshare as ak

        df = pd.DataFrame()
        for fn in ("stock_news_em", "stock_js_news"):
            try:
                f = getattr(ak, fn, None)
                if f is None:
                    continue
                # 优先使用要求的 stock 参数；若不兼容则回退 symbol。
                try:
                    df = f(stock=stock_code)
                except TypeError:
                    df = f(symbol=stock_code)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    break
            except Exception:
                continue
        if not df.empty:
            time_col = next((c for c in ["发布时间", "时间", "publish_time", "datetime"] if c in df.columns), None)
            title_col = next((c for c in ["标题", "title", "新闻标题"] if c in df.columns), None)
            content_col = next((c for c in ["内容", "content", "摘要"] if c in df.columns), None)
            if title_col:
                if time_col:
                    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
                    df = df[df[time_col] >= cutoff]
                for _, row in df.head(100).iterrows():
                    news_items.append(
                        {
                            "title": row.get(title_col),
                            "content": row.get(content_col) if content_col else None,
                            "publish_time": row.get(time_col) if time_col else None,
                            "source": "akshare",
                        }
                    )
    except Exception as exc:
        LOGGER.warning("AkShare 新闻获取失败: %s err=%s", stock_code, exc)

    if news_items:
        return news_items

    # tushare 作为补充（第三方环境可能不可用）
    try:
        fetcher = DataFetcher()
        if fetcher.pro is not None:
            df = fetcher.pro.news(start_date=cutoff.strftime("%Y-%m-%d %H:%M:%S"))
            if isinstance(df, pd.DataFrame) and not df.empty:
                for _, row in df.head(100).iterrows():
                    title = row.get("title")
                    if title:
                        news_items.append(
                            {
                                "title": title,
                                "content": row.get("content"),
                                "publish_time": row.get("datetime"),
                                "source": "tushare",
                            }
                        )
    except Exception as exc:
        LOGGER.warning("Tushare 新闻获取失败: %s err=%s", stock_code, exc)

    return news_items


def get_market_sentiment() -> dict:
    default = {
        "up_count": 0,
        "down_count": 0,
        "limit_up_count": 0,
        "limit_down_count": 0,
        "northbound_net_flow": 0.0,
        "margin_balance_change": None,
        "market_sentiment_score": 0.0,
    }
    try:
        import akshare as ak

        spot = ak.stock_zh_a_spot()
        if isinstance(spot, pd.DataFrame) and not spot.empty:
            chg_col = "涨跌幅" if "涨跌幅" in spot.columns else None
            if chg_col:
                chg = pd.to_numeric(spot[chg_col], errors="coerce")
                up_count = int((chg > 0).sum())
                down_count = int((chg < 0).sum())
                limit_up_count = int((chg >= 9.5).sum())
                limit_down_count = int((chg <= -9.5).sum())
                default.update(
                    {
                        "up_count": up_count,
                        "down_count": down_count,
                        "limit_up_count": limit_up_count,
                        "limit_down_count": limit_down_count,
                    }
                )
                total = max(1, up_count + down_count)
                default["market_sentiment_score"] = round((up_count - down_count) / total, 4)

        north = ak.stock_hsgt_north_net_flow_in_em()
        if isinstance(north, pd.DataFrame) and not north.empty:
            col = next((c for c in ["value", "当日净流入", "净流入"] if c in north.columns), None)
            if col:
                v = pd.to_numeric(north[col], errors="coerce").dropna()
                if not v.empty:
                    default["northbound_net_flow"] = float(v.iloc[-1])

        # 融资融券余额变化（可选，接口可能受版本影响）
        try:
            margin_df = ak.stock_margin_sse(start_date=(datetime.now() - timedelta(days=7)).strftime("%Y%m%d"))
            if isinstance(margin_df, pd.DataFrame) and not margin_df.empty:
                bal_col = next((c for c in ["融资余额", "rzye"] if c in margin_df.columns), None)
                if bal_col:
                    bal = pd.to_numeric(margin_df[bal_col], errors="coerce").dropna()
                    if len(bal) >= 2:
                        default["margin_balance_change"] = float(bal.iloc[-1] - bal.iloc[-2])
        except Exception:
            pass
    except Exception as exc:
        LOGGER.warning("市场情绪获取失败: %s", exc)

    if (
        CONFIG.get("tickflow_api_key")
        and CONFIG.get("tickflow_use_universe_market_stats", True)
        and default.get("up_count", 0) == 0
        and default.get("down_count", 0) == 0
    ):
        default = enrich_market_stats_from_universe(default)

    return default

