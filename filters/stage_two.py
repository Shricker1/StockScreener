"""
Stage Two 趋势模板筛选
----------------------
核心思想参考 Mark Minervini 的 Stage 2 趋势模板：
通过多条均线与 52 周价格位置条件，识别处于中长期强势趋势的股票。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from analysis.indicators import sma
from config import CONFIG
from data.cleaner import clean_daily_data, standardize_columns
from data.fetcher import DataFetcher


def _build_logger() -> logging.Logger:
    """构建模块日志器，复用 config 中的日志路径配置。"""
    logger = logging.getLogger("stock_screener.stage_two")
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


def check_stage_two(df: pd.DataFrame) -> dict:
    """
    判断单只股票是否满足 Stage Two 条件。

    参数:
    - df: 已清洗的日线数据，至少包含 close 列，建议已按日期升序。

    返回:
    - {
        "passed": bool,      # 是否通过全部条件
        "reasons": list[str] # 每条标准的判断结果与依据
      }

    判断标准（常见 Stage 2 版本）：
    1) 当前价 > MA150 且 > MA200
    2) MA150 > MA200（中长期多头排列）
    3) MA200 在最近约 1 个月上升（这里用 20 个交易日近似）
    4) MA50 > MA150 且 > MA200（短中长期强势）
    5) 当前价 > MA50（价格位于短期均线之上）
    6) 当前价 >= 52 周最低价的 1.25 倍（已明显脱离低位）
    7) 当前价 >= 52 周最高价的 0.75 倍（接近 52 周高点）
    """
    reasons: list[str] = []

    if df is None or df.empty:
        return {"passed": False, "reasons": ["数据为空，无法执行 Stage Two 判断"]}

    if "close" not in df.columns:
        return {"passed": False, "reasons": ["缺少 close 列，无法计算均线"]}

    work = df.copy()
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    work = work.dropna(subset=["close"]).reset_index(drop=True)

    # 为保证 MA200 与 52 周统计稳定，至少建议 250 个交易日数据。
    if len(work) < 250:
        return {
            "passed": False,
            "reasons": [f"数据不足：当前仅 {len(work)} 条，建议至少 250 条日线数据"],
        }

    work["ma50"] = sma(work, period=50)
    work["ma150"] = sma(work, period=150)
    work["ma200"] = sma(work, period=200)

    last = work.iloc[-1]
    close = float(last["close"])
    ma50 = float(last["ma50"])
    ma150 = float(last["ma150"])
    ma200 = float(last["ma200"])

    # 52 周近似为最近 252 个交易日。
    window_52w = work.tail(252)
    low_52w = float(window_52w["close"].min())
    high_52w = float(window_52w["close"].max())

    # MA200 上升判定：当前 MA200 > 20 个交易日前 MA200。
    ma200_20d_ago = work["ma200"].iloc[-21] if len(work) >= 221 else float("nan")

    checks: list[tuple[bool, str, str]] = []
    checks.append(
        (
            close > ma150 and close > ma200,
            "当前价位于 MA150 与 MA200 之上",
            f"close={close:.2f}, ma150={ma150:.2f}, ma200={ma200:.2f}",
        )
    )
    checks.append(
        (
            ma150 > ma200,
            "MA150 高于 MA200（中长期多头结构）",
            f"ma150={ma150:.2f}, ma200={ma200:.2f}",
        )
    )
    checks.append(
        (
            pd.notna(ma200_20d_ago) and ma200 > float(ma200_20d_ago),
            "MA200 最近约 20 个交易日保持上行",
            f"ma200_now={ma200:.2f}, ma200_20d_ago={float(ma200_20d_ago):.2f}",
        )
    )
    checks.append(
        (
            ma50 > ma150 and ma50 > ma200,
            "MA50 位于 MA150 与 MA200 之上",
            f"ma50={ma50:.2f}, ma150={ma150:.2f}, ma200={ma200:.2f}",
        )
    )
    checks.append(
        (
            close > ma50,
            "当前价位于 MA50 之上",
            f"close={close:.2f}, ma50={ma50:.2f}",
        )
    )
    checks.append(
        (
            close >= low_52w * 1.25,
            "当前价至少高于 52 周低点 25%",
            f"close={close:.2f}, low_52w={low_52w:.2f}, threshold={low_52w*1.25:.2f}",
        )
    )
    checks.append(
        (
            close >= high_52w * 0.75,
            "当前价位于 52 周高点的 75% 以上（接近高位）",
            f"close={close:.2f}, high_52w={high_52w:.2f}, threshold={high_52w*0.75:.2f}",
        )
    )

    passed = True
    for ok, rule, detail in checks:
        prefix = "PASS" if ok else "FAIL"
        reasons.append(f"[{prefix}] {rule} | {detail}")
        if not ok:
            passed = False

    return {"passed": passed, "reasons": reasons}


def _extract_symbol(stock_item: Any) -> str:
    """
    从多种输入格式中提取股票代码，兼容：
    - "000001"（字符串）
    - {"symbol": "000001"} / {"code": "000001"} / {"ts_code": "000001.SZ"}（字典）
    - pandas.Series（来自 DataFrame 行）
    """
    if isinstance(stock_item, str):
        return stock_item.strip()

    if isinstance(stock_item, pd.Series):
        stock_item = stock_item.to_dict()

    if isinstance(stock_item, dict):
        for key in ("symbol", "code", "ts_code"):
            value = stock_item.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()

    return ""


def screen_stage_two(stock_list: list[Any]) -> list[str]:
    """
    批量执行 Stage Two 筛选，返回通过筛选的股票代码列表。

    参数:
    - stock_list: 股票列表，支持字符串代码列表或字典列表。

    返回:
    - 通过 Stage Two 的股票代码列表（list[str]）
    """
    if not stock_list:
        LOGGER.warning("stock_list 为空，跳过 Stage Two 批量筛选")
        return []

    fetcher = DataFetcher()
    source = str(CONFIG.get("data_source", "akshare")).lower()
    passed_symbols: list[str] = []

    # 拉取约 500 天自然日数据，通常可覆盖 250+ 交易日，满足 MA200/52周判断。
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=500)).strftime("%Y%m%d")

    for item in stock_list:
        symbol = _extract_symbol(item)
        if not symbol:
            LOGGER.warning("跳过无效股票项: %s", item)
            continue

        try:
            raw_df = fetcher.get_daily(symbol=symbol, start_date=start_date, end_date=end_date)
            std_df = standardize_columns(raw_df, source=source)
            clean_df = clean_daily_data(std_df)

            result = check_stage_two(clean_df)
            if result["passed"]:
                passed_symbols.append(symbol)
                LOGGER.info("Stage Two 通过: %s", symbol)
            else:
                LOGGER.info("Stage Two 未通过: %s | %s", symbol, " ; ".join(result["reasons"]))
        except Exception as exc:
            LOGGER.exception("筛选 %s 时发生异常: %s", symbol, exc)

    return passed_symbols
