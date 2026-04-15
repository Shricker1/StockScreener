"""
数据清洗与字段标准化
-----------------
将 AkShare / Tushare 返回的行情列统一为：
date, open, high, low, close, volume
"""

from __future__ import annotations

import pandas as pd


TARGET_COLUMNS = ["date", "open", "high", "low", "close", "volume"]


def standardize_columns(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    按数据源将原始列名映射为统一列名。

    参数:
    - df: 原始 DataFrame
    - source: 数据源类型，支持 "akshare" / "tushare"（不区分大小写）

    返回:
    - 重命名后的 DataFrame（仅重命名，不做类型转换）
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    source = (source or "").lower().strip()
    renamed = df.copy()

    # AkShare 常见列名（中文）与目标列名映射
    # 兼容部分接口可能出现的英文列名。
    if source == "akshare":
        column_map = {
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "vol": "volume",
        }
    elif source == "tushare":
        column_map = {
            "trade_date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "vol": "volume",
            "volume": "volume",
        }
    else:
        # 若 source 未知，尽量用通用映射兜底
        column_map = {
            "日期": "date",
            "trade_date": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "vol": "volume",
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }

    renamed = renamed.rename(columns=column_map)
    return renamed


def clean_daily_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗并标准化日线数据（要求输入已通过 standardize_columns 重命名）。

    清洗步骤：
    1) 保留目标字段 date/open/high/low/close/volume
    2) 转换 date 为 datetime
    3) 转换数值列为数值类型（非法值转 NaN）
    4) 删除关键字段缺失行
    5) 按日期升序排序并重置索引
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    cleaned = df.copy()

    # 只保留目标字段；若缺失则补空列，保证输出结构稳定。
    for col in TARGET_COLUMNS:
        if col not in cleaned.columns:
            cleaned[col] = pd.NA
    cleaned = cleaned[TARGET_COLUMNS]

    # 日期字段清洗
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")

    # 数值字段清洗
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

    # 删除无法参与后续分析的关键缺失行
    cleaned = cleaned.dropna(subset=["date", "open", "high", "low", "close"])

    # 按日期升序，便于后续技术指标计算
    cleaned = cleaned.sort_values("date").reset_index(drop=True)

    return cleaned
