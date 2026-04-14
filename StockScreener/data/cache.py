"""
本地缓存模块
-----------
提供 DataFrame 的缓存读写能力，默认缓存目录为 ./cache/
建议文件名使用：<股票代码>_<数据类型>
例如：000001_daily, 000001_financial
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd


CACHE_DIR = Path("./cache")

LOGGER = logging.getLogger("stock_screener.cache")


def _build_cache_path(filename: str) -> Path | None:
    """
    根据传入文件名生成缓存路径。
    - 自动去除首尾空格
    - 自动补全 .csv 后缀
    """
    safe_name = (filename or "").strip()
    if not safe_name:
        LOGGER.error("filename 不能为空，建议格式：<股票代码>_<数据类型>")
        return None

    if not safe_name.endswith(".csv"):
        safe_name = f"{safe_name}.csv"

    return CACHE_DIR / safe_name


def save_to_cache(df: pd.DataFrame, filename: str) -> Path:
    """
    将 DataFrame 保存到本地缓存目录 ./cache/

    参数:
    - df: 需要缓存的 DataFrame
    - filename: 文件名（建议：<股票代码>_<数据类型>）

    返回:
    - 实际保存路径（Path）
    """
    if df is None:
        LOGGER.error("缓存保存失败：df 不能为 None")
        return Path()

    cache_path = _build_cache_path(filename)
    if cache_path is None:
        return Path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_csv(cache_path, index=False, encoding="utf-8-sig")
        LOGGER.info("缓存保存成功: %s, rows=%s", cache_path, len(df))
        return cache_path
    except Exception as exc:
        LOGGER.exception("缓存保存失败: %s, err=%s", cache_path, exc)
        return Path()


def load_from_cache(filename: str) -> pd.DataFrame:
    """
    从本地缓存目录 ./cache/ 读取 DataFrame。

    参数:
    - filename: 文件名（建议：<股票代码>_<数据类型>）

    返回:
    - 读取成功返回 DataFrame
    - 文件不存在时返回空 DataFrame
    """
    cache_path = _build_cache_path(filename)
    if cache_path is None:
        return pd.DataFrame()

    if not cache_path.exists():
        LOGGER.warning("缓存文件不存在: %s", cache_path)
        return pd.DataFrame()

    try:
        df = pd.read_csv(cache_path)
        LOGGER.info("缓存读取成功: %s, rows=%s", cache_path, len(df))
        return df
    except Exception as exc:
        LOGGER.exception("缓存读取失败: %s, err=%s", cache_path, exc)
        return pd.DataFrame()
