"""
缠论计算（与项目 DataFetcher 日线完全对齐）：
- 使用项目 `DataFetcher.get_daily()` 拉取日线（遵循 DATA_SOURCE_PRIORITY 回退、清洗逻辑等）
- 将 OHLCV DataFrame 转换为 chan.py 所需的 CKLine_Unit 列表
- 通过 `CChan.trigger_load()` 注入数据，避免 chan.py 内置数据源造成口径差异

未安装 chan.py 或 Python 版本过低时返回 None，由上层生成说明性报告。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

LOGGER = logging.getLogger("stock_screener.chan_analysis.calculator")


def _date_range(days: int = 730) -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def compute_chan_struct(
    stock_code: str,
    *,
    lookback_days: int = 730,
) -> tuple[Any | None, dict[str, Any] | None, str | None]:
    """
    返回 (chan_obj 或 None, structured_json, error_message)。
    chan_obj 仅在成功时非空，供绘图模块使用。
    """
    begin, end = _date_range(lookback_days)
    try:
        from Chan import CChan  # type: ignore
        from ChanConfig import CChanConfig  # type: ignore
        from Common.CEnum import KL_TYPE  # type: ignore
        from Common.CTime import CTime  # type: ignore
        from Common.CEnum import DATA_FIELD  # type: ignore
        from KLine.KLine_Unit import CKLine_Unit  # type: ignore
    except ImportError as exc:
        return None, None, (
            f"未加载 chan.py 缠论库: {exc}。"
            "请使用 Python 3.11+ 并安装: pip install git+https://github.com/Vespa314/chan.py"
        )

    # 1) 使用项目 DataFetcher 拉取并清洗日线（完全对齐项目口径）
    try:
        from data.fetcher import DataFetcher
        from data.cleaner import clean_daily_data, standardize_columns

        fetcher = DataFetcher()
        raw = fetcher.get_daily(str(stock_code).strip(), begin.replace("-", ""), end.replace("-", ""))
        # 项目下游 standardize_columns 需要 source（用于列名映射）；这里用 config 里的 data_source
        try:
            from config import CONFIG

            source = str(CONFIG.get("data_source", "akshare"))
        except Exception:
            source = "akshare"
        std = standardize_columns(raw, source=source)
        df = clean_daily_data(std)
    except Exception as exc:
        return None, None, f"DataFetcher 日线获取/清洗失败: {exc}"

    if df is None or df.empty or len(df) < 50:
        return None, None, "DataFetcher 返回日线数据不足（<50根），无法计算缠论结构。"

    # 2) 转换为 chan.py K 线单元并注入（trigger_load）
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"], how="any")
    if df.empty or len(df) < 50:
        return None, None, "清洗后有效日线不足（<50根），无法计算缠论结构。"

    kl_units: list[Any] = []
    for _, row in df.iterrows():
        d = row["date"]
        # 日线用 00:00 时间即可（chan.py day级别按日期）
        t = CTime(int(d.year), int(d.month), int(d.day), 0, 0)
        item = {
            DATA_FIELD.FIELD_TIME: t,
            DATA_FIELD.FIELD_OPEN: float(row.get("open", 0.0) or 0.0),
            DATA_FIELD.FIELD_HIGH: float(row.get("high", 0.0) or 0.0),
            DATA_FIELD.FIELD_LOW: float(row.get("low", 0.0) or 0.0),
            DATA_FIELD.FIELD_CLOSE: float(row.get("close", 0.0) or 0.0),
            DATA_FIELD.FIELD_VOLUME: float(row.get("volume", 0.0) or 0.0),
            # 项目日线通常无成交额/换手率，缺失时按 0 处理，不影响形态结构计算
            DATA_FIELD.FIELD_TURNOVER: 0.0,
            DATA_FIELD.FIELD_TURNRATE: 0.0,
        }
        kl_units.append(CKLine_Unit(item))

    try:
        # trigger_step=True：不使用 chan.py 内置数据源，完全由我们注入 K 线
        config = CChanConfig({"trigger_step": True})
        chan = CChan(
            code=str(stock_code).strip(),
            begin_time=begin,
            end_time=end,
            lv_list=[KL_TYPE.K_DAY],
            config=config,
        )
        chan.trigger_load({KL_TYPE.K_DAY: kl_units})
        # trigger_step 模式下不会自动计算 seg/zs，这里补上一次全量计算
        try:
            chan[0].cal_seg_and_zs()
        except Exception:
            # 兼容不同版本：从 kl_datas 调用
            try:
                chan.kl_datas[chan.lv_list[0]].cal_seg_and_zs()
            except Exception:
                pass
    except Exception as exc:
        LOGGER.warning("缠论 CChan 计算失败 code=%s err=%s", stock_code, exc)
        return None, None, str(exc)

    structured = extract_structure_json(chan)
    return chan, structured, None


def extract_structure_json(chan: Any) -> dict[str, Any]:
    """从 CChan 对象提取可序列化的摘要（具体属性随 chan.py 版本可能略有差异）。"""
    out: dict[str, Any] = {"levels": []}
    try:
        kl0 = chan[0]
    except Exception as exc:
        return {"error": f"无法读取 chan[0]: {exc}"}

    def _safe_len(obj: Any, name: str) -> int:
        try:
            x = getattr(kl0, name, None)
            if x is None:
                return 0
            return len(x) if hasattr(x, "__len__") else 0
        except Exception:
            return 0

    bi_n = _safe_len(kl0, "bi_list")
    seg_n = _safe_len(kl0, "seg_list")
    zs_n = _safe_len(kl0, "zs_list")

    out["bi_count"] = bi_n
    out["seg_count"] = seg_n
    out["zs_count"] = zs_n

    # 最近中枢高低点（若可访问）
    zs_high = zs_low = None
    try:
        zs_list = getattr(kl0, "zs_list", None) or getattr(kl0, "zs_lst", None)
        if zs_list and len(zs_list) > 0:
            last_zs = zs_list[-1]
            _h, _l = getattr(last_zs, "high", None), getattr(last_zs, "low", None)
            zs_high = float(_h) if _h is not None else None
            zs_low = float(_l) if _l is not None else None
    except Exception:
        pass
    out["last_zs_high"] = zs_high
    out["last_zs_low"] = zs_low

    # 买卖点（取最近若干）
    bsp_info: list[dict[str, Any]] = []
    try:
        latest = chan.get_latest_bsp(number=5)  # type: ignore[attr-defined]
        for p in latest or []:
            bsp_info.append(
                {
                    "type": str(getattr(p, "type", getattr(p, "bsp_type", "unknown"))),
                    "is_buy": bool(getattr(p, "is_buy", False)),
                    "time": str(getattr(p, "time", getattr(p, "kx", ""))),
                }
            )
    except Exception as exc:
        out["bsp_note"] = f"bsp_extract_failed: {exc}"

    out["bsp_recent"] = bsp_info
    return out
