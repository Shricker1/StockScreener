"""
综合排序模块
-----------
对已通过 Stage Two 的股票执行：
1) CAN SLIM 评分
2) 安全边际评分
并按配置权重汇总总分后降序输出。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from config import CONFIG
from filters.can_slim import score_can_slim
from filters.safety_margin import score_safety_margin


DEFAULT_RANK_WEIGHTS = {
    "can_slim": 0.7,
    "safety_margin": 0.3,
}


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.ranker")
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


def _normalize_weights(raw: dict[str, float]) -> dict[str, float]:
    total = sum(max(0.0, float(v)) for v in raw.values())
    if total <= 0:
        return DEFAULT_RANK_WEIGHTS.copy()
    return {k: max(0.0, float(v)) / total for k, v in raw.items()}


def _get_rank_weights() -> dict[str, float]:
    """
    读取综合排序权重配置：
    - CONFIG["ranking_weights"]["can_slim"]
    - CONFIG["ranking_weights"]["safety_margin"]
    未配置则使用默认值。
    """
    cfg = CONFIG.get("ranking_weights", {}) or {}
    merged = DEFAULT_RANK_WEIGHTS.copy()
    for key in merged:
        if key in cfg:
            merged[key] = float(cfg[key])
    return _normalize_weights(merged)


def _extract_stock_code(item: Any) -> str:
    """兼容字符串、dict、pd.Series 三种输入格式提取股票代码。"""
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, pd.Series):
        item = item.to_dict()
    if isinstance(item, dict):
        for key in ("stock_code", "symbol", "code", "ts_code"):
            value = item.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
    return ""


def final_ranking(passed_stocks: list[Any]) -> pd.DataFrame:
    """
    对通过第二阶段筛选的股票进行最终排序。

    参数:
    - passed_stocks: 股票列表（如 ["000001", "600519"]）

    返回:
    - 按 total_score 降序排序的 DataFrame
    """
    if not passed_stocks:
        return pd.DataFrame(
            columns=[
                "stock_code",
                "can_slim_score",
                "safety_margin_score",
                "total_score",
                "can_slim_passed",
            ]
        )

    weights = _get_rank_weights()
    rows: list[dict[str, Any]] = []

    for item in passed_stocks:
        stock_code = _extract_stock_code(item)
        if not stock_code:
            LOGGER.warning("跳过无效股票项: %s", item)
            continue

        try:
            can_slim_result = score_can_slim(stock_code)
            safety_result = score_safety_margin(stock_code)

            can_slim_score = float(can_slim_result.get("total_score", 0.0))
            # 安全边际模块是 0~2 分，这里映射到 0~100 再做加权，量纲统一。
            safety_raw = float(safety_result.get("score", 0.0))
            safety_score = max(0.0, min(100.0, safety_raw / 2.0 * 100.0))

            total_score = (
                can_slim_score * weights["can_slim"]
                + safety_score * weights["safety_margin"]
            )

            row = {
                "stock_code": stock_code,
                "can_slim_score": round(can_slim_score, 2),
                "safety_margin_score": round(safety_score, 2),
                "total_score": round(total_score, 2),
                "can_slim_passed": bool(can_slim_result.get("passed", False)),
                "pe_percentile": safety_result.get("pe_percentile"),
                "pb_percentile": safety_result.get("pb_percentile"),
                "peg": safety_result.get("peg"),
            }
            rows.append(row)
            LOGGER.info(
                "完成综合打分: %s | can_slim=%.2f, safety=%.2f, total=%.2f",
                stock_code,
                can_slim_score,
                safety_score,
                total_score,
            )
        except Exception as exc:
            LOGGER.exception("综合排序失败: %s, err=%s", stock_code, exc)

    if not rows:
        return pd.DataFrame(
            columns=[
                "stock_code",
                "can_slim_score",
                "safety_margin_score",
                "total_score",
                "can_slim_passed",
            ]
        )

    result_df = pd.DataFrame(rows)
    result_df = result_df.sort_values("total_score", ascending=False).reset_index(
        drop=True
    )
    return result_df
