"""AI-assisted factor interpretation agent."""

from __future__ import annotations

import json
from typing import Any

from ai.client import AIClient
from config import CONFIG


def get_factor_suggestion(ic_map: dict[str, float], current_weights: dict[str, float] | None = None) -> dict:
    """让 AI 根据因子效果给出解释与建议权重。"""
    client = AIClient()
    current_weights = current_weights or {}
    system_prompt = (
        "你是量化因子研究员。根据给定的因子IC结果，输出JSON："
        '{"summary":"...","suggested_weights":{"因子A":0.3},"risk_notes":["..."]}'
    )
    user_message = (
        f"因子IC结果：{json.dumps(ic_map, ensure_ascii=False)}\n"
        f"当前权重：{json.dumps(current_weights, ensure_ascii=False)}\n"
        "请给出动态权重建议。"
    )
    raw = client.analyze_with_custom_prompt(system_prompt, user_message)
    try:
        return json.loads(raw)
    except Exception:
        return {"summary": raw, "suggested_weights": {}, "risk_notes": []}


def get_factor_insights(factor_performance_df) -> dict[str, Any]:
    """
    基于因子绩效表生成 AI 可读解释与建议权重。
    期望返回：
    {
      "factors": {
        "rsi": {"interpretation":"...", "recommended_weight":0.1, "confidence":7}
      },
      "meta_summary": "..."
    }
    """
    client = AIClient()
    records = factor_performance_df.to_dict(orient="records") if factor_performance_df is not None else []
    system_prompt = (
        "你是资深量化研究员。请根据输入的因子绩效数据输出严格JSON，结构为："
        '{"factors":{"因子名":{"interpretation":"说明","recommended_weight":0.0,"confidence":1}},'
        '"meta_summary":"总评"}。recommended_weight在0到1之间。'
    )
    user_message = f"因子绩效数据：{json.dumps(records, ensure_ascii=False)}"
    raw = client.analyze_with_custom_prompt(system_prompt, user_message)
    try:
        data = json.loads(raw)
        if "factors" not in data:
            return {"factors": {}, "meta_summary": raw}
        return data
    except Exception:
        return {"factors": {}, "meta_summary": raw}


def blend_recommended_weights(
    ai_recommended: dict[str, float], alpha: float = 0.5
) -> dict[str, float]:
    """
    将 AI 推荐权重与现有 CAN SLIM 权重融合，得到最终权重（归一化）。
    alpha: AI 权重占比，(1-alpha) 为基础权重占比。
    """
    base = CONFIG.get("can_slim_weights", {}) or {}
    keys = sorted(set(base.keys()) | set(ai_recommended.keys()))
    merged: dict[str, float] = {}
    for k in keys:
        b = float(base.get(k, 0) or 0)
        a = float(ai_recommended.get(k, 0) or 0)
        merged[k] = (1 - alpha) * b + alpha * a
    total = sum(v for v in merged.values() if v > 0)
    if total <= 0:
        n = max(1, len(merged))
        return {k: 1.0 / n for k in merged}
    return {k: v / total for k, v in merged.items()}

