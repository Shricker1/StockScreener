"""筹码与技术面 AI 顾问（独立于辩论模式）。"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import requests

from config import CONFIG, TECH_ADVISOR_CONFIG


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.tech_advisor")
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


SYSTEM_PROMPT = (
    "你是一名专注筹码博弈与短线技术面的A股分析师，只根据提供的筹码分布、委比、日涨幅和K线形态数据发表独立观点。"
    "你不需要参考基本面，也不需要考虑大盘趋势。你的任务是从技术面和资金博弈角度判断该股短期（1-3个交易日）是否有交易机会。"
    "输出必须是JSON，包含字段：tech_suggestion（强烈关注/可观察/回避），tech_reasons（列表），tech_confidence（1-10），"
    "key_observation（最关键的观察点一句话）。"
)


def _safe_default() -> dict:
    return {
        "tech_suggestion": "可观察",
        "tech_reasons": [],
        "tech_confidence": 0,
        "key_observation": "技术面数据不足",
    }


def _chat_url(provider: str, base_url: str) -> str:
    base = base_url.rstrip("/")
    if provider == "deepseek" and not base.endswith("/v1"):
        base = f"{base}/v1"
    return f"{base}/chat/completions"


def analyze_tech_position(report_md: str) -> dict:
    """
    独立技术顾问 AI 调用。
    关键要求：不做任何截断，完整投喂 report_md。
    """
    provider = str(TECH_ADVISOR_CONFIG.get("provider", "deepseek")).lower()
    api_key = TECH_ADVISOR_CONFIG.get("api_key")
    base_url = str(TECH_ADVISOR_CONFIG.get("base_url", "https://your-ai-endpoint/v1"))
    model = str(TECH_ADVISOR_CONFIG.get("model", "deepseek-reasoner"))
    temperature = float(TECH_ADVISOR_CONFIG.get("temperature", 0.2))
    max_tokens_cfg = TECH_ADVISOR_CONFIG.get("max_tokens")
    max_tokens = int(max_tokens_cfg) if isinstance(max_tokens_cfg, int) and max_tokens_cfg > 0 else 4096

    if not api_key:
        LOGGER.warning("技术顾问AI未配置 API Key，跳过调用")
        return _safe_default()

    user_prompt = f"请阅读以下筹码与技术面报告，给出你的独立判断。\n{report_md}"
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    headers = {"Content-Type": "application/json"}

    if provider == "azure":
        headers["api-key"] = str(api_key)
        deployment_name = str(TECH_ADVISOR_CONFIG.get("deployment_name") or model)
        api_version = str(TECH_ADVISOR_CONFIG.get("api_version", "2024-02-15-preview"))
        url = (
            f"{base_url.rstrip('/')}/openai/deployments/{deployment_name}/chat/completions"
            f"?api-version={api_version}"
        )
        payload.pop("model", None)
    else:
        headers["Authorization"] = f"Bearer {api_key}"
        url = _chat_url(provider, base_url)

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=90)
        resp.raise_for_status()
        content = str(resp.json()["choices"][0]["message"]["content"])
        try:
            parsed = json.loads(content)
        except Exception:
            start = content.find("{")
            end = content.rfind("}")
            if start != -1 and end > start:
                parsed = json.loads(content[start : end + 1])
            else:
                return _safe_default()
        out = _safe_default()
        out["tech_suggestion"] = parsed.get("tech_suggestion", out["tech_suggestion"])
        out["tech_reasons"] = parsed.get("tech_reasons", out["tech_reasons"])
        out["tech_confidence"] = parsed.get("tech_confidence", out["tech_confidence"])
        out["key_observation"] = parsed.get("key_observation", out["key_observation"])
        if not isinstance(out["tech_reasons"], list):
            out["tech_reasons"] = [str(out["tech_reasons"])]
        return out
    except Exception as exc:
        LOGGER.warning("技术顾问AI调用失败 provider=%s model=%s err=%s", provider, model, exc)
        return _safe_default()

