"""AI API client compatible with OpenAI-style chat completions."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import requests

from ai.prompt import build_user_message, get_default_system_prompt
from ai.token_utils import estimate_tokens, truncate_report, truncate_text_by_tokens
from config import AI_CONFIG, CONFIG


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.ai_client")
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


class AIClient:
    def __init__(self) -> None:
        # 当前默认走 OpenAI 兼容格式；通过 provider 分支预留不同厂商扩展点。
        self.provider = str(AI_CONFIG.get("provider", "deepseek")).lower()
        self.api_key = AI_CONFIG.get("api_key", "your-api-key")
        self.base_url = AI_CONFIG.get("base_url", "https://your-ai-endpoint/v1").rstrip("/")
        self.model = AI_CONFIG.get("model", "deepseek-reasoner")
        self.max_tokens = int(AI_CONFIG.get("max_tokens", 8000))
        self.temperature = float(AI_CONFIG.get("temperature", 0.3))
        self.max_context_tokens = int(AI_CONFIG.get("max_context_tokens", 40000))
        self.reserved_output_tokens = int(AI_CONFIG.get("reserved_output_tokens", 2000))
        self.token_safety_margin = int(AI_CONFIG.get("token_safety_margin", 200))
        self.max_input_tokens = int(
            AI_CONFIG.get(
                "max_input_tokens",
                self.max_context_tokens - self.reserved_output_tokens - self.token_safety_margin,
            )
        )
        self.token_count_method = str(AI_CONFIG.get("token_count_method", "estimate"))
        self.api_version = AI_CONFIG.get("api_version", "2024-02-15-preview")
        self.deployment_name = AI_CONFIG.get("deployment_name", self.model)

    def _chat_completions_url(self) -> str:
        """
        构造聊天接口 URL：
        - DeepSeek 兼容两种 base_url 写法（根域名或 /v1）；
        - 其他 OpenAI 兼容服务保持原逻辑。
        """
        base = self.base_url.rstrip("/")
        if self.provider == "deepseek":
            if not base.endswith("/v1"):
                base = f"{base}/v1"
        return f"{base}/chat/completions"

    def _validate_api_key(self) -> None:
        """在请求前做基础鉴权配置检查，避免无效重试。"""
        bad_values = {"", "your-api-key", "your-ai-api-key-here", None}
        if self.api_key in bad_values:
            raise ValueError("AI_API_KEY 未配置或为占位符，请检查 .env。")

    def _build_request(self, markdown_report: str) -> tuple[str, dict, dict]:
        messages = [
            {"role": "system", "content": get_default_system_prompt(self.provider)},
            {"role": "user", "content": build_user_message(markdown_report, self.provider)},
        ]
        headers = {"Content-Type": "application/json"}
        payload = {"model": self.model, "messages": messages, "max_tokens": self.max_tokens, "temperature": self.temperature}

        if self.provider in {"openai", "deepseek", "qwen", "zhipu"}:
            headers["Authorization"] = f"Bearer {self.api_key}"
            url = self._chat_completions_url()
            return url, headers, payload

        if self.provider == "azure":
            headers["api-key"] = self.api_key
            url = (
                f"{self.base_url}/openai/deployments/{self.deployment_name}/chat/completions"
                f"?api-version={self.api_version}"
            )
            payload.pop("model", None)
            return url, headers, payload

        headers["Authorization"] = f"Bearer {self.api_key}"
        url = self._chat_completions_url()
        return url, headers, payload

    def analyze_with_custom_prompt(self, system_prompt: str, user_message: str) -> str:
        """
        通用 AI 调用方法：
        - 允许外部传入自定义 system/user prompt
        - 返回 AI 原始文本回复
        - 内置重试机制（最多 3 次）
        """
        # 发送前做上下文 token 预算控制（system + user）
        request_budget = self.max_input_tokens
        request_budget = max(500, request_budget)
        system_tokens = estimate_tokens(system_prompt)
        user_tokens = estimate_tokens(user_message)
        total_tokens = system_tokens + user_tokens

        if total_tokens > request_budget:
            # 优先将用户消息中报告部分做智能截断；若仍超限则按通用截断继续压缩
            original_chars = len(user_message)
            keep_user_budget = max(100, request_budget - system_tokens)
            if "报告" in user_message:
                user_message = truncate_report(user_message, keep_user_budget)
            if estimate_tokens(user_message) > keep_user_budget:
                user_message = truncate_text_by_tokens(user_message, keep_user_budget)
            new_chars = len(user_message)
            LOGGER.info(
                "Token截断触发: total=%s > budget=%s, chars %s -> %s (%.2f%%)",
                total_tokens,
                request_budget,
                original_chars,
                new_chars,
                (1 - (new_chars / max(1, original_chars))) * 100,
            )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
        }

        headers = {"Content-Type": "application/json"}
        if self.provider == "azure":
            headers["api-key"] = self.api_key
            url = (
                f"{self.base_url}/openai/deployments/{self.deployment_name}/chat/completions"
                f"?api-version={self.api_version}"
            )
            payload.pop("model", None)
        else:
            headers["Authorization"] = f"Bearer {self.api_key}"
            url = self._chat_completions_url()

        for attempt in range(1, 4):
            try:
                self._validate_api_key()
                resp = requests.post(url, headers=headers, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                return str(data["choices"][0]["message"]["content"])
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "N/A"
                body = ""
                try:
                    body = (exc.response.text or "")[:300] if exc.response is not None else ""
                except Exception:
                    body = ""
                LOGGER.warning(
                    "AI 自定义请求失败 attempt=%s status=%s provider=%s model=%s base_url=%s err=%s body=%s",
                    attempt,
                    status,
                    self.provider,
                    self.model,
                    self.base_url,
                    exc,
                    body,
                )
                if status in (401, 403):
                    # 鉴权错误无需重试三次，直接返回
                    return ""
                time.sleep(1.5 * attempt)
            except Exception as exc:
                LOGGER.warning(
                    "AI 自定义请求失败 attempt=%s provider=%s model=%s base_url=%s err=%s",
                    attempt,
                    self.provider,
                    self.model,
                    self.base_url,
                    exc,
                )
                time.sleep(1.5 * attempt)
        return ""

    def analyze_report(self, markdown_report: str) -> dict:
        try:
            raw_text = self.analyze_with_custom_prompt(
                system_prompt=get_default_system_prompt(self.provider),
                user_message=build_user_message(markdown_report, self.provider),
            )
            if not raw_text:
                return {"suggestion": "error", "reasons": [], "confidence": 0}
            try:
                return json.loads(raw_text)
            except Exception:
                start = raw_text.find("{")
                end = raw_text.rfind("}")
                if start != -1 and end != -1 and end > start:
                    return json.loads(raw_text[start : end + 1])
        except Exception as exc:
            LOGGER.warning("analyze_report 解析失败: %s", exc)
        return {"suggestion": "error", "reasons": [], "confidence": 0}

