"""Multi-role debate orchestrator for stock analysis."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ai.prompt import get_analyst_prompt, get_judge_prompt, get_veteran_prompt
from ai.token_utils import truncate_text_by_tokens
from config import CONFIG


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.debate")
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


class DebateOrchestrator:
    def __init__(self, ai_client):
        self.ai_client = ai_client

    def _safe_json(self, text: str) -> dict[str, Any]:
        try:
            return json.loads(text)
        except Exception:
            try:
                s = text.find("{")
                e = text.rfind("}")
                if s != -1 and e != -1 and e > s:
                    return json.loads(text[s : e + 1])
            except Exception:
                pass
        return {
            "final_suggestion": "规避",
            "final_reasons": ["裁判解析失败"],
            "key_divergence": "N/A",
            "winner": "tie",
            "confidence": 0,
        }

    def run_debate(self, stock_code: str, report_markdown: str) -> dict:
        analyst_opinion = ""
        veteran_opinion = ""
        debate_record = ""
        judge_result: dict[str, Any] = {}
        try:
            analyst = get_analyst_prompt()
            veteran = get_veteran_prompt()
            judge = get_judge_prompt()

            analyst_msg = analyst["user_template"].format(report=report_markdown)
            veteran_msg = veteran["user_template"].format(report=report_markdown)

            analyst_opinion = self.ai_client.analyze_with_custom_prompt(
                analyst["system"], analyst_msg
            )
            veteran_opinion = self.ai_client.analyze_with_custom_prompt(
                veteran["system"], veteran_msg
            )

            # 裁判阶段控制辩论上下文长度：优先保留双方观点前 500 字核心内容。
            analyst_for_judge = analyst_opinion[:500]
            veteran_for_judge = veteran_opinion[:500]
            debate_record = (
                f"[系统策略分析师]\n{analyst_for_judge}\n\n"
                f"[老股民]\n{veteran_for_judge}"
            )
            debate_record = truncate_text_by_tokens(debate_record, 3000)
            judge_msg = judge["user_template"].format(
                stock_code=stock_code, debate_record=debate_record, report=report_markdown
            )
            judge_raw = self.ai_client.analyze_with_custom_prompt(judge["system"], judge_msg)
            judge_result = self._safe_json(judge_raw)
        except Exception as exc:
            LOGGER.warning("辩论流程失败: %s err=%s", stock_code, exc)
            judge_result = {
                "final_suggestion": "规避",
                "final_reasons": [str(exc)],
                "key_divergence": "流程异常",
                "winner": "tie",
                "confidence": 0,
            }
        return {
            "stock_code": stock_code,
            "analyst_opinion": analyst_opinion,
            "veteran_opinion": veteran_opinion,
            "debate_record": debate_record,
            "judge_result": judge_result,
        }

