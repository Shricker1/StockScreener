"""
缠论分析模块 ChanAnalysis（可插拔，不参与 AI 辩论流程）。

依赖（可选）: Vespa314/chan.py + Python 3.11+
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from config import CONFIG

from .calculator import compute_chan_struct
from .reporter import build_chan_markdown
from .visualizer import save_chan_figure

LOGGER = logging.getLogger("stock_screener.chan_analysis")


class ChanAnalyzer:
    """对外统一入口。"""

    @staticmethod
    def run_analysis(
        stock_code: str,
        stock_name: str = "N/A",
        *,
        enable_ai: bool | None = None,
    ) -> dict[str, Any]:
        """
        返回:
        - report_md: Markdown 报告
        - plot_path: 图片路径或 None
        - structured: 结构化摘要 dict
        - ai_chan_summary: AI 二次解读（可选）
        - error: 错误信息（非致命）
        """
        if not CONFIG.get("chanlun_enabled", True):
            return {
                "ok": False,
                "report_md": "",
                "plot_path": None,
                "structured": None,
                "ai_chan_summary": None,
                "error": "CHANLUN_ENABLED=false",
            }

        chan, structured, err = compute_chan_struct(stock_code)
        report_md = build_chan_markdown(stock_code, stock_name, structured, error=err)

        plot_path: str | None = None
        if chan is not None:
            img = Path(str(CONFIG.get("chanlun_plot_dir", "./output_files/chanlun_plots"))) / f"{stock_code}_chanlun.png"
            plot_path = save_chan_figure(chan, img)

        ai_summary: str | None = None
        _ai = bool(CONFIG.get("chanlun_ai_enabled", True)) if enable_ai is None else enable_ai
        if _ai and report_md:
            try:
                from ai.client import AIClient

                client = AIClient()
                prompt = (
                    "你是一名缠论助手。请根据以下缠论自动化报告，用中文向学习者简要说明"
                    "走势结构、中枢与买卖点要点。结论清晰，不超过 200 字。\n\n"
                    + report_md
                )
                ai_summary = client.analyze_with_custom_prompt(
                    system_prompt="你是缠论技术分析助手，回答简洁、风险提示充分。",
                    user_message=prompt,
                ).strip() or None
            except Exception as exc:
                LOGGER.warning("缠论 AI 解读失败: %s", exc)
                ai_summary = None

        return {
            "ok": err is None and chan is not None,
            "report_md": report_md,
            "plot_path": plot_path,
            "structured": structured,
            "ai_chan_summary": ai_summary,
            "error": err,
        }
