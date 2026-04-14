"""Analysis page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data.fetcher import DataFetcher
from ui.components import (
    display_debate_result,
    display_metrics_chart,
    display_report_card,
    display_sentiment_gauge,
)


def render_analysis_page(result: dict | None) -> None:
    st.subheader("选股分析")
    if not result:
        st.info("请在左侧选择股票并点击运行。")
        return

    st.success(f"分析完成：{result.get('code')} {result.get('name', '')}")
    display_report_card(result.get("report_markdown", ""))
    if result.get("judge_result") or result.get("debate_record"):
        display_debate_result(result)
    if result.get("chanlun_report_md") or result.get("chanlun_summary"):
        with st.expander("缠论分析（独立模块，不参与辩论）", expanded=False):
            if result.get("chanlun_summary"):
                st.markdown("**缠论要点 / AI 解读**")
                st.write(result.get("chanlun_summary", ""))
            if result.get("chanlun_ai_summary"):
                st.markdown("**缠论专项 AI 摘要**")
                st.write(result.get("chanlun_ai_summary", ""))
            if result.get("chanlun_plot_path"):
                try:
                    st.image(str(result["chanlun_plot_path"]), use_container_width=True)
                except Exception:
                    st.caption("缠论图表路径无效或文件不存在。")
            if result.get("chanlun_report_md"):
                st.markdown("**完整缠论 Markdown 报告**")
                st.markdown(result.get("chanlun_report_md", ""))

    if result.get("tech_suggestion"):
        with st.expander("技术面顾问独立观点", expanded=True):
            st.markdown(f"- 建议：**{result.get('tech_suggestion', 'N/A')}**")
            st.markdown(f"- 置信度：**{result.get('tech_confidence', 'N/A')}**")
            st.markdown(f"- 关键观察：{result.get('tech_observation', 'N/A')}")
            reasons = result.get("tech_reasons", [])
            if isinstance(reasons, list) and reasons:
                st.markdown("**理由：**")
                for item in reasons:
                    st.markdown(f"- {item}")
            if result.get("tech_report_md"):
                st.markdown("**技术面报告输入：**")
                st.markdown(result.get("tech_report_md", ""))

    raw_data = result.get("raw_data", {}) if isinstance(result, dict) else {}
    display_sentiment_gauge(float(raw_data.get("news_sentiment", 0) or 0))

    code = str(result.get("code", ""))
    try:
        fetcher = DataFetcher()
        df = fetcher.get_daily(code, "20240101", "20300101")
    except Exception:
        df = pd.DataFrame()
    display_metrics_chart(df if isinstance(df, pd.DataFrame) else pd.DataFrame())

