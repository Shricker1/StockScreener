"""Reusable Streamlit UI components."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def display_report_card(report_md: str) -> None:
    st.markdown("### 分析报告")
    with st.container(border=True):
        st.markdown(report_md or "N/A")


def display_debate_result(debate_data: dict) -> None:
    st.markdown("### 辩论结果")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**系统策略分析师**")
        st.write(debate_data.get("analyst_opinion", "N/A"))
    with col2:
        st.markdown("**老股民**")
        st.write(debate_data.get("veteran_opinion", "N/A"))
    st.markdown("**裁判结论**")
    st.json(debate_data.get("judge_result", {}))


def display_metrics_chart(df: pd.DataFrame) -> None:
    st.markdown("### K线与指标")
    if df is None or df.empty:
        st.info("暂无可绘制数据")
        return
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=df.get("date"),
            open=df.get("open"),
            high=df.get("high"),
            low=df.get("low"),
            close=df.get("close"),
            name="K线",
        )
    )
    st.plotly_chart(fig, width="stretch")


def display_sentiment_gauge(score: float) -> None:
    st.markdown("### 情绪仪表盘")
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=float(score),
            gauge={"axis": {"range": [-1, 1]}},
            title={"text": "新闻情绪得分"},
        )
    )
    st.plotly_chart(fig, width="stretch")

