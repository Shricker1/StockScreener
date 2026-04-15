"""Factor mining page."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from factor_mining.ai_factor_agent import blend_recommended_weights, get_factor_insights
from factor_mining.evaluator import dynamic_weight_from_ic, evaluate_all_factors


def render_factor_mining_page() -> None:
    st.subheader("因子挖掘")
    st.caption("支持：数据库历史因子评估 + CSV 临时评估。")

    st.write("### A. 基于数据库历史记录")
    if st.button("运行全量因子评估"):
        perf_df = evaluate_all_factors()
        if perf_df.empty:
            st.info("数据库暂无可评估数据。先运行几次选股分析后再试。")
        else:
            st.dataframe(perf_df, width="stretch")
            fig_db = px.bar(perf_df, x="factor_name", y="ic_mean", title="因子 IC 均值")
            st.plotly_chart(fig_db, width="stretch")
            ai_ret = get_factor_insights(perf_df)
            st.write("#### AI 因子解释")
            st.json(ai_ret)
            ai_weights = {
                k: float(v.get("recommended_weight", 0) or 0)
                for k, v in (ai_ret.get("factors", {}) or {}).items()
                if isinstance(v, dict)
            }
            if ai_weights:
                merged = blend_recommended_weights(ai_weights, alpha=0.5)
                st.write("#### 融合后权重（AI + 既有权重）")
                st.dataframe(
                    pd.DataFrame({"factor": list(merged.keys()), "weight": list(merged.values())}),
                    width="stretch",
                )

    st.divider()
    st.write("### B. 上传 CSV 快速评估")
    file = st.file_uploader("上传CSV（需含 factor 与 future_return 列）", type=["csv"])
    if file is None:
        st.info("请先上传数据。")
        return

    df = pd.read_csv(file)
    if "future_return" not in df.columns:
        st.error("CSV 缺少 future_return 列。")
        return

    factor_cols = [c for c in df.columns if c != "future_return"]
    ic_map: dict[str, float] = {}
    for col in factor_cols:
        x = pd.to_numeric(df[col], errors="coerce")
        y = pd.to_numeric(df["future_return"], errors="coerce")
        valid = ~(x.isna() | y.isna())
        ic_map[col] = float(x[valid].corr(y[valid], method="spearman")) if valid.sum() > 5 else 0.0

    w = dynamic_weight_from_ic(ic_map)
    st.write("### IC 结果")
    st.dataframe(pd.DataFrame({"factor": list(ic_map.keys()), "ic": list(ic_map.values())}))
    st.write("### 动态权重")
    w_df = pd.DataFrame({"factor": list(w.keys()), "weight": list(w.values())})
    st.dataframe(w_df)
    fig = px.bar(w_df, x="factor", y="weight", title="动态权重")
    st.plotly_chart(fig, width="stretch")

    if st.button("生成 AI 因子建议"):
        ai_ret = get_factor_suggestion(ic_map, current_weights=w)
        st.json(ai_ret)

