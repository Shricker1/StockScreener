"""Streamlit web app entry for StockScreener AI."""

from __future__ import annotations

import logging

import streamlit as st

from config import AI_CONFIG, DEBATE_CONFIG, STOCK_LIST, UI_CONFIG
from main import analyze_stock
from ui.pages.analysis import render_analysis_page
from ui.pages.factor_mining import render_factor_mining_page
from ui.pages.history import render_history_page

LOGGER = logging.getLogger("stock_screener.streamlit")


def _default_stock_list() -> list[str]:
    if STOCK_LIST:
        return [str(x) for x in STOCK_LIST]
    return ["600519"]


def main() -> None:
    st.set_page_config(
        page_title=UI_CONFIG.get("app_title", "StockScreener AI"),
        page_icon=UI_CONFIG.get("app_icon", "📈"),
        layout=UI_CONFIG.get("layout", "wide"),
    )
    st.title("StockScreener AI 可视化平台")

    with st.sidebar:
        st.header("运行配置")
        default_codes = ",".join(_default_stock_list())
        codes_text = st.text_area("股票代码（逗号分隔）", value=default_codes, height=100)
        enable_debate = st.toggle("启用辩论模式", value=bool(DEBATE_CONFIG.get("enabled", False)))
        provider = st.selectbox("AI Provider", options=["deepseek", "openai", "qwen", "azure"], index=0)
        model = st.text_input("AI 模型", value=str(AI_CONFIG.get("model", "deepseek-reasoner")))
        run_btn = st.button("一键运行", type="primary")
        page = st.radio("页面", ["选股分析", "历史记录", "因子挖掘"], index=0)

    AI_CONFIG["provider"] = provider
    AI_CONFIG["model"] = model

    if "last_result" not in st.session_state:
        st.session_state["last_result"] = None
    if "results_map" not in st.session_state:
        st.session_state["results_map"] = {}
    if "run_errors" not in st.session_state:
        st.session_state["run_errors"] = {}
    if "selected_code" not in st.session_state:
        st.session_state["selected_code"] = ""

    if run_btn:
        # 每次重新运行都清空上一次结果，避免显示陈旧数据
        st.session_state["last_result"] = None
        st.session_state["results_map"] = {}
        st.session_state["run_errors"] = {}
        st.session_state["selected_code"] = ""

        codes = [x.strip() for x in codes_text.split(",") if x.strip()]
        if not codes:
            st.warning("请至少输入一个股票代码。")
        else:
            prog = st.progress(0.0, text="开始分析")
            success_count = 0
            for i, code in enumerate(codes, 1):
                prog.progress((i - 1) / len(codes), text=f"分析中: {code}")
                try:
                    result = analyze_stock(code, enable_debate=enable_debate)
                    st.session_state["results_map"][code] = result
                    st.session_state["last_result"] = result
                    success_count += 1
                except Exception as exc:
                    LOGGER.exception("分析失败 code=%s err=%s", code, exc)
                    st.session_state["run_errors"][code] = str(exc)
            prog.progress(1.0, text="全部完成")
            if success_count:
                first_code = next(iter(st.session_state["results_map"].keys()), "")
                st.session_state["selected_code"] = first_code
                st.success(f"分析流程完成：成功 {success_count} 只，失败 {len(st.session_state['run_errors'])} 只。")
            else:
                st.warning("本次分析全部失败，请检查数据源和 AI 配置。")

    if page == "选股分析":
        results_map = st.session_state.get("results_map", {})
        run_errors = st.session_state.get("run_errors", {})

        if results_map:
            st.caption(f"本次运行成功 {len(results_map)} 只；失败 {len(run_errors)} 只。")
            code_options = list(results_map.keys())
            selected_code = st.selectbox(
                "查看分析结果",
                options=code_options,
                index=max(0, code_options.index(st.session_state["selected_code"]))
                if st.session_state.get("selected_code") in code_options
                else 0,
            )
            st.session_state["selected_code"] = selected_code
            render_analysis_page(results_map.get(selected_code))

            if run_errors:
                with st.expander("失败代码明细", expanded=False):
                    for code, err in run_errors.items():
                        st.write(f"- `{code}`: {err}")
        else:
            render_analysis_page(None)
    elif page == "历史记录":
        render_history_page()
    else:
        render_factor_mining_page()


if __name__ == "__main__":
    main()

