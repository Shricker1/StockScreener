"""History page."""

from __future__ import annotations

import streamlit as st

from database.db_manager import DBManager


def render_history_page() -> None:
    st.subheader("历史记录")
    db = DBManager()
    limit = st.slider("读取条数", min_value=20, max_value=500, value=200, step=20)
    df = db.load_analysis_history(limit=limit)
    if df.empty:
        st.info("暂无历史记录。")
        return
    st.dataframe(df, width="stretch")

