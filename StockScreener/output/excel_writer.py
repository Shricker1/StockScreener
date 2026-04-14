"""
Excel 输出模块
-------------
提供 write_to_excel(df_results, filename)：
1) 综合排名
2) 详细指标
3) 淘汰记录
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config import CONFIG


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("stock_screener.excel_writer")
    if logger.handlers:
        return logger

    log_path = Path(CONFIG.get("output", {}).get("log_file", "logs/app.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


LOGGER = _build_logger()


def _ensure_dataframe(df_results: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df_results, pd.DataFrame):
        return df_results.copy()
    return pd.DataFrame(df_results)


def _build_summary_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """
    生成“综合排名”：
    目标包含：代码、名称、各项得分、总分
    """
    code_col = "stock_code" if "stock_code" in df.columns else "code" if "code" in df.columns else None
    name_col = "name" if "name" in df.columns else "stock_name" if "stock_name" in df.columns else None
    total_col = "total_score" if "total_score" in df.columns else None

    score_cols = [c for c in df.columns if "score" in c.lower()]

    cols = []
    if code_col:
        cols.append(code_col)
    if name_col and name_col not in cols:
        cols.append(name_col)
    for c in score_cols:
        if c not in cols:
            cols.append(c)
    if total_col and total_col not in cols:
        cols.append(total_col)

    if not cols:
        # 若字段极少，至少返回全部数据
        return df.copy()

    summary = df[cols].copy()
    if total_col and total_col in summary.columns:
        summary = summary.sort_values(total_col, ascending=False).reset_index(drop=True)
    return summary


def _build_detail_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """
    生成“详细指标”：
    默认直接保留全部列，便于追溯每只股票的原始指标与中间结果。
    """
    return df.copy()


def _build_reject_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """
    生成“淘汰记录”：
    规则优先级：
    1) 有 passed 列：passed=False 视为淘汰
    2) 有 stage_two_passed 列：False 视为淘汰
    3) 都没有：返回空表（仅列头）
    """
    reject_mask = None
    if "passed" in df.columns:
        reject_mask = ~df["passed"].fillna(False).astype(bool)
    elif "stage_two_passed" in df.columns:
        reject_mask = ~df["stage_two_passed"].fillna(False).astype(bool)

    if reject_mask is None:
        return pd.DataFrame(columns=["stock_code", "name", "reason"])

    rejected = df[reject_mask].copy()

    # 标准化输出列（如果存在）
    candidate_cols = [
        "stock_code",
        "code",
        "name",
        "stock_name",
        "reason",
        "reasons",
        "fail_reason",
        "failed_reason",
    ]
    cols = [c for c in candidate_cols if c in rejected.columns]
    if cols:
        rejected = rejected[cols]
    return rejected.reset_index(drop=True)


def write_to_excel(df_results: pd.DataFrame, filename: str) -> Path:
    """
    将结果写入 Excel，并生成三个 Sheet：
    1) 综合排名
    2) 详细指标
    3) 淘汰记录
    """
    df = _ensure_dataframe(df_results)

    output_dir = Path(CONFIG.get("output", {}).get("output_dir", "output_files"))
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = Path(filename)
    if not file_path.suffix:
        file_path = file_path.with_suffix(".xlsx")
    if not file_path.is_absolute():
        file_path = output_dir / file_path

    summary_df = _build_summary_sheet(df)
    detail_df = _build_detail_sheet(df)
    reject_df = _build_reject_sheet(df)

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="综合排名", index=False)
        detail_df.to_excel(writer, sheet_name="详细指标", index=False)
        reject_df.to_excel(writer, sheet_name="淘汰记录", index=False)

    LOGGER.info(
        "Excel 写入完成: %s | summary=%s, detail=%s, rejected=%s",
        file_path,
        len(summary_df),
        len(detail_df),
        len(reject_df),
    )
    return file_path


def write_ai_results_to_excel(
    results_list: list[dict],
    filename: str,
    include_raw_reports: bool = True,
    use_legacy: bool = False,
) -> Path:
    """
    新版 AI 输出：
    - AI建议汇总
    - 原始报告（可选）
    同时保留旧版写法：use_legacy=True 时退回 write_to_excel。
    """
    if use_legacy:
        return write_to_excel(pd.DataFrame(results_list), filename)

    output_dir = Path(CONFIG.get("output", {}).get("output_dir", "output_files"))
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = Path(filename)
    if not file_path.suffix:
        file_path = file_path.with_suffix(".xlsx")
    if not file_path.is_absolute():
        file_path = output_dir / file_path

    df = pd.DataFrame(results_list)
    if df.empty:
        df = pd.DataFrame(columns=["code", "name", "suggestion", "confidence", "reasons", "report_time"])

    if "reasons" in df.columns:
        df["reasons"] = df["reasons"].apply(
            lambda x: "; ".join(x) if isinstance(x, list) else (x if x is not None else "")
        )
    if "tech_reasons" in df.columns:
        df["tech_reasons"] = df["tech_reasons"].apply(
            lambda x: "; ".join(x) if isinstance(x, list) else (x if x is not None else "")
        )

    order = {"买入": 0, "观望": 1, "规避": 2, "error": 3}
    if "suggestion" in df.columns:
        df["_order"] = df["suggestion"].map(order).fillna(99)
    else:
        df["_order"] = 99
    if "confidence" in df.columns:
        df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0)

    # 若存在裁判结果，展开为扁平列，便于在汇总表展示
    has_debate = "judge_result" in df.columns or "debate_record" in df.columns
    if "judge_result" in df.columns:
        def _jr(x, key, default=None):
            if isinstance(x, dict):
                return x.get(key, default)
            return default
        df["裁判结论"] = df["judge_result"].apply(lambda x: _jr(x, "final_suggestion", "N/A"))
        df["核心分歧"] = df["judge_result"].apply(lambda x: _jr(x, "key_divergence", "N/A"))
        df["裁判置信度"] = df["judge_result"].apply(lambda x: _jr(x, "confidence", 0))
        df["裁判理由"] = df["judge_result"].apply(
            lambda x: "; ".join(_jr(x, "final_reasons", [])) if isinstance(_jr(x, "final_reasons", []), list) else _jr(x, "final_reasons", "")
        )

    summary_df = df.sort_values(["_order", "confidence"], ascending=[True, False]).drop(columns=["_order"])

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        summary_cols = [
            c
            for c in [
                "code",
                "name",
                "suggestion",
                "confidence",
                "reasons",
                "tech_suggestion",
                "tech_confidence",
                "tech_reasons",
                "tech_observation",
                "chanlun_summary",
                "chanlun_ai_summary",
                "chanlun_plot_path",
                "report_time",
            ]
            if c in summary_df.columns
        ]
        if has_debate:
            summary_cols += [c for c in ["analyst_opinion", "veteran_opinion", "裁判结论", "核心分歧", "裁判置信度"] if c in summary_df.columns]
        summary_df[summary_cols].to_excel(writer, sheet_name="AI建议汇总", index=False)
        if include_raw_reports:
            raw_cols = [c for c in ["code", "name", "report_markdown"] if c in summary_df.columns]
            if raw_cols:
                summary_df[raw_cols].to_excel(writer, sheet_name="原始报告", index=False)
        if has_debate and "debate_record" in summary_df.columns:
            debate_cols = [c for c in ["code", "name", "debate_record", "裁判结论", "裁判理由", "核心分歧", "裁判置信度"] if c in summary_df.columns]
            summary_df[debate_cols].to_excel(writer, sheet_name="完整辩论记录", index=False)

    LOGGER.info("AI结果写入完成: %s | rows=%s", file_path, len(summary_df))
    return file_path
