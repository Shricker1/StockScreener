"""主程序入口：StageTwo -> 报告 -> AI 建议 -> Excel。"""

from __future__ import annotations

import logging
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from ai.client import AIClient
from ai.debate import DebateOrchestrator
from ai.tech_advisor import analyze_tech_position
from config import AI_CONFIG, CONFIG, DEBATE_CONFIG, STOCK_LIST
from database.db_manager import DatabaseManager
from data.fetcher import DataFetcher
from factor_mining.factor_library import extract_factor_values_from_report
from filters.stage_two import screen_stage_two
from output.excel_writer import write_ai_results_to_excel
from reports.data_collector import collect_stock_report_data
from reports.markdown_builder import build_markdown_report, build_tech_position_report


def setup_logging() -> logging.Logger:
    """初始化主程序日志器（文件 + 控制台）。"""
    logger = logging.getLogger("stock_screener.main")
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


LOGGER = setup_logging()


def _extract_code_column(df: pd.DataFrame) -> str | None:
    for col in ("symbol", "code", "ts_code"):
        if col in df.columns:
            return col
    return None


def _try_get_hs300_codes() -> list[str]:
    """
    尝试获取沪深300成分股代码，接口失败则返回空。
    注意：该函数仅用于测试股票池构建。
    """
    try:
        import akshare as ak

        hs300_df = pd.DataFrame()
        for func_name in ("index_stock_cons_csindex", "index_stock_cons_weight_csindex"):
            try:
                func = getattr(ak, func_name, None)
                if func is None:
                    continue
                hs300_df = func(symbol="000300")
                if isinstance(hs300_df, pd.DataFrame) and not hs300_df.empty:
                    break
            except Exception:
                continue

        if hs300_df.empty:
            return []

        for col in ("成分券代码", "证券代码", "code", "symbol"):
            if col in hs300_df.columns:
                codes = hs300_df[col].dropna().astype(str).str.strip().tolist()
                return [c for c in codes if c]
        return []
    except Exception:
        return []


def _preprocess_stock_pool(stock_df: pd.DataFrame) -> list[str]:
    """
    股票池预处理：
    1) 剔除 ST
    2) 剔除上市不满 1 年
    """
    if stock_df is None or stock_df.empty:
        return []

    df = stock_df.copy()
    code_col = _extract_code_column(df)
    if code_col is None:
        return []

    # 1) 剔除 ST
    if "name" in df.columns:
        name_series = df["name"].fillna("").astype(str).str.upper()
        st_mask = name_series.str.contains("ST", regex=False)
        before = len(df)
        df = df[~st_mask].copy()
        LOGGER.info("预处理-剔除ST: %s -> %s", before, len(df))

    # 2) 剔除上市不满 1 年
    if "list_date" in df.columns:
        listed = pd.to_datetime(df["list_date"].astype(str), format="%Y%m%d", errors="coerce")
        cutoff = pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=365)
        before = len(df)
        df = df[listed <= cutoff].copy()
        LOGGER.info("预处理-剔除上市<1年: %s -> %s", before, len(df))

    codes = df[code_col].dropna().astype(str).str.strip().tolist()
    return [c for c in codes if c]


def build_universe(fetcher: DataFetcher, use_hs300_test: bool = True) -> list[str]:
    """
    构建股票池：
    - use_hs300_test=True：优先沪深300成分股（便于测试）
    - 失败时回退为全市场股票列表
    """
    all_df = fetcher.get_all_stock_list()
    if all_df is None or all_df.empty:
        return []

    code_col = _extract_code_column(all_df)
    if code_col is None:
        return []

    base_df = all_df.copy()

    if use_hs300_test:
        hs300_codes = _try_get_hs300_codes()
        if hs300_codes:
            base_df = base_df[base_df[code_col].astype(str).isin(set(hs300_codes))].copy()
            LOGGER.info("使用沪深300测试股票池，原始数量=%s", len(base_df))
        else:
            LOGGER.warning("沪深300成分获取失败，回退全市场股票列表")
    else:
        LOGGER.info("使用全市场股票池，原始数量=%s", len(base_df))

    processed_codes = _preprocess_stock_pool(base_df)
    LOGGER.info("预处理后股票池数量=%s", len(processed_codes))
    return processed_codes


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="StockScreener AI pipeline")
    parser.add_argument("--limit", type=int, default=0, help="仅分析前N只股票（测试用）")
    parser.add_argument("--save-reports", action="store_true", help="保存每只股票的Markdown报告")
    return parser.parse_args()


def analyze_stock(stock_code: str, enable_debate: bool = True) -> dict[str, Any]:
    """
    单只股票分析主流程（供 UI/外部模块复用）：
    Stage2 数据收集 -> Markdown 报告 -> AI 建议/辩论。
    """
    ai_client = AIClient()
    data = collect_stock_report_data(stock_code)
    report_md = build_markdown_report(data)
    tech_report_md = build_tech_position_report(data)

    def _run_primary_ai() -> dict[str, Any]:
        if enable_debate:
            orchestrator = DebateOrchestrator(ai_client)
            debate = orchestrator.run_debate(stock_code, report_md)
            judge_result = debate.get("judge_result", {}) if isinstance(debate, dict) else {}
            return {
                "suggestion": judge_result.get("final_suggestion", "error"),
                "confidence": judge_result.get("confidence", 0),
                "reasons": judge_result.get("final_reasons", []),
                "analyst_opinion": debate.get("analyst_opinion", ""),
                "veteran_opinion": debate.get("veteran_opinion", ""),
                "debate_record": debate.get("debate_record", ""),
                "judge_result": judge_result,
            }
        ai_resp = ai_client.analyze_report(report_md)
        return {
            "suggestion": ai_resp.get("suggestion", "error"),
            "confidence": ai_resp.get("confidence", 0),
            "reasons": ai_resp.get("reasons", []),
        }

    # 与现有 AI 并行运行，互不干扰
    with ThreadPoolExecutor(max_workers=2) as executor:
        primary_future = executor.submit(_run_primary_ai)
        tech_future = executor.submit(analyze_tech_position, tech_report_md)
        primary = primary_future.result()
        tech_result = tech_future.result()

    result = {
        "code": stock_code,
        "name": data.get("name", "N/A"),
        "report_markdown": report_md,
        "raw_data": data,
        **primary,
        "tech_suggestion": tech_result.get("tech_suggestion", "可观察"),
        "tech_reasons": tech_result.get("tech_reasons", []),
        "tech_confidence": tech_result.get("tech_confidence", 0),
        "tech_observation": tech_result.get("key_observation", "N/A"),
        "tech_report_md": tech_report_md,
    }

    # 缠论分析（独立模块，不参与辩论）
    if CONFIG.get("chanlun_enabled", True):
        try:
            from analysis.chan_analysis import ChanAnalyzer

            cr = ChanAnalyzer.run_analysis(stock_code, str(data.get("name", "N/A")))
            result["chanlun_report_md"] = cr.get("report_md") or ""
            result["chanlun_plot_path"] = cr.get("plot_path")
            result["chanlun_structured"] = cr.get("structured")
            result["chanlun_ai_summary"] = cr.get("ai_chan_summary")
            _ai = cr.get("ai_chan_summary") or ""
            _rep = cr.get("report_md") or ""
            result["chanlun_summary"] = _ai[:2000] if _ai else _rep[:500]
        except Exception as exc:
            LOGGER.warning("缠论模块异常: %s", exc)
            result["chanlun_summary"] = str(exc)
            result["chanlun_report_md"] = ""
            result["chanlun_plot_path"] = None

    # 因子落库：未来收益先留空，后续可用定时任务回填。
    result["analysis_date"] = datetime.now().strftime("%Y-%m-%d")
    result["factor_values"] = extract_factor_values_from_report(data)
    result.setdefault("forward_return_5d", None)
    result.setdefault("forward_return_10d", None)
    result.setdefault("forward_return_20d", None)
    try:
        DatabaseManager().save_analysis_result(result)
    except Exception as exc:
        LOGGER.warning("分析结果写入数据库失败: %s err=%s", stock_code, exc)
    return result


def main() -> None:
    LOGGER.info("===== StockScreener 启动 =====")

    args = _parse_args()
    step_bar = tqdm(total=5, desc="主流程进度", unit="step")
    try:
        config = CONFIG
        LOGGER.info("配置加载完成，data_source=%s", config.get("data_source"))
        LOGGER.info(
            "AI Token配置: context=%s, reserved_output=%s, safety_margin=%s, method=%s",
            AI_CONFIG.get("max_context_tokens"),
            AI_CONFIG.get("reserved_output_tokens"),
            AI_CONFIG.get("token_safety_margin"),
            AI_CONFIG.get("token_count_method"),
        )
        step_bar.update(1)

        fetcher = DataFetcher(config=config)
        # 优先使用 .env 配置的股票列表；为空时回退原有全市场构建逻辑
        if STOCK_LIST:
            stock_list = STOCK_LIST.copy()
            LOGGER.info("股票来源: .env STOCK_LIST，数量=%s", len(stock_list))
        else:
            use_hs300_test = bool(config.get("use_hs300_test", True))
            stock_list = build_universe(fetcher, use_hs300_test=use_hs300_test)
            LOGGER.info("股票来源: 全市场/测试池预处理，数量=%s", len(stock_list))
        if args.limit and args.limit > 0:
            stock_list = stock_list[: args.limit]
            LOGGER.info("测试限制 --limit=%s, 实际股票数=%s", args.limit, len(stock_list))
        LOGGER.info("待筛选股票数量=%s", len(stock_list))
        step_bar.update(1)

        if not stock_list:
            LOGGER.error("股票池为空，流程终止")
            return

        passed_stocks = screen_stage_two(stock_list)
        LOGGER.info("Stage Two 通过数量=%s", len(passed_stocks))
        step_bar.update(1)

        if not passed_stocks:
            LOGGER.warning("没有股票通过 Stage Two，流程结束")
            return

        debate_enabled = bool(DEBATE_CONFIG.get("enabled", False))
        results_list: list[dict[str, Any]] = []
        reports_dir = Path(config.get("output", {}).get("output_dir", "output_files")) / "reports"
        if args.save_reports:
            reports_dir.mkdir(parents=True, exist_ok=True)

        for code in tqdm(passed_stocks, desc="AI分析", unit="stk"):
            try:
                result = analyze_stock(code, enable_debate=debate_enabled)
                report_md = result.get("report_markdown", "")
                if args.save_reports:
                    report_file = reports_dir / f"{code}.md"
                    report_file.write_text(report_md, encoding="utf-8")
                result["report_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                results_list.append(result)
            except Exception as exc:
                LOGGER.warning("单股AI流程失败: %s err=%s", code, exc)
                results_list.append(
                    {
                        "code": code,
                        "name": "N/A",
                        "suggestion": "error",
                        "confidence": 0,
                        "reasons": [str(exc)],
                        "report_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "report_markdown": "",
                    }
                )

        LOGGER.info("AI分析完成，结果数量=%s", len(results_list))
        step_bar.update(1)

        default_filename = f"ai_stock_suggestions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_file = write_ai_results_to_excel(results_list, default_filename, include_raw_reports=True)
        LOGGER.info("结果已写入: %s", output_file)
        step_bar.update(1)

        LOGGER.info("===== StockScreener 运行完成 =====")
    except Exception as exc:
        LOGGER.exception("主流程发生未捕获异常，已安全退出: %s", exc)
    finally:
        step_bar.close()


if __name__ == "__main__":
    main()
