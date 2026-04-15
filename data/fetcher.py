"""
数据拉取模块
-----------
根据 config.py 中的 data_source 自动切换 AkShare / Tushare。
对外统一提供：
1) get_all_stock_list()
2) get_daily()
3) get_financial()
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from config import AKSHARE_API_URL, CONFIG, DATA_SOURCE, TUSHARE_API_URL, TUSHARE_TOKEN


def _build_logger() -> logging.Logger:
    """
    构建模块级 logger（文件 + 控制台）。
    - 文件路径读取自 CONFIG["output"]["log_file"]
    - 若日志目录不存在则自动创建
    """
    logger = logging.getLogger("stock_screener.fetcher")
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


def get_hs300_stocks(config: Optional[dict] = None) -> list[str]:
    """
    获取沪深300成分股代码列表（纯数字格式）。
    优先 Tushare（与全项目数据源优先级一致），失败时回退 AkShare。
    """
    cfg = config or CONFIG

    # 1) Tushare 路径（优先）
    try:
        fetcher = DataFetcher(config=cfg)
        if fetcher.pro is not None:
            df = fetcher.pro.index_weight(index_code="000300.SH")
            if isinstance(df, pd.DataFrame) and not df.empty and "con_code" in df.columns:
                codes = df["con_code"].dropna().astype(str).str.split(".").str[0].tolist()
                codes = [c for c in codes if c]
                if codes:
                    LOGGER.info("Tushare 获取沪深300成功: %s", len(codes))
                    return list(dict.fromkeys(codes))
    except Exception as exc:
        LOGGER.warning("Tushare 获取沪深300失败: %s", exc)

    # 2) AkShare 路径
    try:
        import akshare as ak

        for func_name in ("index_stock_cons_csindex", "index_stock_cons_weight_csindex", "index_stock_cons"):
            try:
                func = getattr(ak, func_name, None)
                if func is None:
                    continue
                df = func(symbol="000300")
                if isinstance(df, pd.DataFrame) and not df.empty:
                    for col in ("成分券代码", "证券代码", "品种代码", "code", "symbol"):
                        if col in df.columns:
                            codes = (
                                df[col].dropna().astype(str).str.strip().str.split(".").str[0].tolist()
                            )
                            codes = [c for c in codes if c]
                            if codes:
                                LOGGER.info("AkShare 获取沪深300成功: %s", len(codes))
                                return list(dict.fromkeys(codes))
            except Exception:
                continue
    except Exception:
        pass

    LOGGER.warning("沪深300成分股获取失败，返回空列表")
    return []


class DataFetcher:
    """统一数据拉取入口，支持按优先级自动回退。

    支持的数据源（可通过 .env 的 DATA_SOURCE_PRIORITY 配置顺序；默认 tushare 最先）：
    - tushare
    - akshare_em（AkShare: stock_zh_a_hist / stock_zh_a_spot_em）
    - akshare_sina（AkShare: stock_zh_a_daily）
    - tencent（AkShare: stock_zh_a_hist_tx）
    - efinance（可选，若安装 efinance）
    """

    def __init__(self, config: Optional[dict] = None) -> None:
        self.config = config or CONFIG
        # 兼容旧配置：data_source 仍可用，但新逻辑优先走 data_source_priority
        self.data_source = str(self.config.get("data_source", DATA_SOURCE)).lower()
        self.data_source_priority = self._parse_priority(
            self.config.get("data_source_priority")
        )
        self.ts_token = self.config.get("tushare_token", TUSHARE_TOKEN)
        self.ts_api_url = self.config.get("tushare_api_url", TUSHARE_API_URL)
        self.ak_api_url = self.config.get("akshare_api_url", AKSHARE_API_URL)
        self.pro = None
        self.ak = None
        self.available = True
        self._init_client()

    def _parse_priority(self, raw: object) -> list[str]:
        if raw is None:
            raw = ""
        if isinstance(raw, (list, tuple)):
            items = [str(x).strip().lower() for x in raw]
        else:
            items = [x.strip().lower() for x in str(raw).split(",")]
        items = [x for x in items if x]
        if not items:
            # 若未配置优先级，则回退旧模式
            return [self.data_source]
        # 去重但保持顺序
        seen: set[str] = set()
        out: list[str] = []
        for x in items:
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    def _init_client(self) -> None:
        """初始化可用客户端（按需加载，不强依赖）。"""
        # AkShare 初始化（用于 tencent / akshare_sina / akshare_em）
        try:
            if self.ak_api_url:
                import os

                os.environ["AKSHARE_API_URL"] = str(self.ak_api_url).strip()
                LOGGER.info("检测到 AKSHARE_API_URL，已注入运行环境")
            import akshare as ak

            self.ak = ak
        except Exception as exc:
            self.ak = None
            LOGGER.warning("AkShare 初始化失败（将跳过相关数据源）: %s", exc)

        # Tushare 初始化（仅在存在 token 时启用）
        if self.ts_token and self.ts_token != "YOUR_TUSHARE_TOKEN":
            try:
                import tushare as ts

                ts.set_token(self.ts_token)
                self.pro = ts.pro_api()
                if self.ts_api_url:
                    self.pro._DataApi__http_url = self.ts_api_url
            except Exception as exc:
                self.pro = None
                LOGGER.warning("Tushare 初始化失败（将跳过 tushare 数据源）: %s", exc)
        else:
            self.pro = None

        # 只要至少一个路径可用，就认为 available
        self.available = bool(self.ak is not None or self.pro is not None)
        if not self.available:
            LOGGER.error("未初始化任何可用数据源客户端（AkShare/Tushare均不可用）")
        else:
            LOGGER.info(
                "数据源优先级=%s | tushare=%s | akshare=%s",
                ",".join(self.data_source_priority),
                "on" if self.pro is not None else "off",
                "on" if self.ak is not None else "off",
            )

    def _format_ak_sina_symbol(self, code: str) -> str:
        """Sina 需要 sh600519 / sz000001 / sh000300 形式。"""
        code = str(code).strip()
        if code.lower().startswith(("sh", "sz")):
            return code.lower()
        if "." in code:
            code = code.split(".")[0]
        if code in {
            "000300",
            "000016",
            "000905",
            "000852",
            "000688",
            "000932",
            "000933",
            "000903",
        }:
            return f"sh{code}"
        return f"sh{code}" if code.startswith("6") else f"sz{code}"

    def _format_ak_tx_symbol(self, code: str) -> str:
        """Tencent 需要 sh000300 / sz000001 形式。"""
        return self._format_ak_sina_symbol(code)

    def get_all_stock_list(self) -> pd.DataFrame:
        """
        获取全市场股票列表。
        将按 data_source_priority 顺序尝试（默认先 tushare）：
        - tushare: pro.stock_basic()
        - akshare_em: ak.stock_zh_a_spot_em() / ak.stock_zh_a_spot()
        """
        if not self.available:
            LOGGER.error("数据源不可用，无法获取股票列表")
            return pd.DataFrame()

        last_err: Exception | None = None
        for source in self.data_source_priority:
            try:
                if source == "tushare":
                    if self.pro is None:
                        continue
                    df = self.pro.stock_basic(
                        exchange="",
                        list_status="L",
                        fields="ts_code,symbol,name,area,industry,list_date",
                    )
                    if not df.empty and "ts_code" in df.columns:
                        df["code"] = df["ts_code"].astype(str).str.split(".").str[0]
                    LOGGER.info("股票列表拉取成功 source=tushare rows=%s", len(df))
                    return df

                if source in {"akshare_em", "akshare"}:
                    if self.ak is None:
                        continue
                    df = pd.DataFrame()
                    for fn in ("stock_zh_a_spot_em", "stock_zh_a_spot"):
                        func = getattr(self.ak, fn, None)
                        if func is None:
                            continue
                        try:
                            df = func()
                            if isinstance(df, pd.DataFrame) and not df.empty:
                                break
                        except Exception:
                            continue
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        if "代码" in df.columns:
                            df["code"] = df["代码"].astype(str)
                        if "名称" in df.columns:
                            df["name"] = df["名称"].astype(str)
                        LOGGER.info("股票列表拉取成功 source=%s rows=%s", source, len(df))
                        return df
            except Exception as exc:
                last_err = exc
                continue

        if last_err is not None:
            LOGGER.warning("获取股票列表失败（已尝试所有数据源）: %s", last_err)
        return pd.DataFrame()

    def get_daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "",
    ) -> pd.DataFrame:
        """
        获取日线行情数据。

        参数说明：
        - symbol: AkShare 使用股票代码（如 '000001'）；
                  Tushare 使用 ts_code（如 '000001.SZ'）
        - start_date/end_date:
            * AkShare: YYYYMMDD（例如 20240101）
            * Tushare: YYYYMMDD（例如 20240101）
        - adjust: 复权类型（仅 AkShare 使用，常见: '', 'qfq', 'hfq'）
        """
        if not self.available:
            LOGGER.error("数据源不可用，无法获取日线数据: symbol=%s", symbol)
            return pd.DataFrame()

        last_err: Exception | None = None
        for source in self.data_source_priority:
            try:
                if source == "tushare":
                    if self.pro is None:
                        continue
                    ts_code = self._format_ts_code(symbol)
                    df = pd.DataFrame()
                    try:
                        df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                    except Exception as exc:
                        LOGGER.warning("Tushare daily 失败 ts_code=%s: %s", ts_code, exc)
                    if df is None or df.empty:
                        try:
                            df = self.pro.index_daily(
                                ts_code=ts_code, start_date=start_date, end_date=end_date
                            )
                        except Exception:
                            df = pd.DataFrame()
                    if df is None or df.empty:
                        from data.tickflow_client import fetch_daily_klines

                        df = fetch_daily_klines(ts_code, start_date, end_date)
                    if df is None or df.empty:
                        continue
                    df = df.rename(columns={"trade_date": "date", "vol": "volume"})
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"], errors="coerce")
                        df = df.sort_values("date").reset_index(drop=True)
                    LOGGER.info("日线拉取成功 source=tushare symbol=%s rows=%s", ts_code, len(df))
                    return df

                if source == "tencent":
                    if self.ak is None:
                        continue
                    func = getattr(self.ak, "stock_zh_a_hist_tx", None)
                    if func is None:
                        continue
                    tx_symbol = self._format_ak_tx_symbol(symbol)
                    df = func(symbol=tx_symbol, start_date=start_date, end_date=end_date, adjust=adjust or "")
                    if df is None or df.empty:
                        continue
                    # 腾讯接口缺少 volume，这里补齐字段以兼容下游
                    if "volume" not in df.columns:
                        df["volume"] = pd.NA
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"], errors="coerce")
                        df = df.sort_values("date").reset_index(drop=True)
                    LOGGER.info("日线拉取成功 source=tencent symbol=%s rows=%s", tx_symbol, len(df))
                    return df

                if source in {"akshare_sina", "sina"}:
                    if self.ak is None:
                        continue
                    func = getattr(self.ak, "stock_zh_a_daily", None)
                    if func is None:
                        continue
                    sina_symbol = self._format_ak_sina_symbol(symbol)
                    df = func(symbol=sina_symbol, start_date=start_date, end_date=end_date, adjust=adjust or "")
                    if df is None or df.empty:
                        continue
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"], errors="coerce")
                        df = df.sort_values("date").reset_index(drop=True)
                    LOGGER.info("日线拉取成功 source=akshare_sina symbol=%s rows=%s", sina_symbol, len(df))
                    return df

                if source == "efinance":
                    try:
                        import efinance as ef  # type: ignore
                    except Exception:
                        continue
                    # efinance: 默认不复权；字段可能随版本变化，这里尽量兼容
                    code = str(symbol).strip()
                    if "." in code:
                        code = code.split(".")[0]
                    try:
                        df = ef.stock.get_quote_history(code, beg=start_date, end=end_date)  # type: ignore[attr-defined]
                    except Exception as exc:
                        last_err = exc
                        continue
                    if df is None or df.empty:
                        continue
                    # 尝试统一列名到 date/open/high/low/close/volume
                    rename_map = {
                        "日期": "date",
                        "开盘": "open",
                        "最高": "high",
                        "最低": "low",
                        "收盘": "close",
                        "成交量": "volume",
                    }
                    df = df.rename(columns=rename_map)
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"], errors="coerce")
                        df = df.sort_values("date").reset_index(drop=True)
                    LOGGER.info("日线拉取成功 source=efinance symbol=%s rows=%s", code, len(df))
                    return df

                if source in {"akshare_em", "akshare"}:
                    if self.ak is None:
                        continue
                    ak_symbol = self._format_ak_symbol(symbol)
                    func = getattr(self.ak, "stock_zh_a_hist", None)
                    if func is None:
                        continue
                    df = func(
                        symbol=ak_symbol,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust=adjust,
                    )
                    if df is None or df.empty:
                        continue
                    LOGGER.info("日线拉取成功 source=akshare_em symbol=%s rows=%s", ak_symbol, len(df))
                    return df
            except Exception as exc:
                last_err = exc
                continue

        if last_err is not None:
            LOGGER.warning(
                "获取日线数据失败（已尝试所有数据源）symbol=%s err=%s",
                symbol,
                last_err,
            )
        return pd.DataFrame()

    def get_financial(self, symbol: str, period: Optional[str] = None) -> pd.DataFrame:
        """
        获取财务分析指标。
        优先 Tushare（与 DATA_SOURCE_PRIORITY 一致）；失败时回退 AkShare。
        """
        if not self.available:
            LOGGER.error("数据源不可用，无法获取财务指标: symbol=%s", symbol)
            return pd.DataFrame()

        if self.pro is not None:
            try:
                kwargs = {"ts_code": self._format_ts_code(symbol)}
                if period:
                    kwargs["period"] = period
                df = self.pro.fina_indicator(**kwargs)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    LOGGER.info(
                        "Tushare 财务指标拉取成功，ts_code=%s, period=%s, rows=%s",
                        symbol,
                        period,
                        len(df),
                    )
                    return df
            except Exception as exc:
                LOGGER.warning("Tushare 财务指标失败，将尝试 AkShare: symbol=%s err=%s", symbol, exc)

        if self.ak is not None:
            try:
                ak_symbol = symbol
                if symbol and str(symbol).isdigit():
                    ak_symbol = self._format_ak_symbol(symbol)
                df = self.ak.stock_financial_analysis_indicator(symbol=ak_symbol)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    LOGGER.info("AkShare 财务指标拉取成功，symbol=%s, rows=%s", symbol, len(df))
                    return df
            except Exception as exc:
                LOGGER.exception(
                    "AkShare 财务指标失败，symbol=%s, period=%s, err=%s", symbol, period, exc
                )
        return pd.DataFrame()

    def _format_ts_code(self, code: str) -> str:
        """将纯数字代码转换为 Tushare 格式（如 600519 -> 600519.SH）。"""
        code = str(code).strip()
        if "." in code:
            return code
        # 常见上交所指数（Tushare index_daily 使用 .SH）
        if code in {
            "000300",
            "000016",
            "000905",
            "000852",
            "000688",
            "000932",
            "000933",
            "000903",
        }:
            return f"{code}.SH"
        if code.startswith("6"):
            return f"{code}.SH"
        return f"{code}.SZ"

    def _format_ak_symbol(self, code: str) -> str:
        """
        AkShare A 股历史接口通常使用 6 位代码（如 600519 / 000001）。
        若传入 ts_code 则自动截取前半部分。
        """
        code = str(code).strip()
        if "." in code:
            return code.split(".")[0]
        if code.lower().startswith(("sh", "sz")):
            return code[2:]
        return code
