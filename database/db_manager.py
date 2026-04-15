"""SQLite manager for analysis/factor persistence."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config import DATABASE_CONFIG


class DatabaseManager:
    """数据库管理器：提供建表、写入分析结果、读取因子数据、更新因子绩效。"""

    def __init__(self, sqlite_path: str | None = None) -> None:
        self.db_path = sqlite_path or DATABASE_CONFIG.get(
            "sqlite_path", "./output_files/stock_screener_ai.db"
        )
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def init_db(self) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT,
                    analysis_date TEXT NOT NULL,
                    ai_suggestion TEXT,
                    confidence REAL,
                    reasons TEXT,
                    report_markdown TEXT,
                    tech_suggestion TEXT,
                    tech_confidence INTEGER,
                    tech_reasons TEXT,
                    tech_observation TEXT,
                    forward_return_5d REAL,
                    forward_return_10d REAL,
                    forward_return_20d REAL,
                    created_at TEXT NOT NULL
                )
                """
            )
            self._migrate_analysis_history(cur)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS factor_values (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    analysis_date TEXT NOT NULL,
                    factor_name TEXT NOT NULL,
                    factor_value REAL,
                    forward_return_5d REAL,
                    forward_return_10d REAL,
                    forward_return_20d REAL,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS factor_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    factor_name TEXT NOT NULL UNIQUE,
                    ic_mean REAL,
                    ir REAL,
                    win_rate REAL,
                    stratification_json TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    @staticmethod
    def _migrate_analysis_history(cur: sqlite3.Cursor) -> None:
        """对既有库执行无损 ALTER TABLE 迁移。"""
        cur.execute("PRAGMA table_info(analysis_history)")
        cols = {row[1] for row in cur.fetchall()}
        expected = {
            "tech_suggestion": "TEXT",
            "tech_confidence": "INTEGER",
            "tech_reasons": "TEXT",
            "tech_observation": "TEXT",
            "chanlun_summary": "TEXT",
            "chanlun_plot_path": "TEXT",
            "chanlun_report_md": "TEXT",
        }
        for name, dtype in expected.items():
            if name not in cols:
                cur.execute(f"ALTER TABLE analysis_history ADD COLUMN {name} {dtype}")

    def save_analysis_result(self, data: dict[str, Any]) -> None:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        analysis_date = str(data.get("analysis_date", datetime.utcnow().strftime("%Y-%m-%d")))
        stock_code = str(data.get("code", "")).strip()
        if not stock_code:
            return

        factor_map = data.get("factor_values", {}) or {}
        f5 = data.get("forward_return_5d")
        f10 = data.get("forward_return_10d")
        f20 = data.get("forward_return_20d")

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO analysis_history (
                    stock_code, stock_name, analysis_date, ai_suggestion, confidence,
                    reasons, report_markdown, tech_suggestion, tech_confidence, tech_reasons, tech_observation,
                    forward_return_5d, forward_return_10d,
                    forward_return_20d, created_at,
                    chanlun_summary, chanlun_plot_path, chanlun_report_md
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stock_code,
                    data.get("name"),
                    analysis_date,
                    data.get("suggestion"),
                    float(data.get("confidence", 0) or 0),
                    json.dumps(data.get("reasons", []), ensure_ascii=False),
                    data.get("report_markdown", ""),
                    data.get("tech_suggestion"),
                    int(data.get("tech_confidence", 0) or 0),
                    json.dumps(data.get("tech_reasons", []), ensure_ascii=False),
                    data.get("tech_observation", ""),
                    f5,
                    f10,
                    f20,
                    now,
                    data.get("chanlun_summary"),
                    data.get("chanlun_plot_path"),
                    data.get("chanlun_report_md"),
                ),
            )
            for factor_name, factor_value in factor_map.items():
                cur.execute(
                    """
                    INSERT INTO factor_values (
                        stock_code, analysis_date, factor_name, factor_value,
                        forward_return_5d, forward_return_10d, forward_return_20d, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        stock_code,
                        analysis_date,
                        str(factor_name),
                        None if factor_value is None else float(factor_value),
                        f5,
                        f10,
                        f20,
                        now,
                    ),
                )
            conn.commit()

    def get_factor_data(self) -> pd.DataFrame:
        query = """
            SELECT
                fv.stock_code,
                fv.analysis_date,
                fv.factor_name,
                fv.factor_value,
                COALESCE(fv.forward_return_20d, ah.forward_return_20d) AS forward_return
            FROM factor_values fv
            LEFT JOIN analysis_history ah
              ON fv.stock_code = ah.stock_code AND fv.analysis_date = ah.analysis_date
            WHERE fv.factor_value IS NOT NULL
        """
        with self._connect() as conn:
            return pd.read_sql_query(query, conn)

    def update_factor_performance(self, factor_name: str, metrics: dict[str, Any]) -> None:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        strat_json = json.dumps(
            metrics.get("stratification", {}), ensure_ascii=False
        )
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO factor_performance (
                    factor_name, ic_mean, ir, win_rate, stratification_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(factor_name) DO UPDATE SET
                    ic_mean=excluded.ic_mean,
                    ir=excluded.ir,
                    win_rate=excluded.win_rate,
                    stratification_json=excluded.stratification_json,
                    updated_at=excluded.updated_at
                """,
                (
                    factor_name,
                    float(metrics.get("ic_mean", 0) or 0),
                    float(metrics.get("ir", 0) or 0),
                    float(metrics.get("win_rate", 0) or 0),
                    strat_json,
                    now,
                ),
            )
            conn.commit()

    def load_analysis_history(self, limit: int = 200) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(
                """
                SELECT id, stock_code AS code, stock_name AS name, analysis_date,
                       ai_suggestion AS suggestion, confidence, reasons,
                       tech_suggestion, tech_confidence, tech_reasons, tech_observation, created_at
                FROM analysis_history
                ORDER BY id DESC
                LIMIT ?
                """,
                conn,
                params=(int(limit),),
            )


# 兼容旧调用名称
DBManager = DatabaseManager

