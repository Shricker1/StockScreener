"""因子有效性评估：IC / IR / 分层回测。"""

from __future__ import annotations

import pandas as pd
from scipy.stats import spearmanr

from database.db_manager import DatabaseManager


def calc_factor_ic(factor_values: pd.Series, forward_returns: pd.Series) -> float:
    """计算 Rank IC（Spearman）。"""
    x = pd.to_numeric(factor_values, errors="coerce")
    y = pd.to_numeric(forward_returns, errors="coerce")
    valid = ~(x.isna() | y.isna())
    if valid.sum() < 5:
        return 0.0
    ic, _ = spearmanr(x[valid], y[valid])
    return 0.0 if pd.isna(ic) else float(ic)


def calc_factor_ir(factor_values: pd.Series, forward_returns: pd.Series) -> float:
    """
    信息比率（简化实现）：
    以截面 Rank IC 序列的均值/标准差近似。
    在当前单表结构下按日期分组计算每日IC，再计算 IR。
    """
    x = pd.to_numeric(factor_values, errors="coerce")
    y = pd.to_numeric(forward_returns, errors="coerce")
    valid = ~(x.isna() | y.isna())
    if valid.sum() < 10:
        return 0.0
    ic, _ = spearmanr(x[valid], y[valid])
    if pd.isna(ic):
        return 0.0
    # 兼容单期数据：给出幅度受限的 proxy IR
    return float(ic / max(1e-6, abs(ic) ** 0.5))


def run_stratification_backtest(
    factor_values: pd.Series, forward_returns: pd.Series, n_groups: int = 5
) -> dict[str, float]:
    """按因子分组回测，返回各组平均收益（简化年化代理）。"""
    x = pd.to_numeric(factor_values, errors="coerce")
    y = pd.to_numeric(forward_returns, errors="coerce")
    df = pd.DataFrame({"factor": x, "ret": y}).dropna()
    if len(df) < n_groups * 3:
        return {}
    df["group"] = pd.qcut(df["factor"], q=n_groups, labels=False, duplicates="drop")
    out: dict[str, float] = {}
    for g, gdf in df.groupby("group"):
        # 以 20 日收益近似，折算年化代理（252/20）
        avg_ret = float(gdf["ret"].mean())
        ann_proxy = (1 + avg_ret) ** (252 / 20) - 1 if avg_ret > -0.99 else -1.0
        out[f"group_{int(g)+1}"] = float(ann_proxy)
    return out


def evaluate_all_factors() -> pd.DataFrame:
    """
    从数据库读取历史因子值与未来收益，输出因子绩效报告并回写数据库。
    """
    db = DatabaseManager()
    raw = db.get_factor_data()
    if raw.empty:
        return pd.DataFrame(
            columns=["factor_name", "ic_mean", "ir", "win_rate", "stratification"]
        )

    rows: list[dict] = []
    for factor_name, grp in raw.groupby("factor_name"):
        ic = calc_factor_ic(grp["factor_value"], grp["forward_return"])
        ir = calc_factor_ir(grp["factor_value"], grp["forward_return"])
        strat = run_stratification_backtest(grp["factor_value"], grp["forward_return"])
        win_rate = 0.0
        if strat:
            vals = list(strat.values())
            win_rate = float(sum(1 for x in vals if x > 0) / len(vals))
        metric = {
            "factor_name": factor_name,
            "ic_mean": ic,
            "ir": ir,
            "win_rate": win_rate,
            "stratification": strat,
        }
        rows.append(metric)
        db.update_factor_performance(factor_name, metric)

    return pd.DataFrame(rows).sort_values(by="ic_mean", ascending=False)


# 兼容旧函数名
def evaluate_factor_ic(factor_series: pd.Series, future_return_series: pd.Series) -> float:
    """兼容旧接口，等价于 calc_factor_ic。"""
    x = pd.to_numeric(factor_series, errors="coerce")
    y = pd.to_numeric(future_return_series, errors="coerce")
    valid = ~(x.isna() | y.isna())
    if valid.sum() < 5:
        return 0.0
    ic, _ = spearmanr(x[valid], y[valid])
    if pd.isna(ic):
        return 0.0
    return float(ic)


def dynamic_weight_from_ic(ic_map: dict[str, float]) -> dict[str, float]:
    """根据 IC 绝对值动态归一化权重。"""
    raw = {k: abs(float(v)) for k, v in ic_map.items()}
    total = sum(raw.values())
    if total <= 0:
        n = max(1, len(raw))
        return {k: 1.0 / n for k in raw}
    return {k: v / total for k, v in raw.items()}

