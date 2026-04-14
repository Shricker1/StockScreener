"""缠论 K 线可视化（依赖 chan.py 的 CPlotDriver）。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("stock_screener.chan_analysis.visualizer")


def save_chan_figure(
    chan: Any,
    save_path: Path,
    *,
    x_range: int = 200,
) -> str | None:
    """
    将缠论结构图保存为图片。失败时返回 None。
    """
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        from Plot.PlotDriver import CPlotDriver  # type: ignore
    except ImportError as exc:
        LOGGER.warning("无法导入 CPlotDriver: %s", exc)
        return None

    try:
        plot_driver = CPlotDriver(
            chan,
            plot_config=["kline", "bi", "seg", "zs", "bsp"],
            plot_para={
                "bi": {"show_num": True},
                "seg": {"color": "blue"},
                "zs": {"color": "yellow", "alpha": 0.3},
                "figure": {"w": 24, "h": 15, "x_range": x_range},
            },
        )
        plot_driver.save_img(str(save_path))
        return str(save_path.resolve())
    except Exception as exc:
        LOGGER.warning("缠论绘图失败: %s", exc)
        return None
