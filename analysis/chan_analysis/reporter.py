"""缠论文本报告（Markdown）。"""

from __future__ import annotations

from typing import Any


def build_chan_markdown(
    stock_code: str,
    stock_name: str,
    structured: dict[str, Any] | None,
    *,
    error: str | None = None,
) -> str:
    if error and not structured:
        return f"""## 缠论技术分析报告 - {stock_name}({stock_code})

### 状态
- **计算未执行**：{error}

### 说明
安装开源缠论框架 [chan.py](https://github.com/Vespa314/chan.py) 后可自动计算笔、线段、中枢与买卖点（需 **Python 3.11+**）：

```text
pip install git+https://github.com/Vespa314/chan.py
```

安装完成后重新运行分析即可。
"""

    s = structured or {}
    bi = s.get("bi_count", "N/A")
    seg = s.get("seg_count", "N/A")
    zs = s.get("zs_count", "N/A")
    zh = s.get("last_zs_high")
    zl = s.get("last_zs_low")
    bsp_list = s.get("bsp_recent") or []

    zs_line = "N/A"
    if zh is not None and zl is not None:
        zs_line = f"{zl} - {zh}"

    pos = "待结合当前价判断"
    bsp_lines = "\n".join(
        f"- {b.get('type', '?')} | 买={b.get('is_buy')} | 时间={b.get('time')}" for b in bsp_list[:5]
    ) or "- （无最近买卖点记录或接口未返回）"

    return f"""## 缠论技术分析报告 - {stock_name}({stock_code})

### 一、走势结构分析
- 日线级别已处理包含关系，识别 **笔 {bi} 根**，**线段 {seg} 段**，**中枢 {zs} 个**（来自 chan.py 静态计算）。
- 最近一个中枢区间（若可解析）：**{zs_line}**
- 当前价格与中枢相对位置：{pos}（请结合行情软件现价核对）。

### 二、买卖点信号（最近记录）
{bsp_lines}

### 三、操作建议参考
- 本报告为**自动化结构摘要**，不构成投资建议；请结合基本面与资金管理。
- 背驰、区间套等细节请在安装 chan.py 后查看图表与库内对象。

---
*若计算报错：{error or "无"}*
"""
