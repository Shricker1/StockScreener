"""Token estimation and truncation utilities for AI requests."""

from __future__ import annotations

import re


def estimate_tokens(text: str) -> int:
    """
    估算 token 数量（近似）：
    - 中文按 1 token ≈ 1.5 字符
    - 英文按 1 token ≈ 4 字符
    """
    if not text:
        return 0
    zh_chars = re.findall(r"[\u4e00-\u9fff]", text)
    en_chars = re.findall(r"[A-Za-z0-9]", text)
    other_chars = max(0, len(text) - len(zh_chars) - len(en_chars))
    zh_tokens = len(zh_chars) / 1.5
    en_tokens = len(en_chars) / 4.0
    other_tokens = other_chars / 2.0
    return int(zh_tokens + en_tokens + other_tokens) + 1


def truncate_text_by_tokens(text: str, max_tokens: int) -> str:
    """
    通用截断函数：按估算 token 上限裁剪文本。
    使用二分查找，避免线性退化。
    """
    if not text:
        return text
    if max_tokens <= 0:
        return ""
    if estimate_tokens(text) <= max_tokens:
        return text

    lo, hi = 0, len(text)
    best = ""
    while lo <= hi:
        mid = (lo + hi) // 2
        candidate = text[:mid]
        if estimate_tokens(candidate) <= max_tokens:
            best = candidate
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def truncate_report(report: str, max_tokens: int) -> str:
    """
    智能截断 Markdown 报告：
    - 优先保留：趋势状态、CAN SLIM 核心、估值
    - 压缩：长新闻与长段落描述
    """
    if not report:
        return report
    if estimate_tokens(report) <= max_tokens:
        return report

    lines = report.splitlines()
    keep_priority = []
    secondary = []
    for line in lines:
        if (
            "二、趋势状态" in line
            or "三、CAN SLIM" in line
            or "四、估值数据" in line
            or "综合结论" in line
            or "PE(TTM)" in line
            or "PB：" in line
            or "PEG：" in line
        ):
            keep_priority.append(line)
        elif "近期新闻标题" in line:
            # 新闻只保留前 120 字，避免挤占上下文
            secondary.append(line[:120] + ("..." if len(line) > 120 else ""))
        else:
            secondary.append(line)

    compact = "\n".join(keep_priority + [""] + secondary)
    if estimate_tokens(compact) <= max_tokens:
        return compact
    return truncate_text_by_tokens(compact, max_tokens)

