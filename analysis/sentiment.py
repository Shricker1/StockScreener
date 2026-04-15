"""Simple news sentiment analysis utilities."""

from __future__ import annotations

from typing import Any


POSITIVE_WORDS = ["上涨", "利好", "突破", "增长", "盈利", "买入", "增持"]
NEGATIVE_WORDS = ["下跌", "利空", "亏损", "减持", "风险", "卖出", "下滑"]


def analyze_news_sentiment(news_list: list[dict[str, Any]]) -> dict[str, Any]:
    """
    基于词典的情感分析（默认）。
    若未来启用 transformers，可在此函数内部扩展并保留回退逻辑。
    """
    # 可选模型路径：若依赖存在且初始化成功，可替代词典方案。
    try:
        from transformers import pipeline  # type: ignore

        clf = pipeline("sentiment-analysis")
        pos = neg = neu = 0
        for item in news_list or []:
            text = str(item.get("title", "") or item.get("content", ""))
            if not text:
                neu += 1
                continue
            out = clf(text[:512])[0]
            label = str(out.get("label", "")).lower()
            score = float(out.get("score", 0))
            if "pos" in label and score >= 0.55:
                pos += 1
            elif "neg" in label and score >= 0.55:
                neg += 1
            else:
                neu += 1
        total = max(1, pos + neg + neu)
        return {
            "positive_count": pos,
            "negative_count": neg,
            "neutral_count": neu,
            "sentiment_score": round(max(-1.0, min(1.0, (pos - neg) / total)), 4),
        }
    except Exception:
        pass

    positive_count = 0
    negative_count = 0
    neutral_count = 0

    for item in news_list or []:
        text = f"{item.get('title', '')} {item.get('content', '')}"
        pos = sum(1 for w in POSITIVE_WORDS if w in text)
        neg = sum(1 for w in NEGATIVE_WORDS if w in text)
        if pos > neg:
            positive_count += 1
        elif neg > pos:
            negative_count += 1
        else:
            neutral_count += 1

    total = max(1, positive_count + negative_count + neutral_count)
    sentiment_score = (positive_count - negative_count) / total
    return {
        "positive_count": positive_count,
        "negative_count": negative_count,
        "neutral_count": neutral_count,
        "sentiment_score": round(max(-1.0, min(1.0, sentiment_score)), 4),
    }

