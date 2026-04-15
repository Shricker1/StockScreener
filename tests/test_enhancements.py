"""Basic enhancement checks for env/news/sentiment/ai config."""

from __future__ import annotations

from analysis.sentiment import analyze_news_sentiment
from config import AI_CONFIG, TUSHARE_API_URL
from data.news_fetcher import get_market_sentiment, get_stock_news


def test_env_loaded() -> None:
    assert TUSHARE_API_URL is not None
    assert isinstance(AI_CONFIG.get("provider"), str)


def test_news_fetcher_returns_list() -> None:
    data = get_stock_news("000001", days=7)
    assert isinstance(data, list)


def test_market_sentiment_returns_dict() -> None:
    sent = get_market_sentiment()
    assert isinstance(sent, dict)
    for k in ["up_count", "down_count", "northbound_net_flow"]:
        assert k in sent


def test_sentiment_scoring() -> None:
    news = [
        {"title": "公司业绩增长突破预期", "content": ""},
        {"title": "公司面临风险与下跌压力", "content": ""},
    ]
    result = analyze_news_sentiment(news)
    assert "sentiment_score" in result
    assert -1 <= result["sentiment_score"] <= 1

