"""Build markdown report from collected stock data."""

from __future__ import annotations


def _ok(flag: bool) -> str:
    return "✅" if flag else "❌"


def build_markdown_report(data_dict: dict, compact: bool = False) -> str:
    d = {k: ("N/A" if v is None else v) for k, v in data_dict.items()}
    status1 = _ok(d.get("close") != "N/A" and d.get("ma150") != "N/A" and d["close"] > d["ma150"] and d["close"] > d.get("ma200", 0))
    status2 = _ok(d.get("ma150") != "N/A" and d.get("ma200") != "N/A" and d["ma150"] > d["ma200"])
    status3 = _ok(d.get("ma200_change") != "N/A" and d["ma200_change"] > 0)
    status4 = _ok(d.get("rs_trend_6w") == "上升")
    stage_two_result = "通过" if all([status1 == "✅", status2 == "✅", status3 == "✅"]) else "未通过"
    sentiment_val = d.get("news_sentiment")
    if sentiment_val == "N/A":
        sentiment_desc = "N/A"
    else:
        try:
            sentiment_num = float(sentiment_val)
            if sentiment_num > 0.1:
                sentiment_desc = f"{sentiment_num}（偏正面）"
            elif sentiment_num < -0.1:
                sentiment_desc = f"{sentiment_num}（偏负面）"
            else:
                sentiment_desc = f"{sentiment_num}（中性）"
        except Exception:
            sentiment_desc = str(sentiment_val)
    if compact:
        return f"""# 股票分析报告：{d.get('name', 'N/A')}（{d.get('code', 'N/A')}）

## 核心结论
- 第二阶段：{stage_two_result}
- 价格/均线：{d.get('close')} / {d.get('ma50')},{d.get('ma150')},{d.get('ma200')}
- EPS同比：{d.get('eps_q_yoy')}%，ROE：{d.get('roe')}%
- PE/PB/PEG：{d.get('pe')} / {d.get('pb')} / {d.get('peg')}
- 新闻情绪：{sentiment_desc}
- 市场涨跌比：{d.get('market_up_down_ratio')}，北向净流入：{d.get('northbound_net_flow')} 亿元
"""

    return f"""# 股票分析报告：{d.get('name', 'N/A')}（{d.get('code', 'N/A')}）

## 一、基本信息
- 所属行业：{d.get('industry', 'N/A')}
- 总市值：{d.get('total_mv', 'N/A')} 亿元
- 流通市值：{d.get('circ_mv', 'N/A')} 亿元

## 二、趋势状态（第二阶段评估）
| 指标 | 数值 | 状态 |
|------|------|------|
| 股价 vs MA150/200 | {d.get('close')} vs {d.get('ma150')}/{d.get('ma200')} | {status1} |
| MA150 vs MA200 | {d.get('ma150')} vs {d.get('ma200')} | {status2} |
| 200日线趋势 | 近20日变化 {d.get('ma200_change')}% | {status3} |
| 距52周高点 | {d.get('pct_from_high')}% | - |
| 距52周低点 | {d.get('pct_from_low')}% | - |
| RS线趋势 | 近6周变化 | {status4} |
| **综合结论** | **{stage_two_result}** | |

## 三、CAN SLIM 数据
- 当季EPS同比增速：{d.get('eps_q_yoy')}%
- 当季营收同比增速：{d.get('revenue_q_yoy')}%
- 过去3年EPS复合增速：{d.get('eps_cagr_3y')}%
- 最新ROE：{d.get('roe')}%
- 近期新闻关键词：{d.get('news_keywords')}
- 量比(20日)：{d.get('vol_ratio')}
- 股东户数变化：{d.get('holder_change_rate')}
- RSI(14)：{d.get('rsi')}
- 近6月涨幅 vs 行业：{d.get('stock_return_6m')}% vs {d.get('industry_return_6m')}%
- 机构持股比例：{d.get('inst_holding')}（变化：{d.get('inst_change')}）
- 北向持股比例：{d.get('northbound_holding')}（变化：{d.get('northbound_change')}）

## 四、估值数据
- PE(TTM)：{d.get('pe')}（历史分位：{d.get('pe_percentile')}%）
- PB：{d.get('pb')}（历史分位：{d.get('pb_percentile')}%）
- PEG：{d.get('peg')}

## 五、技术形态
- 最新K线形态：{d.get('candle_pattern')}
- MACD状态：{d.get('macd_status')}

## 六、市场情绪与新闻舆情
- 近期新闻标题：{d.get('news_headlines')}
- 个股新闻情感倾向：{sentiment_desc}
- 全市场涨跌比：{d.get('market_up_down_ratio')}
- 北向资金净流入：{d.get('northbound_net_flow')} 亿元
"""


def build_tech_position_report(data: dict) -> str:
    """构建筹码与技术面独立报告（供技术顾问 AI 使用）。"""
    d = {k: ("N/A" if v is None else v) for k, v in data.items()}
    k_table = d.get("recent_k_table_md", "N/A")
    if not isinstance(k_table, str) or not k_table.strip():
        k_table = "N/A"
    return f"""# 筹码与技术面报告：{d.get('name', 'N/A')}（{d.get('code', 'N/A')}）

## 一、筹码分布
| 指标 | 数值 |
|------|------|
| 获利比例 | {d.get('chip_profit_ratio', 'N/A')} |
| 平均成本 | {d.get('chip_avg_cost', 'N/A')} |
| 筹码集中度 | {d.get('chip_concentration', 'N/A')} |

## 二、委比与日内情绪
| 指标 | 数值 |
|------|------|
| 委比 | {d.get('bid_ask_ratio', 'N/A')} |
| 今日涨幅 | {d.get('daily_pct_change', 'N/A')} |
| 今日振幅 | {d.get('daily_amplitude', 'N/A')} |

## 三、近期K线形态
形态描述：{d.get('recent_k_pattern', 'N/A')}

近10日OHLCV：
{k_table}

## 四、综合技术面观察
（留空，由AI填充）
"""

