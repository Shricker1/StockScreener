"""
技术指标计算模块
---------------
优先使用 TA-Lib（若环境已安装），否则自动回退到 pandas 实现。

默认约定输入 DataFrame 至少包含以下列（小写）：
- close
- high（部分指标需要）
- low（部分指标需要）
- volume（部分指标需要）
"""

from __future__ import annotations

import numpy as np
import pandas as pd

try:
    import talib  # type: ignore

    HAS_TALIB = True
except Exception:  # pragma: no cover
    talib = None
    HAS_TALIB = False


def _candle_parts(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算 K 线基础结构，供形态识别函数复用。
    - body: 实体长度
    - upper_shadow: 上影线长度
    - lower_shadow: 下影线长度
    - range_: 当日振幅（最高-最低）
    """
    open_ = pd.to_numeric(df["open"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")

    body = (close - open_).abs()
    upper_shadow = high - pd.concat([open_, close], axis=1).max(axis=1)
    lower_shadow = pd.concat([open_, close], axis=1).min(axis=1) - low
    range_ = high - low
    return pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "body": body,
            "upper_shadow": upper_shadow,
            "lower_shadow": lower_shadow,
            "range_": range_,
        },
        index=df.index,
    )


def sma(df: pd.DataFrame, period: int = 20, price_col: str = "close") -> pd.Series:
    """
    计算简单移动平均线（SMA）。
    标准公式：SMA = 最近 N 期收盘价算术平均
    """
    close = pd.to_numeric(df[price_col], errors="coerce")
    if HAS_TALIB:
        return pd.Series(talib.SMA(close.values, timeperiod=period), index=df.index)
    return close.rolling(window=period, min_periods=period).mean()


def ema(df: pd.DataFrame, period: int = 20, price_col: str = "close") -> pd.Series:
    """
    计算指数移动平均线（EMA）。
    标准公式：EMA_t = alpha * P_t + (1-alpha) * EMA_{t-1}, alpha=2/(N+1)
    """
    close = pd.to_numeric(df[price_col], errors="coerce")
    if HAS_TALIB:
        return pd.Series(talib.EMA(close.values, timeperiod=period), index=df.index)
    return close.ewm(span=period, adjust=False).mean()


def rsi(df: pd.DataFrame, period: int = 14, price_col: str = "close") -> pd.Series:
    """
    计算 RSI（相对强弱指标）。
    标准思路：RSI = 100 - 100 / (1 + RS)，RS = 平均上涨幅度 / 平均下跌幅度
    """
    close = pd.to_numeric(df[price_col], errors="coerce")
    if HAS_TALIB:
        return pd.Series(talib.RSI(close.values, timeperiod=period), index=df.index)

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # 使用 Wilder 平滑（等价于 alpha=1/period 的 EMA）
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi_val = 100 - (100 / (1 + rs))
    return rsi_val


def macd(
    df: pd.DataFrame,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
    price_col: str = "close",
) -> pd.DataFrame:
    """
    计算 MACD 指标。

    返回三列：
    - macd: DIF（快 EMA - 慢 EMA）
    - signal: DEA（DIF 的 EMA）
    - hist: 柱状图（DIF - DEA）
    """
    close = pd.to_numeric(df[price_col], errors="coerce")

    if HAS_TALIB:
        macd_line, signal_line, hist = talib.MACD(
            close.values,
            fastperiod=fast_period,
            slowperiod=slow_period,
            signalperiod=signal_period,
        )
        return pd.DataFrame(
            {"macd": macd_line, "signal": signal_line, "hist": hist}, index=df.index
        )

    ema_fast = close.ewm(span=fast_period, adjust=False).mean()
    ema_slow = close.ewm(span=slow_period, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    hist = macd_line - signal_line

    return pd.DataFrame(
        {"macd": macd_line, "signal": signal_line, "hist": hist}, index=df.index
    )


def bollinger_bands(
    df: pd.DataFrame,
    period: int = 20,
    std_dev: float = 2.0,
    price_col: str = "close",
) -> pd.DataFrame:
    """
    计算布林带（BOLL）。

    标准公式：
    - middle = N 日均线
    - upper = middle + k * N 日标准差
    - lower = middle - k * N 日标准差
    """
    close = pd.to_numeric(df[price_col], errors="coerce")

    if HAS_TALIB:
        upper, middle, lower = talib.BBANDS(
            close.values,
            timeperiod=period,
            nbdevup=std_dev,
            nbdevdn=std_dev,
            matype=0,
        )
        return pd.DataFrame(
            {"boll_upper": upper, "boll_middle": middle, "boll_lower": lower},
            index=df.index,
        )

    middle = close.rolling(window=period, min_periods=period).mean()
    sigma = close.rolling(window=period, min_periods=period).std(ddof=0)
    upper = middle + std_dev * sigma
    lower = middle - std_dev * sigma
    return pd.DataFrame(
        {"boll_upper": upper, "boll_middle": middle, "boll_lower": lower},
        index=df.index,
    )


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算 ATR（平均真实波幅）。
    需要 high/low/close 三列。
    """
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")

    if HAS_TALIB:
        return pd.Series(
            talib.ATR(high.values, low.values, close.values, timeperiod=period),
            index=df.index,
        )

    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def add_common_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    为行情表批量添加常用指标列，便于后续筛选与排序模块直接使用。

    新增列示例：
    - ma50, ma150, ma200
    - rsi14
    - macd, macd_signal, macd_hist
    - boll_upper, boll_middle, boll_lower
    """
    out = df.copy()

    out["ma50"] = sma(out, period=50)
    out["ma150"] = sma(out, period=150)
    out["ma200"] = sma(out, period=200)
    out["rsi14"] = rsi(out, period=14)

    macd_df = macd(out, fast_period=12, slow_period=26, signal_period=9)
    out["macd"] = macd_df["macd"]
    out["macd_signal"] = macd_df["signal"]
    out["macd_hist"] = macd_df["hist"]

    boll_df = bollinger_bands(out, period=20, std_dev=2.0)
    out["boll_upper"] = boll_df["boll_upper"]
    out["boll_middle"] = boll_df["boll_middle"]
    out["boll_lower"] = boll_df["boll_lower"]

    out["atr14"] = atr(out, period=14)
    return out


def macd_cross_signal(df: pd.DataFrame) -> pd.Series:
    """
    生成 MACD 金叉/死叉信号：
    - 1  : 金叉（macd 上穿 signal）
    - -1 : 死叉（macd 下穿 signal）
    - 0  : 无信号
    """
    lines = macd(df)
    m = lines["macd"]
    s = lines["signal"]
    cross_up = (m > s) & (m.shift(1) <= s.shift(1))
    cross_down = (m < s) & (m.shift(1) >= s.shift(1))
    signal = pd.Series(0, index=df.index, dtype="int64")
    signal[cross_up] = 1
    signal[cross_down] = -1
    return signal


def is_doji(df: pd.DataFrame, body_ratio_threshold: float = 0.1) -> pd.Series:
    """
    十字线（Doji）
    标准定义：开盘价与收盘价非常接近，实体极小。
    实现规则：实体 <= 振幅 * 阈值（默认 10%）
    """
    p = _candle_parts(df)
    cond = p["body"] <= (p["range_"] * body_ratio_threshold)
    return cond.fillna(False)


def is_hammer(df: pd.DataFrame) -> pd.Series:
    """
    锤子线（Hammer）
    常见定义：
    - 小实体（位于区间上半部）
    - 下影线明显较长（通常 >= 实体 2 倍）
    - 上影线较短
    """
    p = _candle_parts(df)
    small_body = p["body"] <= p["range_"] * 0.4
    long_lower = p["lower_shadow"] >= p["body"] * 2
    short_upper = p["upper_shadow"] <= p["body"]
    close_near_high = (p["high"] - p["close"]) <= p["range_"] * 0.3
    cond = small_body & long_lower & short_upper & close_near_high
    return cond.fillna(False)


def is_hanging_man(df: pd.DataFrame) -> pd.Series:
    """
    上吊线（Hanging Man）
    K 线形态与锤子线近似，但通常出现在上升趋势后。
    这里只识别蜡烛形态本身（不强制附加趋势过滤）。
    """
    return is_hammer(df)


def is_bullish_engulfing(df: pd.DataFrame) -> pd.Series:
    """
    看涨吞没（Bullish Engulfing）
    - 前一根为阴线（close_prev < open_prev）
    - 当前为阳线（close > open）
    - 当前实体吞没前一根实体
    """
    p = _candle_parts(df)
    open_prev = p["open"].shift(1)
    close_prev = p["close"].shift(1)

    prev_bear = close_prev < open_prev
    curr_bull = p["close"] > p["open"]
    engulf = (p["open"] <= close_prev) & (p["close"] >= open_prev)
    cond = prev_bear & curr_bull & engulf
    return cond.fillna(False)


def is_bearish_engulfing(df: pd.DataFrame) -> pd.Series:
    """
    看跌吞没（Bearish Engulfing）
    - 前一根为阳线
    - 当前为阴线
    - 当前实体吞没前一根实体
    """
    p = _candle_parts(df)
    open_prev = p["open"].shift(1)
    close_prev = p["close"].shift(1)

    prev_bull = close_prev > open_prev
    curr_bear = p["close"] < p["open"]
    engulf = (p["open"] >= close_prev) & (p["close"] <= open_prev)
    cond = prev_bull & curr_bear & engulf
    return cond.fillna(False)


def is_bullish_harami(df: pd.DataFrame) -> pd.Series:
    """
    看涨孕线（Bullish Harami）
    - 前一根为较大阴线
    - 当前为阳线，且实体完全包含在前一根实体内部
    """
    p = _candle_parts(df)
    open_prev = p["open"].shift(1)
    close_prev = p["close"].shift(1)
    body_prev = p["body"].shift(1)

    prev_bear = close_prev < open_prev
    curr_bull = p["close"] > p["open"]
    inside = (p["open"] >= close_prev) & (p["close"] <= open_prev)
    smaller = p["body"] < body_prev
    cond = prev_bear & curr_bull & inside & smaller
    return cond.fillna(False)


def is_bearish_harami(df: pd.DataFrame) -> pd.Series:
    """
    看跌孕线（Bearish Harami）
    - 前一根为较大阳线
    - 当前为阴线，且实体完全包含在前一根实体内部
    """
    p = _candle_parts(df)
    open_prev = p["open"].shift(1)
    close_prev = p["close"].shift(1)
    body_prev = p["body"].shift(1)

    prev_bull = close_prev > open_prev
    curr_bear = p["close"] < p["open"]
    inside = (p["open"] <= close_prev) & (p["close"] >= open_prev)
    smaller = p["body"] < body_prev
    cond = prev_bull & curr_bear & inside & smaller
    return cond.fillna(False)


def is_morning_star(df: pd.DataFrame) -> pd.Series:
    """
    启明星（Morning Star，三日形态）
    常见定义（简化实现）：
    - 第1日：较长阴线
    - 第2日：小实体（星线）
    - 第3日：阳线，且收盘回到第1日实体中部上方
    """
    p = _candle_parts(df)
    o1, c1, b1 = p["open"].shift(2), p["close"].shift(2), p["body"].shift(2)
    o2, c2, b2 = p["open"].shift(1), p["close"].shift(1), p["body"].shift(1)
    o3, c3, b3 = p["open"], p["close"], p["body"]

    first_bear = c1 < o1
    second_small = b2 <= b1 * 0.5
    third_bull = c3 > o3
    recover_mid = c3 >= (o1 + c1) / 2
    cond = first_bear & second_small & third_bull & recover_mid & (b3 > 0)
    return cond.fillna(False)


def is_evening_star(df: pd.DataFrame) -> pd.Series:
    """
    黄昏星（Evening Star，三日形态）
    常见定义（简化实现）：
    - 第1日：较长阳线
    - 第2日：小实体（星线）
    - 第3日：阴线，且收盘跌入第1日实体中部下方
    """
    p = _candle_parts(df)
    o1, c1, b1 = p["open"].shift(2), p["close"].shift(2), p["body"].shift(2)
    o2, c2, b2 = p["open"].shift(1), p["close"].shift(1), p["body"].shift(1)
    o3, c3, b3 = p["open"], p["close"], p["body"]

    first_bull = c1 > o1
    second_small = b2 <= b1 * 0.5
    third_bear = c3 < o3
    drop_mid = c3 <= (o1 + c1) / 2
    cond = first_bull & second_small & third_bear & drop_mid & (b3 > 0)
    return cond.fillna(False)


def is_piercing_line(df: pd.DataFrame) -> pd.Series:
    """
    刺透形态（Piercing Line，二日看涨反转）
    - 前一日阴线
    - 当日阳线
    - 当日开盘低于前收（或接近向下跳空）
    - 当日收盘高于前一日实体中点，但不超过前开盘
    """
    p = _candle_parts(df)
    o1, c1 = p["open"].shift(1), p["close"].shift(1)
    o2, c2 = p["open"], p["close"]

    prev_bear = c1 < o1
    curr_bull = c2 > o2
    open_lower = o2 <= c1
    close_above_mid = c2 > (o1 + c1) / 2
    close_below_prev_open = c2 < o1
    cond = prev_bear & curr_bull & open_lower & close_above_mid & close_below_prev_open
    return cond.fillna(False)


def is_dark_cloud_cover(df: pd.DataFrame) -> pd.Series:
    """
    乌云盖顶（Dark Cloud Cover，二日看跌反转）
    - 前一日阳线
    - 当日阴线
    - 当日开盘高于前收（或接近向上跳空）
    - 当日收盘跌破前一日实体中点，但不低于前收盘
    """
    p = _candle_parts(df)
    o1, c1 = p["open"].shift(1), p["close"].shift(1)
    o2, c2 = p["open"], p["close"]

    prev_bull = c1 > o1
    curr_bear = c2 < o2
    open_higher = o2 >= c1
    close_below_mid = c2 < (o1 + c1) / 2
    close_above_prev_open = c2 > o1
    cond = prev_bull & curr_bear & open_higher & close_below_mid & close_above_prev_open
    return cond.fillna(False)
