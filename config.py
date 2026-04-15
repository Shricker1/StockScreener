"""
全局配置文件
-----------
本文件使用字典形式集中管理项目配置，便于在各模块中统一读取。
建议在主程序中通过 `from config import CONFIG` 导入使用。
"""
import os
import logging
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger("stock_screener.config")


def _parse_optional_int(raw: str | None) -> int | None:
    """将环境变量解析为可选 int；空字符串/none/null 返回 None。"""
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "" or text.lower() in {"none", "null"}:
        return None
    try:
        return int(text)
    except Exception:
        return None

# ========== 数据源配置 ==========
# 默认：AkShare（公开接口，无需单独 API Key）
# 可选: "akshare" / "tushare"（可通过环境变量 DATA_SOURCE 覆盖）
_ds = os.getenv("DATA_SOURCE", "akshare").strip().lower()
DATA_SOURCE = _ds if _ds in {"akshare", "tushare"} else "akshare"

# 可选数据源优先级（逗号分隔，按顺序尝试；默认 Tushare 优先）：
# - tushare: 需 TUSHARE_TOKEN
# - akshare_em / akshare: 东方财富(AkShare)
# - akshare_sina: 新浪财经日线
# - tencent: 腾讯财经（注：可能缺少 volume 字段）
# - efinance: 东财 efinance 库（若已安装）
DATA_SOURCE_PRIORITY = os.getenv(
    "DATA_SOURCE_PRIORITY",
    "tushare,akshare_em,akshare_sina,tencent,efinance",
).strip()

# ========== Tushare 配置（第三方服务） ==========
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")
TUSHARE_API_URL = os.getenv("TUSHARE_API_URL", "")

# ========== AkShare 配置（可选） ==========
# 说明：AkShare 大多数接口无需单独 token。
# 若你有自建/代理接口地址，可通过 AKSHARE_API_URL 传入。
AKSHARE_API_URL = os.getenv("AKSHARE_API_URL", "")
AKSHARE_TIMEOUT = int(os.getenv("AKSHARE_TIMEOUT", 30))

# ========== TickFlow（可选，与 Tushare 协作）==========
# 用于：指数 K 线回退/补充、标的池全市场行情（涨跌家数等）。
# 兼容别名：部分用户会写成 TICKETFLOW_*（少一个 k），这里统一兼容两套环境变量。
_tickflow_key = os.getenv("TICKFLOW_API_KEY", "").strip()
_ticketflow_key = os.getenv("TICKETFLOW_API_KEY", "").strip()
TICKFLOW_API_KEY = _tickflow_key or _ticketflow_key

_tickflow_base = os.getenv("TICKFLOW_BASE_URL", "").strip()
_ticketflow_base = os.getenv("TICKETFLOW_BASE_URL", "").strip()
TICKFLOW_BASE_URL = (_tickflow_base or _ticketflow_base or "").strip()

_kline_fb = os.getenv("TICKFLOW_USE_KLINE_FALLBACK", os.getenv("TICKETFLOW_USE_KLINE_FALLBACK", "true"))
TICKFLOW_USE_KLINE_FALLBACK = str(_kline_fb).lower() == "true"

_univ_stats = os.getenv(
    "TICKFLOW_USE_UNIVERSE_MARKET_STATS",
    os.getenv("TICKETFLOW_USE_UNIVERSE_MARKET_STATS", "true"),
)
TICKFLOW_USE_UNIVERSE_MARKET_STATS = str(_univ_stats).lower() == "true"

TICKFLOW_MARKET_UNIVERSE = (
    os.getenv("TICKFLOW_MARKET_UNIVERSE", "").strip()
    or os.getenv("TICKETFLOW_MARKET_UNIVERSE", "").strip()
    or "CN_Equity_A"
)

# ========== 均线周期配置 ==========
MA_SHORT = 50
MA_MEDIUM = 150
MA_LONG = 200

# ========== CAN SLIM 评分权重 ==========
CAN_SLIM_WEIGHTS = {
    "C": 0.25,
    "A": 0.25,
    "N": 0.10,
    "S": 0.10,
    "L": 0.15,
    "I": 0.10,
    "M": 0.05,
}

# ========== 输出路径配置 ==========
OUTPUT_DIR = "./output_files/"
CACHE_DIR = "./cache/"
LOG_DIR = "./logs/"

# ========== 缠论分析 ChanAnalysis（可选模块）==========
# 完整功能需安装: pip install git+https://github.com/Vespa314/chan.py （建议 Python 3.11+）
CHANLUN_ENABLED = os.getenv("CHANLUN_ENABLED", "true").lower() == "true"
CHANLUN_AI_ENABLED = os.getenv("CHANLUN_AI_ENABLED", "true").lower() == "true"
_chan_plot_dir = os.getenv("CHANLUN_PLOT_DIR", "").strip()
CHANLUN_PLOT_DIR = _chan_plot_dir if _chan_plot_dir else os.path.join(OUTPUT_DIR, "chanlun_plots")


def parse_stock_list(stock_list_str: str) -> list[str]:
    """
    解析环境变量中的股票列表配置。
    - 空字符串: 返回空列表（主流程回退到全市场）
    - hs300/沪深300: 动态获取沪深300成分股
    - 其他: 按逗号分隔返回代码列表
    """
    raw = (stock_list_str or "").strip()
    if not raw:
        return []

    lowered = raw.lower()
    if lowered in {"hs300", "沪深300"}:
        try:
            # 延迟导入，避免模块初始化时循环依赖
            from data.fetcher import get_hs300_stocks

            return get_hs300_stocks()
        except Exception as exc:
            LOGGER.warning("解析 STOCK_LIST=hs300 失败: %s", exc)
            return []

    items = [x.strip() for x in raw.split(",")]
    return [x for x in items if x]


RAW_STOCK_LIST = os.getenv("STOCK_LIST", "")
STOCK_LIST = parse_stock_list(RAW_STOCK_LIST)

CONFIG = {
    # 1) 数据源选择（默认 akshare，无需 token）
    # - "akshare": 行情/财务以 AkShare 为主（见 DATA_SOURCE_PRIORITY）
    # - "tushare": 财务等仍走 Tushare 分支时需配置 token
    "data_source": DATA_SOURCE,

    # 2) Tushare Token
    # 在使用 Tushare 时用于身份认证。
    # 你可以将真实 token 填到这里，或在生产环境改为从环境变量读取。
    "tushare_token": TUSHARE_TOKEN,
    "tushare_api_url": TUSHARE_API_URL,
    "akshare_api_url": AKSHARE_API_URL,
    "akshare_timeout": AKSHARE_TIMEOUT,
    "data_source_priority": DATA_SOURCE_PRIORITY,
    "tickflow_api_key": TICKFLOW_API_KEY,
    "tickflow_base_url": TICKFLOW_BASE_URL,
    "tickflow_use_kline_fallback": TICKFLOW_USE_KLINE_FALLBACK,
    "tickflow_use_universe_market_stats": TICKFLOW_USE_UNIVERSE_MARKET_STATS,
    "tickflow_market_universe": TICKFLOW_MARKET_UNIVERSE,

    # 3) 均线周期配置
    # 用于趋势判断（例如 Stage 2 模板）和相关技术指标计算。
    # 保持升序有助于提高可读性：短期 -> 中期 -> 长期。
    "ma_periods": [MA_SHORT, MA_MEDIUM, MA_LONG],

    # 3.1) 是否优先使用沪深300作为测试股票池
    # - True: 优先使用沪深300成分股（更快，便于调试）
    # - False: 使用全市场股票池
    "use_hs300_test": True,

    # 3.2) Stage Two 并发线程数
    # 用于 main.py 中 ThreadPoolExecutor 的 max_workers。
    # 机器性能较强可适当调大，若遇到接口限流可调小。
    "stage_two_workers": 8,

    # 4) CAN SLIM 各项阈值配置
    # 以下阈值为策略参数示例，后续可根据回测结果动态优化。
    # 字段说明：
    # - C: 当前季度每股收益(EPS)同比增速下限（百分比）
    # - A: 年度EPS复合增长率下限（百分比）
    # - N: 是否要求近一年内创出新高（布尔值）
    # - S: 供需因子阈值（示例中使用最低成交额，单位：元）
    # - L: 相对强度(RS)最低分数（0-100）
    # - I: 机构持仓比例最低阈值（百分比）
    # - M: 市场趋势过滤（是否仅在大盘上升趋势下选股）
    "can_slim_thresholds": {
        "C_quarterly_eps_growth_min": 25.0,
        "A_annual_eps_growth_min": 20.0,
        "N_require_new_high": True,
        "S_min_turnover_amount": 100000000.0,
        "L_relative_strength_min": 80,
        "I_institution_holding_min": 5.0,
        "M_require_market_uptrend": True,
    },
    "can_slim_weights": CAN_SLIM_WEIGHTS,

    # 5) 输出文件路径配置
    # output_dir: 输出目录（程序会在此目录下保存结果）
    # result_filename: 默认结果文件名（Excel）
    # log_file: 默认日志文件路径
    "output": {
        "output_dir": OUTPUT_DIR,
        "result_filename": "stock_screening_result.xlsx",
        "log_file": f"{LOG_DIR}app.log",
    },

    # 缠论 ChanAnalysis（与主 AI/辩论独立）
    "chanlun_enabled": CHANLUN_ENABLED,
    "chanlun_ai_enabled": CHANLUN_AI_ENABLED,
    "chanlun_plot_dir": CHANLUN_PLOT_DIR,
}

# ========== AI API 配置 ==========
AI_CONFIG = {
    "provider": os.getenv("AI_PROVIDER", "deepseek"),
    "api_key": os.getenv("AI_API_KEY"),
    "base_url": os.getenv("AI_BASE_URL", "https://your-ai-endpoint/v1"),
    "model": os.getenv("AI_MODEL", "deepseek-reasoner"),
    "max_tokens": int(os.getenv("AI_MAX_TOKENS", 8000)),
    "temperature": float(os.getenv("AI_TEMPERATURE", 0.3)),
    "max_context_tokens": int(os.getenv("AI_MAX_CONTEXT_TOKENS", 40000)),
    "reserved_output_tokens": int(os.getenv("AI_RESERVED_OUTPUT_TOKENS", 2000)),
    "token_safety_margin": int(os.getenv("AI_TOKEN_SAFETY_MARGIN", 200)),
    "token_count_method": os.getenv("AI_TOKEN_COUNT_METHOD", "estimate"),
    "api_version": os.getenv("AI_API_VERSION", "2024-02-15-preview"),
    "deployment_name": os.getenv("AI_DEPLOYMENT_NAME", ""),
}

# 自动计算可用于输入（system + user）的 token 上限，便于其他模块直接复用。
AI_CONFIG["max_input_tokens"] = (
    AI_CONFIG["max_context_tokens"]
    - AI_CONFIG["reserved_output_tokens"]
    - AI_CONFIG["token_safety_margin"]
)

# ========== 技术顾问 AI 配置（筹码与技术面独立分析） ==========
TECH_ADVISOR_CONFIG = {
    "provider": os.getenv("TECH_ADVISOR_PROVIDER", AI_CONFIG.get("provider", "deepseek")),
    "api_key": os.getenv("TECH_ADVISOR_API_KEY", AI_CONFIG.get("api_key")),
    "base_url": os.getenv("TECH_ADVISOR_BASE_URL", AI_CONFIG.get("base_url", "https://your-ai-endpoint/v1")),
    "model": os.getenv("TECH_ADVISOR_MODEL", AI_CONFIG.get("model", "deepseek-reasoner")),
    "temperature": float(os.getenv("TECH_ADVISOR_TEMPERATURE", "0.2")),
    # 明确支持 None（不限制）；调用时会转换成较大值。
    "max_tokens": _parse_optional_int(os.getenv("AI_TECH_ADVISOR_MAX_TOKENS")),
    "api_version": os.getenv("TECH_ADVISOR_API_VERSION", AI_CONFIG.get("api_version", "2024-02-15-preview")),
    "deployment_name": os.getenv("TECH_ADVISOR_DEPLOYMENT_NAME", AI_CONFIG.get("deployment_name", "")),
}

# ========== 多角色辩论配置 ==========
DEBATE_CONFIG = {
    "enabled": os.getenv("DEBATE_ENABLED", "false").lower() == "true",
    # 先留空，后续可在 ai/prompt.py 中动态注入
    "analyst_system_prompt": "",
    "veteran_system_prompt": "",
    "judge_system_prompt": "",
    "max_debate_rounds": 1,
}

# ========== Web/UI 与数据库配置 ==========
# 新版数据库路径配置（优先 DATABASE_PATH）。
# 为保持兼容，若未设置 DATABASE_PATH，则回退读取旧变量 SQLITE_PATH。
DATABASE_PATH = os.getenv("DATABASE_PATH", os.getenv("SQLITE_PATH", "data/stock_screener.db"))
UI_THEME = os.getenv("UI_THEME", "light")
FACTOR_EVAL_LOOKBACK_DAYS = int(os.getenv("FACTOR_EVAL_LOOKBACK_DAYS", 365))

DATABASE_CONFIG = {
    "sqlite_path": DATABASE_PATH,
}

UI_CONFIG = {
    "app_title": "StockScreener AI",
    "app_icon": "📈",
    "layout": "wide",
    "theme": UI_THEME,
    "factor_eval_lookback_days": FACTOR_EVAL_LOOKBACK_DAYS,
}

# 多模型配置示例（通义千问）：
# AI_PROVIDER=your-provider
# AI_API_KEY=YOUR_AI_API_KEY
# AI_BASE_URL=https://your-ai-endpoint/v1
# AI_MODEL=your-model

_missing = []
if DATA_SOURCE == "tushare":
    if not TUSHARE_TOKEN:
        _missing.append("TUSHARE_TOKEN")
    if not TUSHARE_API_URL:
        _missing.append("TUSHARE_API_URL")
if not AI_CONFIG.get("api_key"):
    _missing.append("AI_API_KEY")
if _missing:
    LOGGER.warning("关键环境变量缺失: %s。请检查 .env 配置。", ", ".join(_missing))
