"""PyInstaller launcher for Streamlit app.

功能：
1) 自动寻找空闲端口；
2) 在默认浏览器打开页面；
3) 兼容源码运行与 PyInstaller 冻结运行；
4) 处理 .env 缺失场景（若存在 .env.example 则自动生成 .env）。
"""

from __future__ import annotations

import os
import shutil
import socket
import sys
import threading
import time
import webbrowser
from pathlib import Path


def _resource_base() -> Path:
    """返回运行时资源根目录（源码模式/冻结模式统一）。"""
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
    return Path(__file__).resolve().parent


def _find_free_port(start: int = 8501, end: int = 8999) -> int:
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise RuntimeError("未找到可用端口，请关闭部分占用端口的程序后重试。")


def _ensure_env_file(base_dir: Path) -> None:
    env_file = base_dir / ".env"
    env_example = base_dir / ".env.example"
    if env_file.exists():
        return
    if env_example.exists():
        shutil.copy2(env_example, env_file)
        print(f"[INFO] 未检测到 .env，已根据 .env.example 生成: {env_file}")
        return
    env_file.write_text(
        "AI_PROVIDER=your-provider\n"
        "AI_API_KEY=YOUR_AI_API_KEY\n"
        "AI_BASE_URL=https://your-ai-endpoint/v1\n"
        "AI_MODEL=your-model\n",
        encoding="utf-8",
    )
    print(f"[INFO] 未检测到 .env，已生成默认模板: {env_file}")


def _open_browser_later(url: str, delay: float = 1.5) -> None:
    def _task() -> None:
        time.sleep(delay)
        webbrowser.open(url)

    threading.Thread(target=_task, daemon=True).start()


def main() -> None:
    base = _resource_base()
    app_file = base / "app.py"
    if not app_file.exists():
        raise FileNotFoundError(f"未找到 app.py: {app_file}")

    _ensure_env_file(base)
    os.chdir(base)

    port = _find_free_port()
    url = f"http://127.0.0.1:{port}"
    _open_browser_later(url)

    # 通过 streamlit CLI 主入口运行，避免 subprocess + -m 在冻结态的兼容问题。
    from streamlit.web import cli as stcli

    sys.argv = [
        "streamlit",
        "run",
        str(app_file),
        "--server.port",
        str(port),
        "--server.address",
        "127.0.0.1",
        "--browser.gatherUsageStats",
        "false",
    ]
    stcli.main()


if __name__ == "__main__":
    main()

