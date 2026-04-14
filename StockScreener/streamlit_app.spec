# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for Streamlit onefile build."""

from pathlib import Path
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

project_dir = Path.cwd()

datas = []
hiddenimports = []

# Streamlit 静态资源与子模块
datas += collect_data_files("streamlit")
hiddenimports += collect_submodules("streamlit")

# 常见运行时依赖（按需可增减）
for pkg in ("plotly", "altair", "pyarrow", "watchdog", "sqlalchemy", "scipy", "tickflow"):
    try:
        datas += collect_data_files(pkg)
    except Exception:
        pass
    try:
        hiddenimports += collect_submodules(pkg)
    except Exception:
        pass

# 项目资源文件（确保冻结后能找到 app.py / 配置模板）
for rel in ("app.py", "config.py", ".env", ".env.example"):
    p = project_dir / rel
    if p.exists():
        datas.append((str(p), "."))

# 显式收集项目自定义模块
custom_packages = [
    "data",
    "filters",
    "analysis",
    "reports",
    "ai",
    "output",
    "ui",
    "factor_mining",
    "database",
]
for pkg in custom_packages:
    try:
        hiddenimports += collect_submodules(pkg)
    except Exception:
        pass

block_cipher = None

a = Analysis(
    ["run_app.py"],
    pathex=[str(project_dir)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="StockScreenerAI",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

