import os
import subprocess
import sys
import platform
import shutil
import argparse
from pathlib import Path

def check_and_install(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        __import__(import_name)
    except ImportError:
        print(f"[INFO] {package} 未安装，尝试通过 pip 安装...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def collect_qt_zh_files():
    """收集 PySide6/Qt 的 zh_CN.qm 文件（返回绝对路径列表）"""
    zh_files = []
    try:
        from PySide6.QtCore import QLibraryInfo
        trans_path = QLibraryInfo.path(QLibraryInfo.LibraryPath.TranslationsPath)
        if trans_path and os.path.isdir(trans_path):
            for name in os.listdir(trans_path):
                if name.endswith("zh_CN.qm") or name.endswith("_zh_CN.qm"):
                    zh_files.append(os.path.join(trans_path, name))
    except Exception as e:
        print(f"[WARN] 无法获取 Qt translations 路径: {e}")
    return zh_files

def build(args):
    # 确保依赖
    check_and_install("pyinstaller", "PyInstaller")
    check_and_install("pyside6", "PySide6")

    main_script = "main.py"
    if not os.path.exists(main_script):
        print(f"[ERROR] 找不到入口脚本 '{main_script}'，请放在当前目录后重试。")
        return

    is_windows = platform.system() == "Windows"
    sep = ";" if is_windows else ":"

    data_files = []

    # icon
    icon_path = "app_icon.ico"
    icon_arg = None
    if os.path.exists(icon_path):
        icon_arg = f"--icon={icon_path}"
        # 目标放到根目录
        data_files.append(f"{icon_path}{sep}.")
        print(f"[INFO] 找到图标: {icon_path}")
    else:
        print("[WARN] 未找到 app_icon.ico，打包时将使用默认图标。")

    # Qt 自带 zh_CN.qm
    qt_zh = collect_qt_zh_files()
    if qt_zh:
        for f in qt_zh:
            # 放到 translations 子目录
            data_files.append(f"{f}{sep}translations")
        print(f"[INFO] 添加 Qt 自带翻译文件 ({len(qt_zh)} 个)。")
    else:
        print("[WARN] 未在 Qt 安装目录找到 zh_CN.qm（qt_zh_CN 等）。")

    # 自定义 translations 文件夹
    if os.path.isdir("translations"):
        data_files.append(f"translations{sep}translations")
        print("[INFO] 已添加工程 translations/ 文件夹。")
    else:
        print("[WARN] 未发现工程 translations/ 文件夹（若你有自定义 .qm，请放在 translations/ 下）。")

    # ✅ 新增：打包 GXT_Tables（Chinese） 和 GXT_Tables（original） 文件夹
    if os.path.isdir("GXT_Tables（Chinese）"):
        data_files.append(f"GXT_Tables（Chinese）{sep}GXT_Tables（Chinese）")
        print("[INFO] 已添加 GXT_Tables（Chinese） 文件夹。")
    else:
        print("[WARN] 未找到 GXT_Tables（Chinese） 文件夹。")

    if os.path.isdir("GXT_Tables（original）"):
        data_files.append(f"GXT_Tables（original）{sep}GXT_Tables（original）")
        print("[INFO] 已添加 GXT_Tables（original） 文件夹。")
    else:
        print("[WARN] 未找到 GXT_Tables（original） 文件夹。")

    # 构造 pyinstaller 命令
    cmd = ["pyinstaller"]
    if args.onefile:
        cmd.append("--onefile")
    else:
        cmd.append("--onedir")

    if not args.console:
        cmd.append("--windowed")

    cmd.append(f"--name={args.name}")

    if icon_arg:
        cmd.append(icon_arg)

    # 添加所有数据文件
    for d in data_files:
        cmd.extend(["--add-data", d])

    # cmd.append("--noupx")  # 可选：关闭 UPX 压缩以防出错
    cmd.append(main_script)

    print("\n[COMMAND] " + " ".join(f'\"{c}\"' if " " in c else c for c in cmd) + "\n")

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] 打包失败: {e}")
        return

    # 打包完成
    dist_path = Path("dist") / args.name
    if args.onefile:
        exe_path = Path("dist") / (args.name + (".exe" if is_windows else ""))
        print(f"[OK] 打包成功（onefile）。可执行文件位置: {exe_path}")
    else:
        print(f"[OK] 打包成功（onedir）。目录位置: {dist_path}")

    # 清理
    if not args.no_clean:
        for to_rm in ["build"]:
            if os.path.exists(to_rm):
                shutil.rmtree(to_rm, ignore_errors=True)
                print(f"[CLEAN] 已删除: {to_rm}")
        spec_file = f"{args.name}.spec"
        if os.path.exists(spec_file):
            try:
                os.remove(spec_file)
                print(f"[CLEAN] 已删除: {spec_file}")
            except Exception:
                print(f"[CLEAN] 无法删除 spec 文件: {spec_file}（可能正在被占用）")
    else:
        print("[INFO] 已跳过清理（--no-clean 指定）")

def parse_args():
    p = argparse.ArgumentParser(description="打包脚本（PyInstaller + PySide6）")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--onefile", action="store_true", help="打包为单文件 exe（默认）")
    group.add_argument("--onedir", action="store_true", help="打包为目录（便于调试）")
    p.add_argument("--console", action="store_true", help="保留控制台窗口（便于查看 print/debug）")
    p.add_argument("--no-clean", action="store_true", help="打包后不清理 build 和 .spec（便于调试）")
    p.add_argument("--name", default="GTA文本对话表编辑器", help="程序名称（dist 下显示的文件/目录名）")
    args = p.parse_args()

    args.onefile = not args.onedir
    return args

if __name__ == "__main__":
    args = parse_args()
    build(args)
