# GTA GXT 文本编辑器 v2.4 说明文档

这是一款专为《侠盗猎车手》(Grand Theft Auto) 系列游戏设计的强大文本文件编辑器，由“倾城剑舞”开发，支持 `.gxt`、`.gxt2` 和 `whm_table.dat` 文件格式。它为游戏汉化人员、Mod 开发者以及爱好者提供了一个高效、直观的工具，用于编辑和管理游戏中的文本内容，适用于从经典的 GTA III 到最新的 GTA V。

## 运行程序

该程序基于 **Python** 和 **PySide6** 开发，采用现代化深色主题界面，支持跨平台运行（主要针对 Windows 系统优化）。您可以从源代码运行程序。

### 环境要求
- **Python**: 3.6 或更高版本
- **PySide6**: 用于图形界面的 Qt 框架绑定
- 推荐安装其他依赖库（如 `pathlib`、`collections` 等，通常随 Python 标准库提供）

### 安装步骤
1. 安装 Python 3.x（确保已添加至系统 PATH）。
2. 安装 PySide6 库：
    ```bash
    pip install PySide6
    ```
3. 将以下文件放置在同一目录下：
   - `main.py`（主程序）
   - `gxt_parser.py`（GXT 文件解析核心）
   - `IVGXT.py`（GTA IV GXT 处理）
   - `VCGXT.py`（GTA Vice City GXT 处理）
   - `SAGXT.py`（GTA San Andreas GXT 处理）
   - `LCGXT.py`（GTA III GXT 处理）
   - `GTA4_WHM_Text_Extractor.py`（GTA4 WHM  文件处理）
   - `gta5_gxt2.py`（GTA V GXT2 文件处理）
4. 运行主程序：
    ```bash
    python main.py
    ```