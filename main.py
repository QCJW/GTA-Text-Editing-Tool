import os
import shutil
import sys
import re
import json
import struct
import html
from pathlib import Path
from PySide6.QtGui import QIcon
from collections import OrderedDict, defaultdict, Counter
from functools import cmp_to_key
from typing import List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

from PySide6.QtCore import QObject, QThread
from PySide6.QtCore import Qt, QTimer, QRect, Signal, QPoint, QPointF, QTranslator, QLibraryInfo
from PySide6.QtGui import (
    QPalette, QColor, QAction, QGuiApplication, QFont,
    QPixmap, QPainter, QImage, QFontDatabase, QCursor, QFontMetrics
)
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QDockWidget, QListWidget, QTableWidget, QTableWidgetItem,
    QFileDialog, QLineEdit, QMessageBox, QVBoxLayout, QWidget, QMenuBar, QMenu,
    QStatusBar, QPushButton, QHBoxLayout, QLabel, QInputDialog, QTextEdit, QDialog,
    QDialogButtonBox, QAbstractItemView, QHeaderView, QCheckBox, QComboBox, QFontDialog,
    QScrollArea, QSizePolicy, QGroupBox, QFrame, QProgressDialog, QSplitter,
    QListWidgetItem, QTabWidget, QFormLayout, QProgressBar, QStyle, QTreeWidget, QTreeWidgetItem
)

try:
    from gxt_parser import getVersion, getReader, MemoryMappedFile
    from IVGXT import generate_binary as write_iv, process_special_chars, gta4_gxt_hash
    from VCGXT import VCGXT
    from SAGXT import SAGXT
    from LCGXT import LCGXT
    import gta5_gxt2
    from GTA4_WHM_Text_Extractor import CHtmlTextExport, WhmTextData
except ImportError as e:
    print(f"警告：缺少依赖项，部分功能可能无法使用 - {e}")
    class MockGxtParser:
        def getVersion(self, mm): return None
        def getReader(self, v): return None
        class MemoryMappedFile:
            def __init__(self, path): pass
            def __enter__(self): return self
            def __exit__(self, exc_type, exc_val, exc_tb): pass
            def seek(self, offset): pass
    gxt_parser = MockGxtParser()
    getVersion, getReader, MemoryMappedFile = gxt_parser.getVersion, gxt_parser.getReader, gxt_parser.MemoryMappedFile
    def mock_iv_func(*args, **kwargs): return {}, [], []
    def mock_process_chars(*args, **kwargs): pass
    def mock_gxt_hash(s): return hash(s) & 0xFFFFFFFF
    write_iv = mock_iv_func
    process_special_chars = mock_process_chars
    gta4_gxt_hash = mock_gxt_hash
    class MockGxtLib:
        def __init__(self): self.m_GxtData = {}; self.m_WideCharCollection = set()
        def SaveAsGXT(self, path): pass
        def save_as_gxt(self, path): pass
        def _utf8_to_utf16(self, s): return s.encode('utf-16le')
        def utf8_to_utf16(self, s): return s.encode('utf-16le')
        def GenerateQCJWStuff(self): pass
        def generate_qcjw_stuff(self): pass
        def _table_sort_method(self, a, b): return a < b
    VCGXT, SAGXT, LCGXT = MockGxtLib, MockGxtLib, MockGxtLib
    class MockGta5Gxt2:
        def parse_gxt2(self, path): return {}
        def save_gxt2(self, data, path): pass
        def parse_txt(self, path): return {}
        def joaat(self, key): return hash(key) & 0xFFFFFFFF
    gta5_gxt2 = MockGta5Gxt2()
    class MockWhm:
        class CHtmlTextExport:
            def ExtractWhmStrings(self, path, s): return []
            def ExportText(self, path, container): pass
            def GenerateDataBase(self, in_path, out_path): pass
            def decode_bytes(self, b): return b.decode('utf-8', 'ignore')
        class WhmTextData:
            def __init__(self): self.hash = 0; self.offset = 0
    CHtmlTextExport, WhmTextData = MockWhm.CHtmlTextExport, MockWhm.WhmTextData


def _get_key_validation_message(version, file_type='gxt'):
    if file_type == 'dat': return "DAT文件键名必须是0x或0X开头的8位十六进制数 (例如: 0x12345678)"
    if version == 'VC': return "VC键名必须是1-7位数字、大写字母或下划线"
    if version == 'SA': return "SA键名必须是1-8位十六进制数"
    if version == 'III': return "III键名必须是1-7位数字、字母或下划线"
    if version == 'IV': return "IV键名必须是字母数字下划线组成的明文，或是0x/0X开头的8位十六进制数"
    if version == 'V': return "V键名必须是明文，或是0x/0X开头的8位十六进制数"
    return "键名格式不正确"


def _validate_key_static(key, version, file_type='gxt'):
    if file_type == 'dat':
        return re.fullmatch(r'0[xX][0-9a-fA-F]{8}', key) is not None
    
    if version == 'VC':
        return re.fullmatch(r'[0-9A-Z_]{1,7}', key) is not None
    elif version == 'SA':
        return re.fullmatch(r'[0-9a-fA-F]{1,8}', key) is not None
    elif version == 'III':
        return re.fullmatch(r'[0-9a-zA-Z_]{1,7}', key) is not None
    elif version == 'IV' or version == 'V':
        if key.lower().startswith('0x'):
            return re.fullmatch(r'0[xX][0-9a-fA-F]{8}', key) is not None
        else:
            return bool(key and re.fullmatch(r'[A-Za-z0-9_]+', key))
    return True


def _validate_key_for_import_optimized(key, version):
    """Optimized validation function for TXT import, returning a boolean and a message."""
    if _validate_key_static(key, version, 'gxt'):
        return True, ""
    else:
        return False, _get_key_validation_message(version, 'gxt')


class FontTextureGenerator:
    """GTA 字体贴图生成器核心类"""
    def __init__(self):
        self.margin = 2
        self.y_offset = -4
        self.bg_color = QColor(0, 0, 0, 0)
        self.text_color = QColor('white')

    def create_pixmap(self, characters, version, texture_size, font):
        """创建并返回 QPixmap 对象，用于预览或保存"""
        if not characters:
            return QPixmap()

        chars_per_line = 64 if texture_size == 4096 else 32
        pixmap = QPixmap(texture_size, texture_size)
        pixmap.fill(self.bg_color)

        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.TextAntialiasing)
        painter.setFont(font)
        painter.setPen(self.text_color)

        char_width = texture_size // chars_per_line
        char_height_map = {"III": 80, "VC": 64, "SA": 80, "IV": 66}
        char_height = char_height_map.get(version, 64)

        x, y = 0, 0
        for char in characters:
            draw_rect = QRect(
                x + self.margin, y + self.margin + self.y_offset,
                char_width - 2 * self.margin, char_height - 2 * self.margin
            )
            painter.drawText(draw_rect, Qt.AlignmentFlag.AlignCenter, char)
            x += char_width
            if x >= texture_size:
                x = 0
                y += char_height
                if y + char_height > texture_size:
                    print(f"警告：字符过多，部分字符 '{char}' 之后的内容可能未被绘制")
                    break
        painter.end()
        return pixmap

    def generate_and_save(self, characters, output_path, version, texture_size, font):
        """生成贴图并保存到文件"""
        pixmap = self.create_pixmap(characters, version, texture_size, font)
        if not pixmap.isNull():
            if not pixmap.save(output_path, "PNG"):
                raise IOError(f"无法保存文件到 {output_path}")

    def generate_html_preview(self, settings, texture_filename, output_path):
        """生成HTML预览文件"""
        char_width = settings['resolution'] // (64 if settings['resolution'] == 4096 else 32)
        char_height_map = {"III": 80, "VC": 64, "SA": 80, "IV": 66}
        char_height = char_height_map.get(settings['version'], 64)

        html_content = f"""
        <!DOCTYPE html>
        <html lang="zh-CN"><head><meta charset="UTF-8"><title>字体贴图预览</title>
        <style>
            body {{ font-family: sans-serif; background-color: #1e1e1e; color: #e0e0e0; }}
            .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
            h1, h2 {{ text-align: center; color: #4fc3f7; }}
            .info-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; background-color: #2d2d2d; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .info-item {{ margin: 5px 0; }} .info-item strong {{ color: #82b1ff; }}
            .texture-container {{ text-align: center; margin-bottom: 30px; }}
            .texture-img {{ max-width: 100%; border: 1px solid #444; }}
            .char-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(80px, 1fr)); gap: 10px; margin-top: 20px; }}
            .char-item {{ background-color: #2d2d2d; border: 1px solid #444; border-radius: 4px; padding: 10px; text-align: center; }}
            .char-display {{ font-size: 24px; margin-bottom: 5px; height: 40px; display: flex; align-items: center; justify-content: center; }}
            .char-code {{ font-size: 12px; color: #aaa; }}
        </style></head><body><div class="container">
            <h1>字体贴图预览</h1>
            <div class="info-grid">
                <div class="info-item"><strong>游戏版本:</strong> {settings['version']}</div>
                <div class="info-item"><strong>贴图尺寸:</strong> {settings['resolution']}x{settings['resolution']}px</div>
                <div class="info-item"><strong>字符总数:</strong> {len(settings['characters'])}</div>
                <div class="info-item"><strong>单元格尺寸:</strong> {char_width}x{char_height}px</div>
                <div class="info-item"><strong>字体:</strong> {settings['font_normal'].family()}, {settings['font_normal'].pointSize()}pt</div>
            </div>
            <div class="texture-container"><h2>字体贴图</h2><img src="{os.path.basename(texture_filename)}" alt="字体贴图" class="texture-img"></div>
            
            <div class="char-container">
                <h2>字符列表 (共 {len(settings['characters'])} 个字符)</h2>
                <div class="char-grid">
        """
        
        for char in settings['characters']:
            char_code = ord(char)
            html_content += f"""
                <div class="char-item">
                    <div class="char-display">{char}</div>
                    <div class="char-code">U+{char_code:04X}</div>
                </div>
            """
        
        html_content += """
                </div>
            </div>
        </div></body></html>
        """
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)


class ImageViewer(QDialog):
    """图片查看器对话框，支持滚轮缩放和鼠标拖动平移"""
    def __init__(self, pixmap, title="图片预览", parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.original_pixmap = pixmap
        self.scale_factor = 1.0

        self.image_label = QLabel()
        self.image_label.setScaledContents(False)
        self.image_label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)

        self.scroll_area = QScrollArea()
        self.scroll_area.setBackgroundRole(QPalette.ColorRole.Dark)
        self.scroll_area.setWidget(self.image_label)
        self.scroll_area.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.scroll_area.setWidgetResizable(False)
        self.scroll_area.viewport().setCursor(Qt.CursorShape.OpenHandCursor)
        self.is_panning = False
        self.last_mouse_pos = QPoint()

        main_layout = QVBoxLayout(self)
        main_layout.addWidget(self.scroll_area)
        self.setLayout(main_layout)

        self.scroll_area.viewport().installEventFilter(self)

        self.update_image_scale()
        self.resize(2048, 2048)

    def fit_to_window(self):
        if self.original_pixmap.isNull() or self.original_pixmap.width() == 0 or self.original_pixmap.height() == 0:
            return

        area_size = self.scroll_area.viewport().size()
        pixmap_w = self.original_pixmap.width()
        pixmap_h = self.original_pixmap.height()

        w_ratio = area_size.width() / pixmap_w
        h_ratio = area_size.height() / pixmap_h

        self.scale_factor = min(w_ratio, h_ratio)
        self.update_image_scale()

    def update_image_scale(self):
        if self.original_pixmap.isNull():
            return

        new_w = max(1, int(self.original_pixmap.width() * self.scale_factor))
        new_h = max(1, int(self.original_pixmap.height() * self.scale_factor))

        scaled_pixmap = self.original_pixmap.scaled(
            new_w, new_h,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation
        )
        self.image_label.setPixmap(scaled_pixmap)
        self.image_label.resize(scaled_pixmap.size())

    def _perform_zoom_at(self, delta_y, point_under_cursor):
        """delta_y: angleDelta().y()（>0 放大, <0 缩小）
           point_under_cursor: QPoint（相对于 scroll_area.viewport() 的局部坐标）"""
        if delta_y == 0:
            return

        zoom_in_factor = 1.15
        zoom_out_factor = 1 / 1.15
        old_scale = self.scale_factor
        factor = zoom_in_factor if delta_y > 0 else zoom_out_factor

        h = self.scroll_area.horizontalScrollBar().value()
        v = self.scroll_area.verticalScrollBar().value()
        pos_on_label = QPointF(point_under_cursor.x() + h, point_under_cursor.y() + v)

        if old_scale != 0:
            pos_on_label /= old_scale

        MIN_SCALE = 0.05
        MAX_SCALE = 8.0
        self.scale_factor = max(MIN_SCALE, min(MAX_SCALE, self.scale_factor * factor))

        self.update_image_scale()

        new_pos_on_label = pos_on_label * self.scale_factor
        new_scrollbar_x = new_pos_on_label.x() - point_under_cursor.x()
        new_scrollbar_y = new_pos_on_label.y() - point_under_cursor.y()

        self.scroll_area.horizontalScrollBar().setValue(int(new_scrollbar_x))
        self.scroll_area.verticalScrollBar().setValue(int(new_scrollbar_y))

    def eventFilter(self, source, event):
        if source == self.scroll_area.viewport():
            if event.type() == event.Type.MouseButtonPress and event.button() == Qt.MouseButton.LeftButton:
                self.is_panning = True
                self.last_mouse_pos = event.globalPosition().toPoint()
                self.scroll_area.viewport().setCursor(Qt.CursorShape.ClosedHandCursor)
                return True
            elif event.type() == event.Type.MouseButtonRelease and event.button() == Qt.MouseButton.LeftButton:
                self.is_panning = False
                self.scroll_area.viewport().setCursor(Qt.CursorShape.OpenHandCursor)
                return True
            elif event.type() == event.Type.MouseMove and self.is_panning:
                delta = event.globalPosition().toPoint() - self.last_mouse_pos
                self.last_mouse_pos = event.globalPosition().toPoint()
                self.scroll_area.horizontalScrollBar().setValue(self.scroll_area.horizontalScrollBar().value() - delta.x())
                self.scroll_area.verticalScrollBar().setValue(self.scroll_area.verticalScrollBar().value() - delta.y())
                return True

            elif event.type() == event.Type.Wheel:
                delta_y = 0
                try:
                    delta = event.angleDelta()
                    if not delta.isNull():
                        delta_y = delta.y()
                    else:
                        delta_y = event.pixelDelta().y()
                except Exception:
                    delta_y = event.angleDelta().y() if hasattr(event, 'angleDelta') else 0

                if hasattr(event, 'position'):
                    local_pt = event.position().toPoint()
                else:
                    local_pt = event.pos()

                self._perform_zoom_at(delta_y, local_pt)
                event.accept()
                return True

        return super().eventFilter(source, event)

    def wheelEvent(self, event):
        try:
            delta_y = event.angleDelta().y() if hasattr(event, 'angleDelta') else 0
        except Exception:
            delta_y = 0
        point_under_cursor = self.scroll_area.mapFromGlobal(QCursor.pos())
        if delta_y != 0:
            self._perform_zoom_at(delta_y, point_under_cursor)
            event.accept()
        else:
            super().wheelEvent(event)


class ClickableLabel(QLabel):
    """可点击的QLabel"""
    clicked = Signal()
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pixmap_cache = None

    def mousePressEvent(self, event):
        self.clicked.emit()


class FontSelectionWidget(QWidget):
    """封装的字体选择控件"""
    def __init__(self, title, default_font=QFont("Microsoft YaHei", 42)):
        super().__init__()
        self.font = default_font
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 5, 0, 5)
        
        title_label = QLabel(f"<b>{title}</b>")
        layout.addWidget(title_label)

        self.font_display_label = QLabel()
        self.font_display_label.setMinimumHeight(30)
        
        btn_layout = QHBoxLayout()
        select_system_button = QPushButton("选择系统字体...")
        select_system_button.clicked.connect(self.select_system_font)
        browse_font_button = QPushButton("浏览文件...")
        browse_font_button.clicked.connect(self.select_font_file)
        
        btn_layout.addWidget(self.font_display_label, 1)
        btn_layout.addWidget(select_system_button)
        btn_layout.addWidget(browse_font_button)
        layout.addLayout(btn_layout)
        
        self.update_font_display()

    def select_system_font(self):
        ok, font = QFontDialog.getFont(self.font, self, "选择字体")
        if ok:
            self.font = font
            self.update_font_display()

    def select_font_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "选择字体文件", "", "字体文件 (*.ttf *.otf)")
        if path:
            font_id = QFontDatabase.addApplicationFont(path)
            if font_id != -1:
                family = QFontDatabase.applicationFontFamilies(font_id)[0]
                self.font.setFamily(family)
                self.update_font_display()
            else:
                QMessageBox.warning(self, "错误", "无法加载字体文件。")

    def update_font_display(self):
        style = []
        if self.font.bold(): style.append("粗体")
        if self.font.italic(): style.append("斜体")
        style_str = ", ".join(style) if style else "常规"
        self.font_display_label.setText(f"{self.font.family()}, {self.font.pointSize()}pt, {style_str}")

    def get_font(self):
        return self.font


class CharacterInputDialog(QDialog):
    """自定义字符输入对话框，支持64字符固定宽度换行"""
    def __init__(self, parent=None, initial_text=""):
        super().__init__(parent)
        self.setWindowTitle("输入字符")
        self.setMinimumSize(520, 400)

        layout = QVBoxLayout(self)
        label = QLabel("请输入需要生成的字符 (可粘贴):")
        layout.addWidget(label)

        self.text_edit = QTextEdit()
        font = QFont("Consolas", 12)
        self.text_edit.setFont(font)
        self.text_edit.setLineWrapMode(QTextEdit.LineWrapMode.FixedColumnWidth)
        self.text_edit.setLineWrapColumnOrWidth(64)
        self.text_edit.setPlainText(initial_text)

        layout.addWidget(self.text_edit, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("确定")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)


class FontGeneratorDialog(QDialog):
    """最终版字体贴图生成器对话框"""
    def __init__(self, parent=None, initial_chars="", initial_version="IV"):
        super().__init__(parent)
        self.setWindowTitle("GTA 字体贴图生成器")
        self.setMinimumSize(640, 700)
        self.gxt_editor = parent
        self.generator = FontTextureGenerator()
        self.characters = initial_chars

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        
        settings_group = QGroupBox("基本设置")
        settings_layout = QVBoxLayout(settings_group)
        settings_layout.setSpacing(6)
        
        top_row = QHBoxLayout()
        top_row.setSpacing(10)
        
        ver_layout = QVBoxLayout()
        ver_layout.setSpacing(4)
        ver_layout.addWidget(QLabel("游戏版本:"))
        self.version_combo = QComboBox()
        self.version_combo.addItems(["GTA IV", "GTA San Andreas", "GTA Vice City", "GTA III"])
        self.version_combo.currentTextChanged.connect(self.update_ui_for_version)
        ver_layout.addWidget(self.version_combo)
        top_row.addLayout(ver_layout)
        
        res_layout = QVBoxLayout()
        res_layout.setSpacing(4)
        res_layout.addWidget(QLabel("分辨率:"))
        self.res_combo = QComboBox()
        self.res_combo.addItems(["4096x4096", "2048x2048"])
        res_layout.addWidget(self.res_combo)
        top_row.addLayout(res_layout)
        
        top_row.addStretch()
        settings_layout.addLayout(top_row)
        
        self.font_normal_widget = FontSelectionWidget("字体设置", QFont("Microsoft YaHei", 42, QFont.Weight.Bold))
        settings_layout.addWidget(self.font_normal_widget)
        
        layout.addWidget(settings_group)
        
        chars_group = QGroupBox("字符操作")
        chars_layout = QVBoxLayout(chars_group)
        chars_layout.setSpacing(6)
        
        char_btn_layout = QHBoxLayout()
        char_btn_layout.setSpacing(5)
        
        self.btn_load_from_gxt = QPushButton("从GXT加载")
        self.btn_load_from_gxt.setToolTip("从当前GXT加载特殊字符")
        self.btn_load_from_gxt.clicked.connect(self.load_chars_from_parent)
        
        self.btn_import_chars = QPushButton("导入文件")
        self.btn_import_chars.setToolTip("导入字符文件")
        self.btn_import_chars.clicked.connect(self.import_char_file)
        
        self.btn_input_chars = QPushButton("输入字符")
        self.btn_input_chars.setToolTip("手动输入字符")
        self.btn_input_chars.clicked.connect(self.input_chars_manually)
        
        char_btn_layout.addWidget(self.btn_load_from_gxt)
        char_btn_layout.addWidget(self.btn_import_chars)
        char_btn_layout.addWidget(self.btn_input_chars)
        self.btn_import_wm_vcchs = QPushButton('导入 wm_vcchs.dat')
        self.btn_import_wm_vcchs.setToolTip('从 wm_vcchs.dat 文件提取字符')
        self.btn_import_wm_vcchs.clicked.connect(self.import_wm_vcchs)
        char_btn_layout.addWidget(self.btn_import_wm_vcchs)

        self.btn_import_char_table = QPushButton('导入 char_table.dat')
        self.btn_import_char_table.setToolTip('从 char_table.dat 文件提取字符')
        self.btn_import_char_table.clicked.connect(self.import_char_table)
        char_btn_layout.addWidget(self.btn_import_char_table)
        char_btn_layout.addStretch()
        
        chars_layout.addLayout(char_btn_layout)
        
        self.char_info_layout = QHBoxLayout()
        self.char_count_label = QLabel("字符数: 0")
        self.char_info_layout.addWidget(self.char_count_label)
        self.char_info_layout.addStretch()
        self.btn_show_chars = QPushButton("查看字符列表")
        self.btn_show_chars.clicked.connect(self.show_chars_list)
        self.char_info_layout.addWidget(self.btn_show_chars)
        chars_layout.addLayout(self.char_info_layout)
        
        layout.addWidget(chars_group)
        
        self.update_char_count()

        preview_group = QGroupBox("预览")
        preview_layout = QVBoxLayout(preview_group)
        preview_layout.setSpacing(6)
        
        preview_btn_layout = QHBoxLayout()
        self.preview_button = QPushButton("刷新预览")
        self.preview_button.clicked.connect(self.update_previews)
        preview_btn_layout.addWidget(self.preview_button)
        preview_btn_layout.addStretch()
        preview_layout.addLayout(preview_btn_layout)
        
        preview_label_layout = QHBoxLayout()
        preview_label_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.preview_normal_label = ClickableLabel("点击'刷新预览'以生成")
        self.preview_normal_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview_normal_label.setMinimumSize(280, 280)
        self.preview_normal_label.setMaximumSize(280, 280)
        self.preview_normal_label.setStyleSheet("""
            border: 1px solid #555; 
            background-color: #2a2a2a;
            border-radius: 4px;
        """)
        self.preview_normal_label.clicked.connect(lambda: self.show_full_preview(self.preview_normal_label))
        
        preview_label_layout.addWidget(self.preview_normal_label)
        preview_layout.addLayout(preview_label_layout)
        
        layout.addWidget(preview_group)

        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel, self)
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setText("生成文件")
        self.buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

        ver_map = {"IV": "GTA IV", "VC": "GTA Vice City", "SA": "GTA San Andreas", "III": "GTA III"}
        if initial_version in ver_map:
            self.version_combo.setCurrentText(ver_map[initial_version])
            
        self.update_ui_for_version()

    def show_full_preview(self, label):
        if label.pixmap_cache and not label.pixmap_cache.isNull():
            viewing_pixmap = label.pixmap_cache.scaled(
                2048, 2048,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation
            )
            viewer = ImageViewer(viewing_pixmap, "字体贴图预览", self)
            viewer.exec()

    def update_ui_for_version(self):
        pass

    def update_previews(self):
        settings = self.get_settings()
        if not settings["characters"]:
            QMessageBox.warning(self, "提示", "字符不能为空，无法预览。")
            return
        
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            pixmap_normal = self.generator.create_pixmap(settings["characters"], settings["version"], settings["resolution"], settings["font_normal"])
            if self.preview_normal_label:
                self.display_pixmap(self.preview_normal_label, pixmap_normal)
        finally:
            QApplication.restoreOverrideCursor()
            
    def display_pixmap(self, label, pixmap):
        if not pixmap.isNull():
            label.pixmap_cache = pixmap
            label.setPixmap(pixmap.scaled(label.size(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))
            label.setText("")
        else:
            label.pixmap_cache = None
            label.setText("生成失败")

    def load_chars_from_parent(self):
        """从父窗口（GXT编辑器）加载字符，使用对应版本的字符收集逻辑"""
        if self.gxt_editor and hasattr(self.gxt_editor, 'collect_and_filter_chars'):
            current_version = self.gxt_editor.version if hasattr(self.gxt_editor, 'version') else "IV"
        
            chars = self.gxt_editor.collect_and_filter_chars()
            if chars:
                self.characters = chars
                self.update_char_count()
            
                ver_map = {"IV": "GTA IV", "VC": "GTA Vice City", "SA": "GTA San Andreas", "III": "GTA III"}
                if current_version in ver_map:
                    self.version_combo.setCurrentText(ver_map[current_version])
            
                QMessageBox.information(self, "成功", 
                                      f"已从当前GXT加载 {len(chars)} 个特殊字符。\n"
                                      f"版本: {current_version}")
            else:
                QMessageBox.warning(self, "提示", "当前GXT中未找到符合条件的特殊字符。")

    def import_char_file(self):
            """导入字符文件 (支持多种编码并自动排序)"""
            path, _ = QFileDialog.getOpenFileName(self, "导入字符文件", "", "文本文件 (*.txt);;所有文件 (*.*)")
            if not path: return

            encodings_to_try = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'utf-16', 'big5', 'latin-1']
            content = None
            detected_encoding = None

            for encoding in encodings_to_try:
                try:
                    with open(path, 'r', encoding=encoding) as f:
                        content = f.read()
                    detected_encoding = encoding
                    break
                except (UnicodeDecodeError, UnicodeError):
                    continue
                except Exception as e:
                    QMessageBox.critical(self, "读取失败", f"读取文件时发生意外错误: {str(e)}")
                    return

            if content is not None:
                chars = content.replace("\n", "").replace(" ", "")
                unique_sorted_chars = "".join(sorted(list(set(chars))))
                self.characters = unique_sorted_chars
                self.update_char_count()
                QMessageBox.information(self, "导入成功", f"已导入 {len(unique_sorted_chars)} 个字符 (编码: {detected_encoding}, 已排序)")
            else:
                QMessageBox.critical(self, "导入失败", "无法识别的文件编码。\n请确保文件是常见的文本编码格式 (如 UTF-8, GBK, UTF-16 等)。")

    def input_chars_manually(self):
            """手动输入字符 (自动排序)"""
            dlg = CharacterInputDialog(self, self.characters)
            if dlg.exec() == QDialog.DialogCode.Accepted:
                text = dlg.text_edit.toPlainText()
                if text:
                    chars_no_whitespace = text.replace("\n", "").replace(" ", "")
                    unique_sorted_chars = "".join(sorted(list(set(chars_no_whitespace))))
                    self.characters = unique_sorted_chars
                    self.update_char_count()
                    QMessageBox.information(self, "成功", f"已设置 {len(unique_sorted_chars)} 个字符 (已按Unicode排序)")

    def show_chars_list(self):
            """显示字符列表对话框"""
            if not self.characters:
                QMessageBox.information(self, "字符列表", "当前没有字符")
                return
            
            dlg = QDialog(self)
            dlg.setWindowTitle("字符列表")
            dlg.setMinimumSize(520, 400)
        
            layout = QVBoxLayout(dlg)
        
            text_edit = QTextEdit()
            text_edit.setReadOnly(True)
        
            font = QFont("Consolas", 12)
            text_edit.setFont(font)
            text_edit.setLineWrapMode(QTextEdit.LineWrapMode.FixedColumnWidth)
            text_edit.setLineWrapColumnOrWidth(64)
            text_edit.setPlainText(self.characters)
        
            layout.addWidget(text_edit)
        
            char_count = len(self.characters)
            unique_count = len(set(self.characters))
            info_label = QLabel(f"字符总数: {char_count} | 唯一字符数: {unique_count}")
            layout.addWidget(info_label)
        
            btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
            btn_box.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
            btn_box.rejected.connect(dlg.reject)
            layout.addWidget(btn_box)
        
            dlg.exec()

    def update_char_count(self):
            """更新字符数量显示"""
            char_count = len(self.characters)
            unique_count = len(set(self.characters))
            self.char_count_label.setText(f"字符总数: {char_count} | 唯一字符数: {unique_count}")

    def get_settings(self):
            ver_map = {"GTA IV": "IV", "GTA San Andreas": "SA", "GTA Vice City": "VC", "GTA III": "III"}
            version = ver_map.get(self.version_combo.currentText())
        
            settings = {
                "version": version,
                "resolution": int(self.res_combo.currentText().split('x')[0]),
                "characters": self.characters,
                "font_normal": self.font_normal_widget.get_font(),
            }
            return settings

    def import_wm_vcchs(self):
        path, _ = QFileDialog.getOpenFileName(self, "导入 wm_vcchs.dat 或 Chinese.dat 文件", "", "VC字库 (wm_vcchs.dat Chinese.dat)")
        if not path:
            return
        try:
            chars = set()
            with open(path, 'rb') as f:
                data = f.read()
                for i in range(0, len(data), 2):
                    row, col = data[i], data[i + 1]
                    if not (row == 63 and col == 63):
                        code = i // 2
                        chars.add(chr(code))
            if chars:
                sorted_chars = "".join(sorted(chars))
                self.characters = sorted_chars
                self.update_char_count()
                QMessageBox.information(self, "导入成功", f"已从 wm_vcchs.dat（Chinese.dat） 读取 {len(chars)} 个字符。")
            else:
                QMessageBox.warning(self, "提示", "未提取到任何有效字符。")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"解析文件失败：{str(e)}")

    def import_char_table(self):
        path, _ = QFileDialog.getOpenFileName(self, "导入 char_table.dat 文件", "", "GTA4字库 (char_table.dat)")
        if not path:
            return
        try:
            with open(path, 'rb') as f:
                count_bytes = f.read(4)
                if len(count_bytes) < 4:
                    raise ValueError("文件格式错误")
                count = int.from_bytes(count_bytes, 'little')
                chars = []
                for _ in range(count):
                    code_bytes = f.read(4)
                    if len(code_bytes) < 4:
                        break
                    code = int.from_bytes(code_bytes, 'little')
                    chars.append(chr(code))
            if chars:
                self.characters = "".join(sorted(set(chars)))
                self.update_char_count()
                QMessageBox.information(self, "导入成功", f"已从 char_table.dat 读取 {len(chars)} 个字符。")
            else:
                QMessageBox.warning(self, "提示", "文件中没有有效字符。")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"解析文件失败：{str(e)}")


class CodepageConverterDialog(QDialog):
    """码表转换工具对话框"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.gxt_editor = parent
        self.setWindowTitle("码表转换工具")
        self.setMinimumSize(600, 500)

        self.forward_map = {}
        self.reverse_map = {}
        self.current_table_path = None

        layout = QVBoxLayout(self)
        
        load_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        chinese_group = QGroupBox("GXT_Tables（Chinese）")
        chinese_layout = QVBoxLayout(chinese_group)
        self.chinese_list_widget = QListWidget()
        self.chinese_list_widget.itemClicked.connect(self.on_list_item_clicked)
        chinese_layout.addWidget(self.chinese_list_widget)
        load_splitter.addWidget(chinese_group)
        
        original_group = QGroupBox("GXT_Tables（original）")
        original_layout = QVBoxLayout(original_group)
        self.original_list_widget = QListWidget()
        self.original_list_widget.itemClicked.connect(self.on_list_item_clicked)
        original_layout.addWidget(self.original_list_widget)
        load_splitter.addWidget(original_group)

        layout.addWidget(load_splitter, 1)

        self.status_label = QLabel("请从上方列表中选择一个码表文件。")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.status_label)
        
        action_group = QGroupBox("执行转换")
        action_layout = QHBoxLayout(action_group)
        action_layout.setSpacing(10)

        self.apply_button = QPushButton("◀️ 用码表解密 (新字符 -> 原文)")
        self.apply_button.setToolTip("将GXT中的码表字符，根据映射还原为原始可读字符。")
        self.apply_button.clicked.connect(lambda: self.run_conversion(reverse=True))
        
        self.revert_button = QPushButton("▶️ 用码表加密 (原文 -> 新字符)")
        self.revert_button.setToolTip("将GXT中的原始字符，根据映射转换为码表中的新字符。")
        self.revert_button.clicked.connect(lambda: self.run_conversion(reverse=False))
        self.view_table_button = QPushButton("👁️ 查看当前码表")
        self.view_table_button.setToolTip("查看已加载码表文件的映射内容。")
        self.view_table_button.clicked.connect(self.view_current_table)

        action_layout.addWidget(self.apply_button)
        action_layout.addWidget(self.revert_button)
        action_layout.addStretch()
        action_layout.addWidget(self.view_table_button)
        layout.addWidget(action_group)
        
        self.apply_button.setEnabled(False)
        self.revert_button.setEnabled(False)
        self.view_table_button.setEnabled(False)

        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, self)
        self.buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

        self._populate_table_lists()

    def _get_base_path(self):
        """获取程序根目录（支持.py和打包后的.exe）"""
        if getattr(sys, 'frozen', False):
            if hasattr(sys, '_MEIPASS'):
                return Path(sys._MEIPASS)
            return Path(sys.executable).parent
        else:
            return Path(__file__).resolve().parent

    def _populate_table_lists(self):
        """自动扫描并填充两个码表列表"""
        base_path = self._get_base_path()
        chinese_dir = base_path / "GXT_Tables（Chinese）"
        original_dir = base_path / "GXT_Tables（original）"

        self.chinese_list_widget.clear()
        if chinese_dir.is_dir():
            for txt_file in sorted(chinese_dir.glob("*.txt")):
                item = QListWidgetItem(txt_file.stem)
                item.setData(Qt.ItemDataRole.UserRole, str(txt_file))
                self.chinese_list_widget.addItem(item)
        
        self.original_list_widget.clear()
        if original_dir.is_dir():
            for txt_file in sorted(original_dir.glob("*.txt")):
                item = QListWidgetItem(txt_file.stem)
                item.setData(Qt.ItemDataRole.UserRole, str(txt_file))
                self.original_list_widget.addItem(item)

        if self.chinese_list_widget.count() == 0 and self.original_list_widget.count() == 0:
            self.status_label.setText("未在程序目录下找到码表文件夹。")

    def on_list_item_clicked(self, item):
        """处理列表点击事件，加载码表"""
        sender_list = self.sender()
        if sender_list == self.chinese_list_widget:
            self.original_list_widget.clearSelection()
        else:
            self.chinese_list_widget.clearSelection()
            
        path = item.data(Qt.ItemDataRole.UserRole)
        if path:
            self._load_table_file(str(path))

    def _load_table_file(self, path):
        """从指定路径加载和解析码表文件"""
        self.forward_map.clear()
        self.reverse_map.clear()
        self.current_table_path = None
        
        try:
            with open(path, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    parsed = self._parse_line(line)
                    if parsed:
                        source_char, dest_char = parsed
                        self.forward_map[source_char] = dest_char
                        self.reverse_map[dest_char] = source_char

            if self.forward_map:
                self.current_table_path = path
                self.status_label.setText(f"加载成功: {Path(path).name} (共 {len(self.forward_map)} 条映射)")
                self.apply_button.setEnabled(True)
                self.revert_button.setEnabled(True)
                self.view_table_button.setEnabled(True)    
            else:
                self.status_label.setText(f"加载失败或文件为空: {Path(path).name}")
                self.apply_button.setEnabled(False)
                self.revert_button.setEnabled(False)
                self.view_table_button.setEnabled(False)
                QMessageBox.warning(self, "警告", "未能从文件中解析出任何有效的映射规则。")

        except Exception as e:
            QMessageBox.critical(self, "错误", f"读取或解析文件失败: {e}")
            self.status_label.setText(f"加载失败: {e}")
            self.apply_button.setEnabled(False)
            self.revert_button.setEnabled(False)
            self.view_table_button.setEnabled(False)

    def _parse_line(self, line):
        line = line.strip()
        if not line or line.startswith('#') or line.startswith('//'):
            return None

        try:
            match = re.fullmatch(r'([0-9a-fA-F]+)\s*=\s*([0-9a-fA-F]+)', line)
            if match:
                key_code = int(match.group(1), 16)
                val_code = int(match.group(2), 16)
                return chr(key_code), chr(val_code)

            match = re.fullmatch(r'\s*(.)\s+([0-9a-fA-F]+)\s*', line, re.UNICODE)
            if match:
                char = match.group(1)
                val_code = int(match.group(2), 16)
                return char, chr(val_code)
        except (ValueError, IndexError):
            return None
            
        return None

    def view_current_table(self):
        if not self.forward_map:
            QMessageBox.information(self, "提示", "当前没有加载码表。")
            return

        dialog = QDialog(self)
        dialog.setWindowTitle(f"码表内容: {Path(self.current_table_path).name}")
        dialog.setMinimumSize(450, 600)

        layout = QVBoxLayout(dialog)
        
        table = QTableWidget()
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(["原文字符 (Original)", "加密字符 (Mapped)"])
        table.setRowCount(len(self.forward_map))
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        for i, (original_char, mapped_char) in enumerate(self.forward_map.items()):
            table.setItem(i, 0, QTableWidgetItem(f"{original_char} (U+{ord(original_char):04X})"))
            table.setItem(i, 1, QTableWidgetItem(f"{mapped_char} (U+{ord(mapped_char):04X})"))
        
        layout.addWidget(table)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        dialog.exec()

    def run_conversion(self, reverse=False):
        if not self.current_table_path or not self.gxt_editor:
            QMessageBox.warning(self, "错误", "请先从列表中选择一个码表文件。")
            return
            
        mapping = self.reverse_map if reverse else self.forward_map
        op_name = "解密 (新字符 -> 原文)" if reverse else "加密 (原文 -> 新字符)"

        
        reply = QMessageBox.question(self, "确认操作",
                                     f"确定要对当前所有GXT数据执行“{op_name}”吗？\n此操作将直接修改内存中的数据。",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.No:
            return

        progress = QProgressDialog(f"正在执行 {op_name}...", "取消", 0, len(self.gxt_editor.data), self)
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.show()

        unmapped_chars = set()
        gxt_data = self.gxt_editor.data
        
        processed_tables = 0
        for table_name, table_content in gxt_data.items():
            if progress.wasCanceled():
                break
            progress.setValue(processed_tables)
            progress.setLabelText(f"正在处理表: {table_name}")
            
            for key, value in table_content.items():
                new_value = []
                for char in value:
                    if char in mapping:
                        new_value.append(mapping[char])
                    else:
                        new_value.append(char)
                        check_map = self.forward_map if reverse else self.reverse_map
                        if char not in check_map:
                             unmapped_chars.add(char)
                gxt_data[table_name][key] = "".join(new_value)
            processed_tables += 1
        
        progress.setValue(len(self.gxt_editor.data))
        
        if progress.wasCanceled():
            self.gxt_editor.refresh_keys()
            QMessageBox.information(self, "已取消", "操作已被用户取消。")
            return

        self.gxt_editor.set_modified(True)
        if self.gxt_editor.global_search_button.isChecked():
            self.gxt_editor.search_key_value()
        else:
            self.gxt_editor.refresh_keys()
        
        if unmapped_chars:
            sorted_unmapped = sorted(list(unmapped_chars))
            char_list_str = "".join(sorted_unmapped)
            
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Icon.Warning)
            msg_box.setWindowTitle("转换警告")
            msg_box.setText(f"转换完成，但有 {len(unmapped_chars)} 个字符在码表中未找到，已保持原样。")
            msg_box.setDetailedText("未映射的字符列表 (已排除码表中的对应字符):\n" + char_list_str)
            msg_box.exec()
        else:
            QMessageBox.information(self, "成功", f"{op_name} 已成功完成！")


class WhmLogEmitter(QObject):
    """将文本输出重定向到Qt信号"""
    message_written = Signal(str)

    def write(self, text):
        timestamp = datetime.now().strftime("%H:%M:%S")
        text = str(text).rstrip('\n')
        if text:
            self.message_written.emit(f"[{timestamp}] {text}")

    def flush(self):
        pass


class WhmBatchWorker(QThread):
    """在后台线程执行WHM批量任务的Worker"""
    finished_with_details = Signal(str, object, int)
    progress_updated = Signal(int, int, str)

    def __init__(self, mode: str, input_path: Path, output_path: Path or None, exporter_instance: CHtmlTextExport):
        super().__init__()
        self.mode = mode
        self.input_path = input_path
        self.output_path = output_path
        self.exporter = exporter_instance
        self.log_queue = Queue()
        self.exported_count = 0

    def _process_single_whm(self, file_path: Path):
        """处理单个WHM文件并记录日志到队列"""
        self.log_queue.put(f"处理: {file_path.name}")
        try:
            container = self.exporter.ExtractWhmStrings(file_path, set())
            if container:
                output_txt_path = file_path.with_suffix(".txt")
                counter = 1
                original_stem = output_txt_path.stem
                while output_txt_path.exists():
                    output_txt_path = file_path.parent / f"{original_stem}_{counter}.txt"
                    counter += 1
                self.exporter.ExportText(output_txt_path, container)
                self.exported_count += 1
                self.log_queue.put(f"处理完成: {file_path.name}")
        except Exception as e:
            self.log_queue.put(f"处理失败: {file_path.name}: {e}")

    def run(self):
        output_result_path = None
        try:
            if self.mode == 'export':
                self.log_queue.put(f"开始导出: {self.input_path}")
                whm_files = list(self.input_path.rglob("*.whm"))
                total_files = len(whm_files)
                if total_files == 0:
                    self.log_queue.put("未找到 WHM 文件")
                    return

                with ThreadPoolExecutor(max_workers=os.cpu_count() or 1) as executor:
                    futures = [executor.submit(self._process_single_whm, fp) for fp in whm_files]
                    for i, future in enumerate(futures):
                        future.result()
                        self.progress_updated.emit(i + 1, total_files, whm_files[i].name)
                        while not self.log_queue.empty():
                            print(self.log_queue.get())
                self.log_queue.put(f"导出完成: 共处理 {total_files} 个文件")

            elif self.mode == 'gendb':
                self.log_queue.put(f"开始生成数据库: {self.input_path}")
                txt_files = list(self.input_path.rglob("*.txt"))
                total_files = len(txt_files)
                
                for i, file_path in enumerate(txt_files):
                    self.progress_updated.emit(i + 1, total_files, file_path.name)
                    self.log_queue.put(f"扫描: {file_path.name}")
                
                self.exporter.GenerateDataBase(self.input_path, self.output_path)
                output_result_path = self.output_path
                self.log_queue.put(f"数据库生成完成: 共处理 {total_files} 个文件")

        except Exception as e:
            self.log_queue.put(f"错误: 发生异常 {str(e)}")
        finally:
            while not self.log_queue.empty():
                print(self.log_queue.get())
            self.finished_with_details.emit(self.mode, output_result_path, self.exported_count)


class WhmBatchToolDialog(QDialog):
    """WHM批量工具的主对话框"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.whm_exporter = CHtmlTextExport()
        self.worker_thread = None
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        self.recent_paths = []

        self.setWindowTitle("WHM 文本提取工具")
        self.setMinimumSize(900, 750)

        self.folder_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon)
        self.file_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)

        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._create_viewer_tab(), "WHM 文件浏览器")
        self.tabs.addTab(self._create_batch_tab(), "批量处理")
        main_layout.addWidget(self.tabs, 1)

        self.log_emitter = WhmLogEmitter()
        self.log_emitter.message_written.connect(self._append_log)
        self.tabs.currentChanged.connect(self._on_tab_changed)

    def _append_log(self, text):
        self.log_view.append(text)
        self.log_view.verticalScrollBar().setValue(self.log_view.verticalScrollBar().maximum())

    def _on_tab_changed(self, index):
        is_batch_tab = (self.tabs.widget(index) == self.batch_tab_widget)
        self.log_label.setVisible(is_batch_tab)
        self.log_view.setVisible(is_batch_tab)

    def _create_viewer_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        top_layout = QHBoxLayout()
        btn_load_whm_root = QPushButton("📂 打开 .whm 文件夹...")
        btn_load_whm_root.setToolTip("选择包含所有网站子文件夹的根目录 (例如: .../pc/html)")
        btn_load_whm_root.clicked.connect(self.browse_and_load_whm_tree)
        
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("🔍 搜索文件名...")
        self.search_edit.textChanged.connect(self._filter_tree)
        
        top_layout.addWidget(btn_load_whm_root)
        top_layout.addWidget(self.search_edit, 1)
        layout.addLayout(top_layout)
        
        viewer_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        self.whm_tree = QTreeWidget()
        self.whm_tree.setHeaderLabels(["文件/文件夹"])
        self.whm_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.whm_tree.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.whm_tree.itemClicked.connect(self.on_whm_tree_item_selected)
        self.whm_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.whm_tree.customContextMenuRequested.connect(self.show_whm_viewer_context_menu)
        viewer_splitter.addWidget(self.whm_tree)

        right_splitter = QSplitter(Qt.Orientation.Vertical)
        
        self.whm_table = QTableWidget()
        self.whm_table.setColumnCount(2)
        self.whm_table.setHorizontalHeaderLabels(["哈希 (Hash)", "文本预览 (Value Preview)"])
        self.whm_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.whm_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.whm_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.whm_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.whm_table.itemSelectionChanged.connect(self.on_whm_table_selection_changed)
        right_splitter.addWidget(self.whm_table)

        viewer_container = QWidget()
        viewer_layout = QVBoxLayout(viewer_container)
        viewer_layout.setContentsMargins(0, 5, 0, 0)
        viewer_layout.setSpacing(5)

        self.whm_value_viewer_label = QLabel("文本显示区：")
        viewer_layout.addWidget(self.whm_value_viewer_label)
        
        self.whm_value_viewer = QTextEdit()
        self.whm_value_viewer.setReadOnly(True)
        self.whm_value_viewer.setPlaceholderText("在此处查看完整的文本值...")
        viewer_layout.addWidget(self.whm_value_viewer)

        right_splitter.addWidget(viewer_container)

        right_splitter.setSizes([400, 200])
        viewer_splitter.addWidget(right_splitter)
        viewer_splitter.setSizes([250, 600])
        
        layout.addWidget(viewer_splitter, 1)
        return tab

    def _create_batch_tab(self) -> QWidget:
        self.batch_tab_widget = QWidget()
        layout = QVBoxLayout(self.batch_tab_widget)
        layout.setSpacing(15)

        info_label = QLabel("此工具支持从 WHM 文件批量导出 TXT，或从 TXT 文件批量生成 whm_table.dat 数据库。")
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        export_group = QGroupBox("从 WHM 批量导出 TXT")
        export_layout = QFormLayout(export_group)
        self.export_input_combo = QComboBox()
        self.export_input_combo.setEditable(True)
        self.export_input_combo.lineEdit().setPlaceholderText("选择包含 .whm 文件的根文件夹 (例如: pc/html)")
        export_browse_btn = QPushButton("浏览...")
        export_browse_btn.clicked.connect(self.browse_whm_root)
        export_input_layout = QHBoxLayout()
        export_input_layout.addWidget(self.export_input_combo, 1)
        export_input_layout.addWidget(export_browse_btn)
        export_layout.addRow("WHM 根文件夹:", export_input_layout)
        
        self.export_run_btn = QPushButton("🚀 开始导出")
        self.export_run_btn.clicked.connect(self.run_export)
        self.export_progress_bar = QProgressBar()
        self.export_progress_bar.setVisible(False)
        export_layout.addRow(self.export_run_btn, self.export_progress_bar)
        
        layout.addWidget(export_group)

        gendb_group = QGroupBox("从 TXT 批量生成数据库")
        gendb_layout = QFormLayout(gendb_group)
        self.gendb_input_combo = QComboBox()
        self.gendb_input_combo.setEditable(True)
        self.gendb_input_combo.lineEdit().setPlaceholderText("选择包含 .txt 文件的根文件夹")
        gendb_browse_input_btn = QPushButton("浏览...")
        gendb_browse_input_btn.clicked.connect(self.browse_txt_root)
        gendb_input_layout = QHBoxLayout()
        gendb_input_layout.addWidget(self.gendb_input_combo, 1)
        gendb_input_layout.addWidget(gendb_browse_input_btn)
        gendb_layout.addRow("TXT 根文件夹:", gendb_input_layout)

        self.gendb_output_combo = QComboBox()
        self.gendb_output_combo.setEditable(True)
        self.gendb_output_combo.lineEdit().setPlaceholderText("选择 whm_table.dat 的保存位置")
        gendb_browse_output_btn = QPushButton("另存为...")
        gendb_browse_output_btn.clicked.connect(self.browse_gendb_output)
        gendb_output_layout = QHBoxLayout()
        gendb_output_layout.addWidget(self.gendb_output_combo, 1)
        gendb_output_layout.addWidget(gendb_browse_output_btn)
        gendb_layout.addRow("输出数据库:", gendb_output_layout)
        
        self.gendb_run_btn = QPushButton("🛠️ 开始生成")
        self.gendb_run_btn.clicked.connect(self.run_gendb)
        self.gendb_progress_bar = QProgressBar()
        self.gendb_progress_bar.setVisible(False)
        gendb_layout.addRow(self.gendb_run_btn, self.gendb_progress_bar)

        layout.addWidget(gendb_group)
        
        self.log_label = QLabel("处理日志:")
        layout.addWidget(self.log_label)
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Consolas", 10))
        layout.addWidget(self.log_view, 1)

        self.log_label.hide()
        self.log_view.hide()

        return self.batch_tab_widget

    def _filter_tree(self, text):
        text = text.lower()
        iterator = QTreeWidgetItemIterator(self.whm_tree)
        while iterator.value():
            item = iterator.value()
            if item.childCount() == 0:
                is_visible = text in item.text(0).lower()
                item.setHidden(not is_visible)
            iterator += 1
        
        for i in range(self.whm_tree.topLevelItemCount()):
            website_item = self.whm_tree.topLevelItem(i)
            has_visible_child = False
            for j in range(website_item.childCount()):
                if not website_item.child(j).isHidden():
                    has_visible_child = True
                    break
            website_item.setHidden(not has_visible_child)
            if has_visible_child and text:
                website_item.setExpanded(True)
            elif not text:
                 website_item.setExpanded(False)

    def browse_and_load_whm_tree(self):
        path_str = QFileDialog.getExistingDirectory(self, "选择 WHM 根文件夹 (例如: pc/html)")
        if not path_str: return

        self.whm_tree.clear()
        self.whm_table.setRowCount(0)
        self.whm_value_viewer.clear()
        self.search_edit.clear()

        root_path = Path(path_str)
        if not root_path.is_dir(): return
        
        self._add_to_recent_paths(path_str)

        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self.whm_tree.setUpdatesEnabled(False)
        try:
            whm_files_in_root = list(root_path.glob("*.whm"))

            dirs_to_process = []
            if whm_files_in_root:
                dirs_to_process = [root_path]
            else:
                dirs_to_process = sorted([d for d in root_path.iterdir() if d.is_dir()], key=lambda p: p.name)

            for website_dir in dirs_to_process:
                website_item = QTreeWidgetItem(self.whm_tree, [website_dir.name])
                website_item.setIcon(0, self.folder_icon)
                
                has_whm_files = False
                for whm_file in sorted(website_dir.glob("*.whm")):
                    has_whm_files = True
                    file_item = QTreeWidgetItem(website_item, [whm_file.name])
                    file_item.setIcon(0, self.file_icon)
                    file_item.setData(0, Qt.ItemDataRole.UserRole, str(whm_file))
                
                if not has_whm_files:
                    website_item.setHidden(True)

        except Exception as e:
            QMessageBox.critical(self, "错误", f"扫描文件夹失败: {e}")
        finally:
            self.whm_tree.setUpdatesEnabled(True)
            QApplication.restoreOverrideCursor()

        if self.whm_tree.topLevelItemCount() == 0 or all(self.whm_tree.topLevelItem(i).isHidden() for i in range(self.whm_tree.topLevelItemCount())):
            QMessageBox.information(self, "提示", "在该文件夹或其直接子目录中未找到 .whm 文件。")

    def on_whm_tree_item_selected(self, item: QTreeWidgetItem, column: int):
        file_path_str = item.data(0, Qt.ItemDataRole.UserRole)
        self.whm_table.setRowCount(0)
        self.whm_value_viewer.clear()
        if not file_path_str: return

        file_path = Path(file_path_str)
        try:
            container = self.whm_exporter.ExtractWhmStrings(file_path, set())
            if not container: return

            self.whm_table.setRowCount(len(container))
            self.whm_table.setUpdatesEnabled(False)
            for i, entry in enumerate(container):
                hash_item = QTableWidgetItem(f"0x{entry.hash:08X}")
                try:
                    full_text = entry.str.decode('windows-1252')
                except UnicodeDecodeError:
                    full_text = f"[解码错误: {entry.str.hex()}]"
                
                preview_text = full_text if len(full_text) <= 100 else full_text[:100] + "..."
                value_item = QTableWidgetItem(preview_text)
                value_item.setData(Qt.ItemDataRole.UserRole, full_text)
                self.whm_table.setItem(i, 0, hash_item)
                self.whm_table.setItem(i, 1, value_item)
        except Exception as e:
            QMessageBox.warning(self, "解析错误", f"无法解析 {file_path.name}: {e}")
        finally:
            self.whm_table.setUpdatesEnabled(True)
            if self.whm_table.rowCount() > 0: self.whm_table.selectRow(0)

    def on_whm_table_selection_changed(self):
        selected_rows = self.whm_table.selectionModel().selectedRows()
        if not selected_rows:
            self.whm_value_viewer.clear()
            return

        value_item = self.whm_table.item(selected_rows[0].row(), 1)
        if value_item:
            full_text = value_item.data(Qt.ItemDataRole.UserRole)
            self.whm_value_viewer.setPlainText(full_text if full_text is not None else "")

    def show_whm_viewer_context_menu(self, position):
        selected_items = self.whm_tree.selectedItems()
        if not selected_items: return

        file_paths = [Path(item.data(0, Qt.ItemDataRole.UserRole)) for item in selected_items if item.data(0, Qt.ItemDataRole.UserRole)]
        if not file_paths: return

        menu = QMenu()
        action = QAction(f"导出 {len(file_paths)} 个选定文件为 TXT...", self)
        action.triggered.connect(lambda: self.export_selected_whm_to_txt(file_paths))
        menu.addAction(action)
        menu.exec(self.whm_tree.viewport().mapToGlobal(position))

    def export_selected_whm_to_txt(self, file_paths):
        if not file_paths: return
        output_dir_str = QFileDialog.getExistingDirectory(self, "选择导出 TXT 的目标文件夹")
        if not output_dir_str: return

        output_path = Path(output_dir_str)
        exported_count, failed_files = 0, []

        progress = QProgressDialog("正在导出 TXT...", "取消", 0, len(file_paths), self)
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.show()

        for i, file_path in enumerate(file_paths):
            if progress.wasCanceled(): break
            progress.setValue(i)
            progress.setLabelText(f"处理中: {file_path.name}")
            try:
                container = self.whm_exporter.ExtractWhmStrings(file_path, set())
                if container:
                    output_txt_path = output_path / file_path.with_suffix(".txt").name
                    self.whm_exporter.ExportText(output_txt_path, container)
                    exported_count += 1
            except Exception as e:
                failed_files.append(file_path.name)
                print(f"导出失败: {file_path.name}: {e}")
        progress.close()

        msg = f"成功导出 {exported_count} 个文件到:\n{output_dir_str}"
        if failed_files:
            msg += f"\n\n有 {len(failed_files)} 个文件导出失败 (详情请查看控制台/日志)。"
            QMessageBox.warning(self, "导出完成（部分失败）", msg)
        else:
            QMessageBox.information(self, "导出成功", msg)

    def _add_to_recent_paths(self, path):
        if path in self.recent_paths:
            self.recent_paths.remove(path)
        self.recent_paths.insert(0, path)
        if len(self.recent_paths) > 5:
            self.recent_paths.pop()
        
        for combo in [self.export_input_combo, self.gendb_input_combo, self.gendb_output_combo]:
            combo.blockSignals(True)
            current_text = combo.lineEdit().text()
            combo.clear()
            combo.addItems(self.recent_paths)
            combo.lineEdit().setText(current_text)
            combo.blockSignals(False)

    def browse_whm_root(self):
        path = QFileDialog.getExistingDirectory(self, "选择 WHM 根文件夹 (例如: pc/html)")
        if path:
            self.export_input_combo.lineEdit().setText(path)
            self._add_to_recent_paths(path)

    def browse_txt_root(self):
        path = QFileDialog.getExistingDirectory(self, "选择 TXT 根文件夹")
        if path:
            self.gendb_input_combo.lineEdit().setText(path)
            self._add_to_recent_paths(path)

    def browse_gendb_output(self):
        path, _ = QFileDialog.getSaveFileName(self, "保存 whm_table.dat", "whm_table.dat", "WHM Table (whm_table.dat)")
        if path:
            if Path(path).name.lower() != "whm_table.dat":
                path = str(Path(path).parent / "whm_table.dat")
            self.gendb_output_combo.lineEdit().setText(path)
            self._add_to_recent_paths(path)

    def run_export(self):
        input_path_str = self.export_input_combo.lineEdit().text()
        if not input_path_str or not Path(input_path_str).is_dir():
            QMessageBox.warning(self, "错误", "请输入有效的 WHM 根文件夹路径。")
            return
        self.log_view.clear()
        self.log_view.append("--- [任务开始] 从 WHM 导出 TXT ---")
        self._run_batch_job('export', Path(input_path_str), None)

    def run_gendb(self):
        input_path_str = self.gendb_input_combo.lineEdit().text()
        output_path_str = self.gendb_output_combo.lineEdit().text()
        if not input_path_str or not Path(input_path_str).is_dir() or not output_path_str:
            QMessageBox.warning(self, "错误", "请输入有效的 TXT 根文件夹和数据库输出路径。")
            return
        self.log_view.clear()
        self.log_view.append("--- [任务开始] 从 TXT 生成数据库 ---")
        self._run_batch_job('gendb', Path(input_path_str), Path(output_path_str))

    def _run_batch_job(self, mode: str, input_path: Path, output_path: Path or None):
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(self, "任务正在运行", "请等待当前批量任务完成。")
            return

        self._set_ui_enabled(False, mode)
        sys.stdout = self.log_emitter
        sys.stderr = self.log_emitter

        self.worker_thread = WhmBatchWorker(mode, input_path, output_path, self.whm_exporter)
        self.worker_thread.finished_with_details.connect(self._on_job_finished)
        self.worker_thread.progress_updated.connect(self._update_progress)
        self.worker_thread.start()

    def _update_progress(self, processed: int, total: int, current_file: str):
        progress_bar = self.export_progress_bar if self.worker_thread.mode == 'export' else self.gendb_progress_bar
        if total > 0:
            progress_bar.setRange(0, total)
            progress_bar.setValue(processed)
            progress_bar.setFormat(f"已处理 {processed}/{total}")

    def _on_job_finished(self, job_mode: str, output_path: Path or None, exported_count: int):
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr
        self.log_view.append("--- [任务完成] ---")
        self._set_ui_enabled(True)
        self.worker_thread = None

        if job_mode == 'export':
            QMessageBox.information(self, "导出完成", f"导出完成！共成功导出 {exported_count} 个 TXT 文件。\nTXT 文件已保存在其对应的 WHM 源文件目录中。")
        elif job_mode == 'gendb' and output_path and output_path.exists():
            reply = QMessageBox.question(self, "生成成功", f"数据库文件已成功生成到:\n{output_path}\n\n是否打开文件所在文件夹？",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.Yes)
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    os.startfile(output_path.parent)
                except AttributeError:
                    import subprocess
                    subprocess.run(['xdg-open', str(output_path.parent)])

    def _set_ui_enabled(self, enabled: bool, current_mode: str or None = None):
        self.tabs.setEnabled(enabled)
        self.export_run_btn.setEnabled(enabled)
        self.gendb_run_btn.setEnabled(enabled)

        if enabled:
            self.export_progress_bar.setVisible(False)
            self.gendb_progress_bar.setVisible(False)
            self.export_progress_bar.reset()
            self.gendb_progress_bar.reset()
        else:
            if current_mode == 'export':
                self.export_progress_bar.setVisible(True)
                self.export_progress_bar.setFormat("处理中...")
            elif current_mode == 'gendb':
                self.gendb_progress_bar.setVisible(True)
                self.gendb_progress_bar.setFormat("处理中...")
    
    def closeEvent(self, event):
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(self, "任务正在运行", "请等待当前批量任务完成或手动取消。")
            event.ignore()
        else:
            sys.stdout = self.original_stdout
            sys.stderr = self.original_stderr
            event.accept()


class EditKeyDialog(QDialog):
    """编辑/新增 键值对对话框，支持多种模式"""
    def __init__(self, parent=None, title="编辑键值对", key="", value="", version="IV", file_type="gxt",
                 is_batch_add=False, is_batch_edit=False, batch_edit_data=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumWidth(520)
        self.version = version
        self.file_type = file_type
        self.original_key = key
        
        self.is_batch_add_mode = is_batch_add
        self.is_batch_edit_mode = is_batch_edit
        self.is_single_mode = not (is_batch_add or is_batch_edit)

        self.original_batch_keys = batch_edit_data['keys'] if self.is_batch_edit_mode and batch_edit_data else []
        self.key_value_pairs = []

        layout = QVBoxLayout(self)

        self.single_mode_widget = QWidget()
        single_layout = QVBoxLayout(self.single_mode_widget)
        single_layout.setContentsMargins(0,0,0,0)
        
        key_layout = QHBoxLayout()
        key_layout.addWidget(QLabel("键名 (Key):"))
        self.key_edit = QLineEdit(key)
        self.key_edit.setPlaceholderText("键名 (Key)")
        key_layout.addWidget(self.key_edit)
        single_layout.addLayout(key_layout)

        single_layout.addWidget(QLabel("值 (Value):"))
        
        self.value_edit = QTextEdit()
        self.value_edit.setPlainText(value)
        
        single_layout.addWidget(self.value_edit, 1)
        layout.addWidget(self.single_mode_widget)

        self.batch_edit = QTextEdit()
        initial_batch_text = batch_edit_data['text'] if self.is_batch_edit_mode and batch_edit_data else ""
        self.batch_edit.setPlainText(initial_batch_text)
        
        if self.is_batch_edit_mode:
            self.batch_edit.setPlaceholderText("每行一个键值对，格式为：键=值\n请确保行数与选择的条目数一致")
        else:
            self.batch_edit.setPlaceholderText("每行输入一个键值对，格式为：键=值\n空行将被忽略")
        layout.addWidget(self.batch_edit)
        
        self.add_mode_widget = QWidget()
        add_mode_layout = QVBoxLayout(self.add_mode_widget)
        add_mode_layout.setContentsMargins(0,0,0,0)
        
        self.batch_toggle = QPushButton("切换到批量添加模式")
        self.batch_toggle.setCheckable(True)
        self.batch_toggle.clicked.connect(self.toggle_add_mode)
        add_mode_layout.addWidget(self.batch_toggle)
        
        self.mode_label = QLabel("当前模式: 单个添加")
        add_mode_layout.addWidget(self.mode_label)
        layout.addWidget(self.add_mode_widget)

        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel, self)
        self.buttons.button(QDialogButtonBox.StandardButton.Save).setText("保存")
        self.buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)
        
        self._update_ui_for_mode()

    def _update_ui_for_mode(self):
        """根据当前模式更新UI可见性"""
        if self.is_batch_edit_mode:
            self.single_mode_widget.hide()
            self.batch_edit.show()
            self.add_mode_widget.hide()
            return

        is_add_operation = (self.original_key == "")
        if is_add_operation:
            self.add_mode_widget.show()
            if self.is_batch_add_mode:
                self.single_mode_widget.hide()
                self.batch_edit.show()
            else:
                self.single_mode_widget.show()
                self.batch_edit.hide()
        else:
            self.add_mode_widget.hide()
            self.single_mode_widget.show()
            self.batch_edit.hide()

    def toggle_add_mode(self):
        """切换单个/批量添加模式"""
        self.is_batch_add_mode = not self.is_batch_add_mode
        self.is_single_mode = not self.is_batch_add_mode
        
        if self.is_batch_add_mode:
            self.mode_label.setText("当前模式: 批量添加")
            self.batch_toggle.setText("切换到单个添加模式")
        else:
            self.mode_label.setText("当前模式: 单个添加")
            self.batch_toggle.setText("切换到批量添加模式")
        self._update_ui_for_mode()

    def validate_key(self, key):
        """验证键名是否符合当前版本的规则 (使用 re.fullmatch)"""
        return _validate_key_static(key, self.version, self.file_type)

    def get_validation_error_message(self):
        """获取当前版本键名的验证错误信息"""
        return _get_key_validation_message(self.version, self.file_type)

    def accept(self):
        if self.is_batch_add_mode or self.is_batch_edit_mode:
            content = self.batch_edit.toPlainText().strip()
            lines = [line.strip() for line in content.split('\n') if line.strip()]

            if self.is_batch_edit_mode:
                if len(lines) != len(self.original_batch_keys):
                    QMessageBox.critical(self, "行数不匹配", 
                                         f"编辑后的行数 ({len(lines)}) 必须与选择的条目数 ({len(self.original_batch_keys)}) 一致。\n"
                                         "请检查是否添加或删除了行。")
                    return

            if not lines and self.is_batch_add_mode:
                QMessageBox.warning(self, "警告", "请输入至少一个键值对")
                return

            parsed_pairs = []
            errors = []
            for i, line in enumerate(lines, 1):
                if '=' not in line:
                    errors.append(f"第 {i} 行: 缺少等号'='分隔符")
                    continue
                key, value = line.split('=', 1)
                key, value = key.strip(), value.strip()
                if not key:
                    errors.append(f"第 {i} 行: 键名不能为空")
                    continue
                if not self.validate_key(key):
                    errors.append(f"第 {i} 行: {self.get_validation_error_message()}")
                    continue
                parsed_pairs.append((key, value))

            if errors:
                error_msg = "\n".join(errors[:10])
                if len(errors) > 10: error_msg += f"\n... 还有 {len(errors) - 10} 个错误"
                QMessageBox.critical(self, "输入错误", f"发现以下错误:\n{error_msg}")
                return

            self.key_value_pairs = parsed_pairs
        
        else:
            new_key = self.key_edit.text().strip()
            new_value_raw = self.value_edit.toPlainText()

            if '\n' in new_value_raw:
                new_value = new_value_raw.replace('\n', '')
                QMessageBox.information(self, "提示", "检测到值(Value)中存在换行符，已被自动移除。")
            else:
                new_value = new_value_raw
            
            new_value = new_value.rstrip()

            if not self.validate_key(new_key):
                QMessageBox.critical(self, "错误", f"键名格式不正确！\n{self.get_validation_error_message()}")
                return
            
            if not new_key:
                QMessageBox.critical(self, "错误", "键名不能为空！")
                return
                
            self.key_value_pairs = [(new_key, new_value)]
            
        super().accept()

    def get_data(self):
        if self.is_batch_add_mode or self.is_batch_edit_mode:
            return self.key_value_pairs
        else:
            return self.key_value_pairs[0] if self.key_value_pairs else ("", "")


class VersionDialog(QDialog):
    """选择 TXT 文件对应的游戏版本。"""
    def __init__(self, parent=None, default="IV", include_whm=False):
        super().__init__(parent)
        self.setWindowTitle("选择版本")
        layout = QVBoxLayout(self)
        self.versions = [
            ("GTA V", "V"),
            ("GTA IV", "IV"), 
            ("GTA Vice City", "VC"), 
            ("GTA San Andreas", "SA"), 
            ("GTA III (LC)", "III")
        ]
        
        if include_whm:
            self.versions.append(("WHM Table (DAT)", "WHM"))
            
        self.inputs = []
        for text, val in self.versions:
            btn = QPushButton(text)
            btn.setCheckable(True)
            btn.clicked.connect(lambda _, b=btn: self._select(b))
            layout.addWidget(btn)
            self.inputs.append((btn, val))

        default_found = False
        for b, val in self.inputs:
            if val == default:
                b.setChecked(True)
                default_found = True
                break
        if not default_found and self.inputs:
             self.inputs[0][0].setChecked(True)

        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel, parent=self)
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setText("确定")
        self.buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def _select(self, clicked_btn):
        for b, _ in self.inputs:
            b.setChecked(b is clicked_btn)

    def get_value(self):
        for b, v in self.inputs:
            if b.isChecked():
                return v
        return "V"


class GXTEditorApp(QMainWindow):
    def __init__(self, file_to_open=None):
        super().__init__()
        self.setWindowTitle(" GTA文本对话表编辑器 v2.2 作者：倾城剑舞")
        self.resize(1240, 760)
        self.setAcceptDrops(True)
        
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            base_dir = Path(sys._MEIPASS)
        elif getattr(sys, 'frozen', False):
            base_dir = Path(sys.executable).parent
        else:
            base_dir = Path(__file__).parent

        icon_path = base_dir / "app_icon.ico"
        if icon_path.exists():
            self.setWindowIcon(QIcon(str(icon_path)))
             
        self.file_to_open = file_to_open

        self.data = {}
        self.version = None
        self.filepath = None
        self.file_type = None
        self.current_table = None
        self.value_display_limit = 60
        self.version_filename_map = {'IV': 'GTA4.txt', 'VC': 'GTAVC.txt', 'SA': 'GTASA.txt', 'III': 'GTA3.txt', 'V': 'GTAV.txt'}
        self.modified = False
        
        self.whm_exporter = CHtmlTextExport()
        self.whm_batch_tool_instance = None
        
        if getattr(sys, 'frozen', False):
            app_dir = Path(sys.executable).parent
        else:
            app_dir = Path(__file__).resolve().parent
        self.settings_path = app_dir / "GXT编辑器设置.json"
        
        self.remember_gen_extra_choice = None
        self.save_prompt_choice = None
        self._load_settings()

        self._apply_neutral_dark_theme()
        self._setup_menu()
        self._setup_statusbar()
        self._setup_body()
        
        if self.file_to_open:
            QTimer.singleShot(300, lambda: self.open_file(path=self.file_to_open))

    def _load_settings(self):
        """从 JSON 文件加载设置"""
        try:
            if os.path.exists(self.settings_path):
                with open(self.settings_path, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                    self.remember_gen_extra_choice = settings.get('记住生成额外文件的选择')
                    self.save_prompt_choice = settings.get('文件变更时的默认操作')
        except Exception as e:
            print(f"无法加载设置: {e}")

    def _save_settings(self):
        """将设置保存到 JSON 文件"""
        try:
            settings = {
                '记住生成额外文件的选择': self.remember_gen_extra_choice,
                '文件变更时的默认操作': self.save_prompt_choice
            }
            with open(self.settings_path, 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"无法保存设置: {e}")
            
    def _apply_neutral_dark_theme(self):
        """应用中性深色主题"""
        app = QApplication.instance()
        palette = QPalette()
        
        dark_bg = QColor(30, 30, 34)
        darker_bg = QColor(25, 25, 28)
        text_color = QColor(220, 220, 220)
        highlight = QColor(0, 122, 204)
        button_bg = QColor(45, 45, 50)
        border_color = QColor(60, 60, 65)
        
        palette.setColor(QPalette.ColorRole.Window, dark_bg)
        palette.setColor(QPalette.ColorRole.WindowText, text_color)
        palette.setColor(QPalette.ColorRole.Base, darker_bg)
        palette.setColor(QPalette.ColorRole.AlternateBase, dark_bg)
        palette.setColor(QPalette.ColorRole.ToolTipBase, dark_bg)
        palette.setColor(QPalette.ColorRole.ToolTipText, text_color)
        palette.setColor(QPalette.ColorRole.Text, text_color)
        palette.setColor(QPalette.ColorRole.Button, button_bg)
        palette.setColor(QPalette.ColorRole.ButtonText, text_color)
        palette.setColor(QPalette.ColorRole.Highlight, highlight)
        palette.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.white)
        palette.setColor(QPalette.ColorRole.BrightText, Qt.GlobalColor.red)
        palette.setColor(QPalette.ColorRole.Link, highlight)
        
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(150, 150, 150))
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(150, 150, 150))
        
        app.setPalette(palette)
        app.setStyle("Fusion")
        
        app.setStyleSheet(f"""
            QWidget {{
                font-family: "Microsoft YaHei", "Segoe UI", sans-serif;
                font-size: 10pt;
            }}
            QMainWindow {{
                background-color: {dark_bg.name()};
            }}
            QMenuBar {{
                background-color: {darker_bg.name()};
                padding: 5px;
                border-bottom: 1px solid {border_color.name()};
            }}
            QMenuBar::item {{
                background: transparent;
                padding: 5px 10px;
                color: {text_color.name()};
                border-radius: 4px;
            }}
            QMenuBar::item:selected {{
                background-color: {highlight.name()};
            }}
            QMenu {{
                background-color: {darker_bg.name()};
                border: 1px solid {border_color.name()};
                padding: 5px;
            }}
            QMenu::item {{
                padding: 5px 30px 5px 20px;
            }}
            QMenu::item:selected {{
                background-color: {highlight.name()};
            }}
            QPushButton {{
                background-color: {button_bg.name()};
                color: {text_color.name()};
                border: 1px solid {border_color.name()};
                border-radius: 4px;
                padding: 5px 10px;
                min-height: 28px;
            }}
            QPushButton:hover {{
                background-color: #3a3a40;
                border-color: #7a7a7a;
            }}
            QPushButton:pressed {{
                background-color: #2a2a2e;
            }}
            QPushButton:checked {{
                background-color: {highlight.name()};
                border-color: {QColor(highlight).lighter(120).name()};
            }}
            QPushButton#globalSearchButton:checked {{
                background-color: #d32f2f;
                border-color: #ff5f52;
                color: white;
                font-weight: bold;
            }}
            QLineEdit, QTextEdit, QListWidget, QTableWidget, QComboBox {{
                background-color: {darker_bg.name()};
                color: {text_color.name()};
                border: 1px solid {border_color.name()};
                border-radius: 4px;
                padding: 5px;
                selection-background-color: {highlight.name()};
                selection-color: white;
            }}
            QLineEdit:focus, QTextEdit:focus, QListWidget:focus, QTableWidget:focus, QComboBox:focus {{
                border: 1px solid {highlight.name()};
            }}
            QDockWidget {{
                background: {dark_bg.name()};
                border: 1px solid {border_color.name()};
                titlebar-normal-icon: none;
            }}
            QDockWidget::title {{
                background: {darker_bg.name()};
                padding: 5px;
                text-align: center;
            }}
            QHeaderView::section {{
                background-color: {button_bg.name()};
                color: {text_color.name()};
                padding: 5px;
                border: 1px solid {border_color.name()};
            }}
            QTableWidget::item {{
                padding: 5px;
            }}
            QTableCornerButton::section {{
                background-color: {button_bg.name()};
                border: 1px solid {border_color.name()};
            }}
            QStatusBar {{
                background-color: {darker_bg.name()};
                border-top: 1px solid {border_color.name()};
                color: {text_color.name()};
            }}
            QScrollBar:vertical {{
                border: none;
                background: {darker_bg.name()};
                width: 16px;
                margin: 2px 0 2px 0;
            }}
            QScrollBar::handle:vertical {{
                background: {button_bg.name()};
                min-height: 25px;
                border-radius: 6px;
                border: 1px solid {border_color.name()};
            }}
            QScrollBar::handle:vertical:hover {{
                background: {QColor(button_bg).lighter(130).name()};
            }}
            QScrollBar::handle:vertical:pressed {{
                background: {QColor(button_bg).darker(110).name()};
            }}
            QScrollBar:horizontal {{
                border: none;
                background: {darker_bg.name()};
                height: 16px;
                margin: 0 2px 0 2px;
            }}
            QScrollBar::handle:horizontal {{
                background: {button_bg.name()};
                min-width: 25px;
                border-radius: 6px;
                border: 1px solid {border_color.name()};
            }}
            QScrollBar::handle:horizontal:hover {{
                background: {QColor(button_bg).lighter(130).name()};
            }}
            QScrollBar::handle:horizontal:pressed {{
                background: {QColor(button_bg).darker(110).name()};
            }}
            QScrollBar::add-line, QScrollBar::sub-line {{
                background: none;
                border: none;
                height: 0px;
                width: 0px;
            }}
            QGroupBox {{
                font-weight: bold;
                border: 1px solid {border_color.name()};
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                subcontrol-position: top center;
                padding: 0 5px;
            }}
            QSplitter::handle {{
                background-color: {border_color.name()};
            }}
            QSplitter::handle:horizontal {{
                width: 2px;
            }}
            QSplitter::handle:vertical {{
                height: 2px;
            }}
            QTabWidget::pane {{
                border: 1px solid {border_color.name()};
            }}
            QTabBar::tab {{
                background: {button_bg.name()};
                border: 1px solid {border_color.name()};
                padding: 8px 15px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                margin-right: 2px;
            }}
            QTabBar::tab:selected {{
                background: {dark_bg.name()};
                border-bottom-color: {dark_bg.name()};
            }}
            QTabBar::tab:!selected:hover {{
                background: #4a4a50;
            }}
            QProgressBar {{
                border: 1px solid {border_color.name()};
                border-radius: 4px;
                text-align: center;
                color: {text_color.name()};
                background-color: {darker_bg.name()};
            }}
            QProgressBar::chunk {{
                background-color: {highlight.name()};
                border-radius: 4px;
            }}
        """)

    def _setup_menu(self):
        menubar = QMenuBar(self)
        self.setMenuBar(menubar)

        file_menu = QMenu("文件", self)
        menubar.addMenu(file_menu)
        file_menu.addAction(self._act("📂 打开GTA文本文件", self.open_file_dialog, "Ctrl+O"))
        file_menu.addAction(self._act("📄 导入TXT文件（可多选）", self.open_txt))
        file_menu.addSeparator()
        file_menu.addAction(self._act("🆕 新建GXT文件", self.new_gxt))
        file_menu.addAction(self._act("📝 新建whm_table文件", self.new_whm))
        file_menu.addAction(self._act("💾 保存", self.save_file, "Ctrl+S"))
        file_menu.addAction(self._act("💾 另存为...", self.save_file_as))
        file_menu.addSeparator()
        file_menu.addAction(self._act("➡ 导出为单个TXT", lambda: self.export_txt(single=True)))
        file_menu.addAction(self._act("➡ 导出为多个TXT（文件夹）", lambda: self.export_txt(single=False)))
        file_menu.addSeparator()
        file_menu.addAction(self._act("📎 设置.gxt/.gxt2文件关联", self.set_file_association))
        file_menu.addSeparator()
        file_menu.addAction(self._act("❌ 退出", self.close, "Ctrl+Q"))
        
        tools_menu = QMenu("工具", self)
        menubar.addMenu(tools_menu)
        self.font_generator_action = self._act("🎨 GTA 字体贴图生成器", self.open_font_generator)
        tools_menu.addAction(self.font_generator_action)
        tools_menu.addAction(self._act("🔄 码表转换工具", self.open_codepage_converter))
        tools_menu.addAction(self._act("🛠️ WHM 文本提取工具", self.open_whm_batch_tool))
        
        help_menu = QMenu("帮助", self)
        menubar.addMenu(help_menu)
        help_menu.addAction(self._act("💡 关于", self.show_about))
        help_menu.addAction(self._act("❓ 使用帮助", self.show_help))
    
    def _setup_statusbar(self):
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.update_status("就绪。将 .gxt, .gxt2, whm_table.dat 或 .txt 文件/文件夹拖入窗口可打开。")

    def _setup_body(self):
        self.tables_dock = QDockWidget("表列表", self)
        self.tables_dock.setFeatures(QDockWidget.DockWidgetFeature.NoDockWidgetFeatures)
        self.tables_dock.setMaximumWidth(200)
        self.tables_dock.setMinimumWidth(150)
        self.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, self.tables_dock)
        
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(5, 5, 5, 5)
        
        self.table_search = QLineEdit()
        self.table_search.setPlaceholderText("🔍 搜索表名...")
        self.table_search.textChanged.connect(self.filter_tables)
        left_layout.addWidget(self.table_search)
        
        self.table_list = QListWidget()
        self.table_list.itemSelectionChanged.connect(self.select_table)
        self.table_list.itemDoubleClicked.connect(self.rename_table)
        left_layout.addWidget(self.table_list, 1)
        
        btn_layout = QHBoxLayout()
        self.btn_add_table = QPushButton("➕")
        self.btn_add_table.setToolTip("添加表")
        self.btn_add_table.clicked.connect(self.add_table)
        
        self.btn_del_table = QPushButton("🗑️")
        self.btn_del_table.setToolTip("删除表")
        self.btn_del_table.clicked.connect(self.delete_table)
        
        self.btn_export_table = QPushButton("📤")
        self.btn_export_table.setToolTip("导出此表")
        self.btn_export_table.clicked.connect(self.export_current_table)
        
        btn_layout.addWidget(self.btn_add_table)
        btn_layout.addWidget(self.btn_del_table)
        btn_layout.addWidget(self.btn_export_table)
        left_layout.addLayout(btn_layout)
        
        self.tables_dock.setWidget(left)
        
        central = QWidget()
        c_layout = QVBoxLayout(central)
        
        search_layout = QHBoxLayout()
        self.key_search = QLineEdit()
        self.key_search.setPlaceholderText("🔍 搜索键或值...")
        self.key_search.textChanged.connect(self.search_key_value)
        
        self.global_search_button = QPushButton("全局搜索")
        self.global_search_button.setObjectName("globalSearchButton")
        self.global_search_button.setCheckable(True)
        self.global_search_button.setToolTip("开启/关闭全局搜索模式。开启后将搜索所有表。")
        self.global_search_button.clicked.connect(self._on_search_mode_changed)

        search_layout.addWidget(self.key_search, 1)
        search_layout.addWidget(self.global_search_button)
        c_layout.addLayout(search_layout)
        
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["序号", "键名 (Key)", "值 (Value)"])
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.doubleClicked.connect(self.on_table_double_click)
        self.table.verticalHeader().setVisible(False)
        
        fm = self.table.fontMetrics()
        six_digit_width = fm.horizontalAdvance("999999") + 20
        self.table.setColumnWidth(0, six_digit_width)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        
        c_layout.addWidget(self.table)
        
        key_btns = QHBoxLayout()
        key_btns.setContentsMargins(0, 5, 0, 0)
        btn_kadd = QPushButton("➕ 添加键")
        btn_kadd.clicked.connect(self.add_key)
        key_btns.addWidget(btn_kadd)
        key_btns.addStretch()
        c_layout.addLayout(key_btns)
        
        self.setCentralWidget(central)
        
    def _on_search_mode_changed(self):
        is_global = self.global_search_button.isChecked()
        if is_global:
            self.table_list.clearSelection()
            self.current_table = None
            self.update_status("全局搜索模式已开启")
        else:
            self.select_table() 
            self.update_status("本地搜索模式")
        self.search_key_value()
        
    def show_context_menu(self, position):
        """显示右键菜单"""
        is_global_search = self.global_search_button.isChecked()
        if not self.current_table and not is_global_search:
            return

        selected_rows = self.table.selectionModel().selectedRows()
        count = len(selected_rows)
        if count == 0:
            return

        if is_global_search:
            first_row_index = selected_rows[0].row()
            is_header_selection = all(self.table.columnSpan(idx.row(), 0) > 1 for idx in selected_rows)
            if is_header_selection:
                return

        menu = QMenu()
        if count == 1:
            edit_action = QAction("✏️ 编辑", self)
            edit_action.triggered.connect(self.edit_selected_items)
            menu.addAction(edit_action)
        elif count > 1: 
            edit_action = QAction("✏️ 批量编辑", self)
            edit_action.triggered.connect(self.edit_selected_items)
            menu.addAction(edit_action)
        
        menu.addSeparator()

        delete_action = QAction("🗑️ 删除", self)
        delete_action.triggered.connect(self.delete_key)
        menu.addAction(delete_action)

        copy_action = QAction("📋 复制", self)
        copy_action.triggered.connect(self.copy_selected)
        menu.addAction(copy_action)

        menu.exec(self.table.viewport().mapToGlobal(position))

    def _act(self, text, slot, shortcut=None):
        a = QAction(text, self)
        if shortcut: a.setShortcut(shortcut)
        a.triggered.connect(slot)
        return a

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls(): event.acceptProposedAction()

    def dropEvent(self, event):
        """处理文件拖放，支持单个或多个文件/文件夹。"""
        urls = event.mimeData().urls()
        if not urls:
            return
    
        paths = [url.toLocalFile() for url in urls]
    
        txt_files = []
        other_files = []

        for path in paths:
            if os.path.isdir(path):
                for root, _, files in os.walk(path):
                    for name in files:
                        if name.lower().endswith('.txt'):
                            txt_files.append(os.path.join(root, name))
            else:
                if path.lower().endswith('.txt'):
                    txt_files.append(path)
                else:
                    other_files.append(path)

        if txt_files:
            self.open_txt(files=txt_files)
        elif other_files:
            self.open_file(other_files[0])

    def open_file(self, path):
        if not path or not os.path.exists(path): return
        
        lower_path = path.lower()
        if lower_path.endswith(".gxt"):
            self.open_gxt(path)
        elif lower_path.endswith(".gxt2"):
            self.open_gxt2(path)
        elif os.path.basename(lower_path) == "whm_table.dat":
            self.open_dat(path)
        elif lower_path.endswith(".txt"):
            self.open_txt(files=[path])
        else:
            self.update_status("错误：请拖拽 .gxt, .gxt2, whm_table.dat 或 .txt 文件/文件夹。")

    def filter_tables(self):
        keyword = self.table_search.text().lower()
        self.table_list.clear()

        other_tables = sorted([name for name in self.data if name != 'MAIN'])
        all_table_names = []
        if 'MAIN' in self.data:
            all_table_names.append('MAIN')
        all_table_names.extend(other_tables)

        for name in all_table_names:
            if keyword in name.lower():
                self.table_list.addItem(name)
        
        self.update_status(f"显示 {self.table_list.count()} 个表")

    def select_table(self):
        items = self.table_list.selectedItems()
        if not items:
            if not self.global_search_button.isChecked():
                self.table.setRowCount(0)
                self.current_table = None
            return
        
        selected_table_name = items[0].text()

        if self.global_search_button.isChecked():
            header_text = f"以下是：{selected_table_name} 的键值对"
            for row in range(self.table.rowCount()):
                item = self.table.item(row, 0)
                if item and self.table.columnSpan(row, 0) > 1 and item.text() == header_text:
                    self.table.scrollToItem(item, QAbstractItemView.ScrollHint.PositionAtTop)
                    return
            return

        self.current_table = selected_table_name
        self.refresh_keys()
        self.update_status(f"查看表: {self.current_table}，共 {len(self.data.get(self.current_table, {}))} 个键值对")

    def refresh_keys(self):
        """优化后的表格刷新方法"""
        if self.global_search_button.isChecked():
            self.search_key_value()
            return
            
        self.table.setUpdatesEnabled(False)
        try:
            self.table.setColumnCount(3)
            self.table.setHorizontalHeaderLabels(["序号", "键名 (Key)", "值 (Value)"])
            self.table.setRowCount(0)
            if self.current_table and self.current_table in self.data:
                items_to_display = self.data[self.current_table].items()
                self.table.setRowCount(len(items_to_display))
                
                for idx, (k, v) in enumerate(items_to_display):
                    display_value = v if len(v) <= self.value_display_limit else v[:self.value_display_limit] + "..."
                    
                    idx_item = QTableWidgetItem(str(idx + 1))
                    idx_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    self.table.setItem(idx, 0, idx_item)
                    self.table.setItem(idx, 1, QTableWidgetItem(k))
                    value_item = QTableWidgetItem(display_value)
                    value_item.setData(Qt.ItemDataRole.UserRole, v)
                    self.table.setItem(idx, 2, value_item)
        finally:
            self.table.setUpdatesEnabled(True)

    def search_key_value(self):
        keyword = self.key_search.text().lower()
        self.table.setUpdatesEnabled(False)
        try:
            self.table.setRowCount(0)
            self.table.setColumnCount(3)
            self.table.setHorizontalHeaderLabels(["序号", "键名 (Key)", "值 (Value)"])
            
            if self.global_search_button.isChecked():
                grouped_results = defaultdict(list)
                total_matches = 0
                for table_name, entries in self.data.items():
                    for original_idx, (k, v) in enumerate(entries.items()):
                        if keyword in k.lower() or keyword in str(v).lower():
                            grouped_results[table_name].append((original_idx, k, v))
                            total_matches += 1
                
                if not grouped_results:
                    self.update_status("全局搜索结果: 0 个匹配项")
                    self.table.setUpdatesEnabled(True)
                    return

                table_names = list(grouped_results.keys())
                sorted_table_names = []
                if 'MAIN' in table_names:
                    sorted_table_names.append('MAIN')
                    table_names.remove('MAIN')
                sorted_table_names.extend(sorted(table_names))

                total_rows = len(grouped_results) + total_matches
                self.table.setRowCount(total_rows)

                current_row = 0
                header_font = QFont()
                header_font.setBold(True)
                header_bg = QColor(45, 45, 50)

                for table_name in sorted_table_names:
                    header_item = QTableWidgetItem(f"以下是：{table_name} 的键值对")
                    header_item.setFont(header_font)
                    header_item.setBackground(header_bg)
                    header_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    self.table.setItem(current_row, 0, header_item)
                    self.table.setSpan(current_row, 0, 1, 3)
                    current_row += 1

                    for original_idx, k, v in grouped_results[table_name]:
                        display_value = v if len(v) <= self.value_display_limit else v[:self.value_display_limit] + "..."
                        
                        idx_item = QTableWidgetItem(str(original_idx + 1))
                        idx_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        self.table.setItem(current_row, 0, idx_item)
                        
                        self.table.setItem(current_row, 1, QTableWidgetItem(k))
                        
                        value_item = QTableWidgetItem(display_value)
                        value_item.setData(Qt.ItemDataRole.UserRole, v)
                        self.table.setItem(current_row, 2, value_item)
                        
                        current_row += 1

                self.table.resizeColumnToContents(1)
                self.update_status(f"全局搜索结果: {total_matches} 个匹配项")
            else:
                if self.current_table and self.current_table in self.data:
                    matching_items = []
                    for original_idx, (k, v) in enumerate(self.data[self.current_table].items()):
                        if keyword in k.lower() or keyword in str(v).lower():
                            matching_items.append((original_idx, k, v))
                    
                    self.table.setRowCount(len(matching_items))
                    for row_idx, (original_idx, k, v) in enumerate(matching_items):
                        display_value = v if len(v) <= self.value_display_limit else v[:self.value_display_limit] + "..."
                        
                        idx_item = QTableWidgetItem(str(original_idx + 1))
                        idx_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        self.table.setItem(row_idx, 0, idx_item)
                        self.table.setItem(row_idx, 1, QTableWidgetItem(k))
                        value_item = QTableWidgetItem(display_value)
                        value_item.setData(Qt.ItemDataRole.UserRole, v)
                        self.table.setItem(row_idx, 2, value_item)
                    
                    self.update_status(f"在表 '{self.current_table}' 中搜索到: {len(matching_items)} 个匹配项")
        finally:
            self.table.setUpdatesEnabled(True)

    def validate_table_name(self, name):
        """验证表名是否符合当前版本的规则"""
        if self.version == 'V': return True
        if self.version == 'VC' or self.version == 'SA':
            return re.match(r'^[0-9A-Z_]{1,7}$', name) is not None
        elif self.version == 'IV':
            return re.match(r'^[0-9a-zA-Z_]{1,7}$', name) is not None
        return True
    
    def _validate_key_for_import(self, key, version):
        """
        用于导入时验证键名的辅助函数。
        此方法现在是新版静态优化函数的包装器，以保持向后兼容性。
        """
        is_valid, message = _validate_key_for_import_optimized(key, version)
        return is_valid, message

    def get_table_validation_error_message(self):
        """获取当前版本表名的验证错误信息"""
        if self.version == 'VC' or self.version == 'SA':
            return "VC/SA 表名必须是1-7位大写字母、数字或下划线"
        elif self.version == 'IV':
            return "IV 表名必须是1-7位字母、数字或下划线"
        return "表名格式不正确"

    def add_table(self):
        if self.file_type == 'dat' or self.version == 'V':
            QMessageBox.information(self, "提示", "当前文件类型不支持多表操作。")
            return
        if not hasattr(self, 'version') or self.version is None:
            QMessageBox.information(self, "提示", "请先新建或打开一个GXT文件。")
            return
            
        name, ok = QInputDialog.getText(self, "新建表", "请输入表名：")
        if ok and name.strip():
            name = name.strip()
            if not self.validate_table_name(name):
                QMessageBox.warning(self, "错误", f"表名 '{name}' 格式不正确！\n{self.get_table_validation_error_message()}")
                return
            
            if name in self.data:
                QMessageBox.warning(self, "错误", f"表 '{name}' 已存在！")
                return
            self.data[name] = {}
            self.table_search.clear()
            self.filter_tables()
            if self.global_search_button.isChecked():
                self.search_key_value()
            items = self.table_list.findItems(name, Qt.MatchFlag.MatchExactly)
            if items: self.table_list.setCurrentItem(items[0])
            self.update_status(f"已添加新表: {name}")
            self.set_modified(True)

    def delete_table(self):
        if self.file_type == 'dat' or self.version == 'V':
            QMessageBox.information(self, "提示", "当前文件类型不支持多表操作。")
            return
        if not self.current_table: return
        msg_box = QMessageBox(QMessageBox.Icon.Question, "确认", f"是否删除表 '{self.current_table}'？\n此操作不可恢复！", 
                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, self)
        msg_box.button(QMessageBox.StandardButton.Yes).setText("是")
        msg_box.button(QMessageBox.StandardButton.No).setText("否")
        if msg_box.exec() == QMessageBox.StandardButton.Yes:
            old = self.current_table
            del self.data[self.current_table]
            self.current_table = None
            self.refresh_keys()
            self.filter_tables()
            if self.global_search_button.isChecked():
                self.search_key_value()
            self.update_status(f"已删除表: {old}")
            self.set_modified(True)

    def rename_table(self, _item):
        if self.file_type == 'dat' or self.version == 'V':
            return
        if not self.current_table: return
        old = self.current_table
        new, ok = QInputDialog.getText(self, "重命名表", "请输入新名称：", text=old)
        if ok and new.strip():
            new = new.strip()
            if not self.validate_table_name(new):
                QMessageBox.warning(self, "错误", f"表名 '{new}' 格式不正确！\n{self.get_table_validation_error_message()}")
                return
                
            if new in self.data and new != old:
                QMessageBox.warning(self, "错误", f"表 '{new}' 已存在！")
                return
            self.data[new] = self.data.pop(old)
            self.current_table = new
            self.filter_tables()
            if self.global_search_button.isChecked():
                self.search_key_value()
            items = self.table_list.findItems(new, Qt.MatchFlag.MatchExactly)
            if items: self.table_list.setCurrentItem(items[0])
            self.update_status(f"已将表 '{old}' 重命名为 '{new}'")
            self.set_modified(True)

    def export_current_table(self):
        if not self.current_table or not self.data.get(self.current_table):
            QMessageBox.information(self, "提示", "没有数据可导出")
            return
        default_filename = f"{self.current_table}.txt"
        filepath, _ = QFileDialog.getSaveFileName(self, "导出当前表为TXT", default_filename, "文本文件 (*.txt)")
        if not filepath: return
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                if self.version != 'III' and self.version != 'V': f.write(f"[{self.current_table}]\n")
                for k, v in sorted(self.data[self.current_table].items()): f.write(f"{k}={v}\n")
            QMessageBox.information(self, "导出成功", f"表 '{self.current_table}' 已导出到:\n{filepath}")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败: {str(e)}")

    def on_table_double_click(self):
        """双击时触发编辑"""
        self.edit_selected_items()

    def edit_selected_items(self):
        is_global_search = self.global_search_button.isChecked()
        if not self.current_table and not is_global_search:
            return
            
        selected_rows = self.table.selectionModel().selectedRows()
        count = len(selected_rows)
        
        if count == 0: return

        if count == 1:
            row = selected_rows[0].row()
            
            if self.table.columnSpan(row, 0) > 1:
                return

            if is_global_search:
                table_name = ""
                for i in range(row, -1, -1):
                    if self.table.columnSpan(i, 0) > 1:
                        text = self.table.item(i, 0).text()
                        table_name = text.replace("以下是：", "").replace(" 的键值对", "")
                        break
                if not table_name: return

                key = self.table.item(row, 1).text()
            else:
                table_name = self.current_table
                key = self.table.item(row, 1).text()
                
            original_value = self.data[table_name].get(key, "")
            
            dlg = EditKeyDialog(self, title=f"编辑: {key}", key=key, value=original_value, version=self.version, file_type=self.file_type)
            if dlg.exec() == QDialog.DialogCode.Accepted:
                new_key, new_val = dlg.get_data()
                
                if new_key != key and new_key in self.data[table_name]:
                    QMessageBox.critical(self, "错误", f"键名 '{new_key}' 已存在！")
                    return
                
                if new_key != key:
                    del self.data[table_name][key]
                self.data[table_name][new_key] = new_val
                
                self.search_key_value()
                self.update_status(f"已更新键: {new_key}")
                self.set_modified(True)
        
        elif True:
        
            original_entries = []

            for idx in selected_rows:
                row = idx.row()

                if self.table.columnSpan(row, 0) > 1:
                    continue

                table_name = None
                if is_global_search:
                    for i in range(row, -1, -1):
                        if self.table.columnSpan(i, 0) > 1:
                            text = self.table.item(i, 0).text()
                            table_name = text.replace("以下是：", "").replace(" 的键值对", "")
                            break
                else:
                    table_name = self.current_table

                if not table_name:
                    continue

                key_item = self.table.item(row, 1)

                if not key_item:
                    continue

                key = key_item.text()

                value = self.data.get(table_name, {}).get(key, "")

                original_entries.append((table_name, key, value))

            if not original_entries:
        
                return
        
            original_keys = [k for (_, k, _) in original_entries]
        
            batch_text = "\n".join([f"{k}={v}" for (_, k, v) in original_entries])
        
            dlg_data = {'keys': original_keys, 'text': batch_text}
        
            dlg = EditKeyDialog(self, title=f"批量编辑 {len(original_entries)} 个条目", version=self.version, file_type=self.file_type,
        
                                is_batch_edit=True, batch_edit_data=dlg_data)
        
            if dlg.exec() == QDialog.DialogCode.Accepted:
                new_pairs = dlg.get_data()

                if len(new_pairs) != len(original_entries):
                    QMessageBox.critical(self, "错误", "批量编辑返回的数据与原始选择数不匹配。")
                    return

                from collections import defaultdict, Counter

                orig_keys_per_table = defaultdict(list)
                for tbl, k, _ in original_entries:
                    orig_keys_per_table[tbl].append(k)

                other_keys_per_table = {t: set(self.data.get(t, {}).keys()) - set(orig_keys_per_table[t]) for t in orig_keys_per_table}

                new_keys_counter_per_table = defaultdict(Counter)
                for (tbl, _, _), (new_k, _) in zip(original_entries, new_pairs):
                    new_keys_counter_per_table[tbl][new_k] += 1

                duplicate_new_keys = []
                for t, counter in new_keys_counter_per_table.items():
                    for k, cnt in counter.items():
                        if cnt > 1:
                            duplicate_new_keys.append(f"{t}:{k} (出现 {cnt} 次)")

                if duplicate_new_keys:
                    QMessageBox.critical(self, "重复键", f"在批量编辑中发现重复键名（同一表内）: {', '.join(duplicate_new_keys)}。\n请确保每个表中键名唯一。")
                    return

                new_keys_per_table = defaultdict(set)
                for (tbl, _, _), (new_k, _) in zip(original_entries, new_pairs):
                    new_keys_per_table[tbl].add(new_k)

                conflicts = []
                for t in new_keys_per_table:
                    conf = new_keys_per_table[t].intersection(other_keys_per_table.get(t, set()))
                    if conf:
                        conflicts.extend([f"{t}:{c}" for c in conf])

                if conflicts:
                    QMessageBox.critical(self, "键名冲突", f"发现键名冲突: {', '.join(conflicts)}\n这些键已在表中存在且不属于当前编辑的条目。")
                    return

                edits_by_table = defaultdict(dict)
                for (tbl, old_k, _), (new_k, new_v) in zip(original_entries, new_pairs):
                    edits_by_table[tbl][old_k] = (new_k, new_v)

                for table_name, edits in edits_by_table.items():
                    if table_name not in self.data: continue

                    original_table_dict = self.data[table_name]
                    new_table_dict = {}
                    
                    for old_key, old_value in original_table_dict.items():
                        if old_key in edits:
                            new_key, new_value = edits[old_key]
                            new_table_dict[new_key] = new_value
                        else:
                            new_table_dict[old_key] = old_value
                    
                    self.data[table_name] = new_table_dict

                self.search_key_value()
                self.update_status(f"已批量更新 {len(new_pairs)} 个键值对")
                self.set_modified(True)

    def add_key(self):
        if self.global_search_button.isChecked():
            QMessageBox.information(self, "提示", "请先退出全局搜索模式，并选择一个表来添加键值对。")
            return
            
        if not self.current_table: 
            QMessageBox.information(self, "提示", "请先选择一个表")
            return
            
        dlg = EditKeyDialog(self, title="添加键值对", version=self.version, file_type=self.file_type)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            result = dlg.get_data()
            
            if isinstance(result, list):
                pairs = result
                added_count = 0
                duplicate_keys = []
                
                for key, value in pairs:
                    if key in self.data[self.current_table]:
                        duplicate_keys.append(key)
                        continue
                        
                    self.data[self.current_table][key] = value
                    added_count += 1
                    
                self.refresh_keys()
                
                msg = f"成功添加 {added_count} 个键值对"
                if duplicate_keys:
                    msg += f"\n有 {len(duplicate_keys)} 个键已存在，未添加: {', '.join(duplicate_keys[:5])}"
                    if len(duplicate_keys) > 5:
                        msg += f" ... (共 {len(duplicate_keys)} 个)"
                        
                QMessageBox.information(self, "添加完成", msg)
                self.update_status(f"批量添加了 {added_count} 个键值对")
                if added_count > 0: self.set_modified(True)

            else:
                new_key, new_val = result
                if new_key in self.data[self.current_table]:
                    QMessageBox.critical(self, "错误", f"键名 '{new_key}' 已存在！")
                    return
                self.data[self.current_table][new_key] = new_val
                self.refresh_keys()
                self.update_status(f"已添加键: {new_key}")
                self.set_modified(True)

    def delete_key(self):
        is_global_search = self.global_search_button.isChecked()
        if not self.current_table and not is_global_search: return
        
        rows = self.table.selectionModel().selectedRows()
        if not rows: return
        
        msg_box = QMessageBox(QMessageBox.Icon.Question, "确认", f"是否删除选中的 {len(rows)} 个键值对？", 
                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, self)
        msg_box.button(QMessageBox.StandardButton.Yes).setText("是")
        msg_box.button(QMessageBox.StandardButton.No).setText("否")
        
        if msg_box.exec() == QMessageBox.StandardButton.Yes:
            deleted_count = 0
            sorted_rows = sorted(rows, key=lambda idx: idx.row(), reverse=True)

            for idx in sorted_rows:
                row_index = idx.row()
                if self.table.columnSpan(row_index, 0) > 1: continue

                if is_global_search:
                    table_name = ""
                    for i in range(row_index, -1, -1):
                        if self.table.columnSpan(i, 0) > 1:
                            text = self.table.item(i, 0).text()
                            table_name = text.replace("以下是：", "").replace(" 的键值对", "")
                            break
                    if not table_name: continue
                    key_to_delete = self.table.item(row_index, 1).text()
                else:
                    table_name = self.current_table
                    key_to_delete = self.table.item(row_index, 1).text()
                
                if table_name in self.data and key_to_delete in self.data[table_name]:
                    del self.data[table_name][key_to_delete]
                    deleted_count += 1

            self.search_key_value()
            self.update_status(f"已删除 {deleted_count} 个键值对")
            if deleted_count > 0: self.set_modified(True)

    def copy_selected(self):
        is_global_search = self.global_search_button.isChecked()
        if not self.current_table and not is_global_search: return

        rows = self.table.selectionModel().selectedRows()
        if not rows: return
        
        pairs = []
        for idx in rows:
            row_index = idx.row()
            if self.table.columnSpan(row_index, 0) > 1: continue

            if is_global_search:
                table_name = ""
                for i in range(row_index, -1, -1):
                    if self.table.columnSpan(i, 0) > 1:
                        text = self.table.item(i, 0).text()
                        table_name = text.replace("以下是：", "").replace(" 的键值对", "")
                        break
                if not table_name: continue
                k = self.table.item(row_index, 1).text()
            else:
                table_name = self.current_table
                k = self.table.item(row_index, 1).text()

            v = self.data[table_name].get(k, "")
            pairs.append(f"{k}={v}")
            
        if pairs:
            QGuiApplication.clipboard().setText("\n".join(pairs))
            self.update_status(f"已复制 {len(pairs)} 个键值对到剪贴板")

    def new_gxt(self):
        if self.modified and not self.prompt_save(): return
        dlg = VersionDialog(self, default="V")
        if dlg.exec() != QDialog.DialogCode.Accepted: return
        self.data.clear()
        self.version = dlg.get_value()
        self.filepath = None
        self.file_type = 'gxt'
        self.current_table = None
        if self.version == 'III' or self.version == 'V':
             self.data["MAIN"] = {}
        self.table_search.clear()
        self.filter_tables()
        if self.global_search_button.isChecked():
            self.search_key_value()
        if self.table_list.count() > 0: self.table_list.setCurrentRow(0)
        self.update_status(f"已创建新GXT文件 (版本: {self.version})")
        self._update_ui_for_version()
        self.set_modified(False)
        QMessageBox.information(self, "成功", f"已成功创建新的GXT文件\n版本: {self.version}")

    def new_whm(self):
        """新建WHM文件"""
        if self.modified and not self.prompt_save(): return
        self.data.clear()
        self.version = "IV"
        self.filepath = None
        self.file_type = 'dat'
        self.current_table = "whm_table"
        self.data[self.current_table] = {}
        self.table_search.clear()
        self.filter_tables()
        if self.global_search_button.isChecked():
            self.search_key_value()
        if self.table_list.count() > 0: self.table_list.setCurrentRow(0)
        self.update_status("已创建新WHM文件")
        self._update_ui_for_version()
        self.set_modified(False)
        QMessageBox.information(self, "成功", "已成功创建新的WHM文件")

    def open_file_dialog(self):
        if self.modified and not self.prompt_save(): return
        path, _ = QFileDialog.getOpenFileName(self, "打开文件", "", "GTA文本文件 (*.gxt *.gxt2 whm_table.dat);;GXT文件 (*.gxt *.gxt2);;WHM Table (whm_table.dat);;所有文件 (*.*)")
        self.open_file(path)

    def open_gxt(self, path=None):
        try:
            with MemoryMappedFile(path) as mm:
                version = getVersion(mm)
                if not version:
                    raise ValueError("无法识别的 GXT 文件版本。")

                reader = getReader(version)
                mm.seek(0)
                self.data.clear()

                if reader.hasTables():
                    for name, offset in reader.parseTables(mm):
                        mm.seek(offset)
                        self.data[name] = dict(reader.parseTKeyTDat(mm))
                else:
                    self.data["MAIN"] = dict(reader.parseTKeyTDat(mm))

                self.version = version
                self.filepath = path
                self.file_type = 'gxt'
                self.table_search.clear()
                self.filter_tables()
                if self.global_search_button.isChecked():
                    self.search_key_value()
                if self.table_list.count() > 0: self.table_list.setCurrentRow(0)
                self.update_status(f"已打开GXT文件: {os.path.basename(path)}, 版本: {version}")
                
                version_map = {'IV': 'GTA4', 'VC': 'Vice City', 'SA': 'San Andreas', 'III': 'GTA3'}
                display_version = version_map.get(version, version)
                total_keys = sum(len(table) for table in self.data.values())
                
                QMessageBox.information(self, "成功", f"已成功打开GXT文件\n版本: {display_version}\n表数量: {len(self.data)}\n键值对总数: {total_keys}")
                self._update_ui_for_version()
                self.set_modified(False)
        except Exception as e:
            QMessageBox.critical(self, "错误", f"打开文件失败: {str(e)}")

    def open_gxt2(self, path):
        try:
            parsed_data = gta5_gxt2.parse_gxt2(path)
            self.data.clear()

            table_name = Path(path).stem
            self.data[table_name] = {f'0x{h:08X}': v for h, v in parsed_data.items()}

            self.version = 'V'
            self.filepath = path
            self.file_type = 'gxt'
            self.table_search.clear()
            self.filter_tables()
            if self.table_list.count() > 0:
                self.table_list.setCurrentRow(0)
            
            self.update_status(f"已打开GXT2文件: {os.path.basename(path)}, 版本: V")
            total_keys = len(self.data[table_name])
            QMessageBox.information(self, "成功", f"已成功打开GTA V GXT2文件\n键值对总数: {total_keys}")
            
            self._update_ui_for_version()
            self.set_modified(False)

        except Exception as e:
            QMessageBox.critical(self, "错误", f"打开 GXT2 文件失败: {str(e)}")


    def open_dat(self, path=None):
        """
        打开 whm_table.dat 文件
        (已重构为使用 GTA4_WHM_Text_Extractor.py 的逻辑)
        """
        try:
            data = Path(path).read_bytes()
            if len(data) < 4:
                raise ValueError(f"文件 {path} 太小")
            
            count = struct.unpack_from("<I", data, 0)[0]
            off = 4
            
            entries = []
            entry_size = struct.calcsize("<II")
            
            if off + (entry_size * count) > len(data):
                 raise ValueError(f"文件 {path} 在条目表处被截断")
            
            for _ in range(count):
                h, o = struct.unpack_from("<II", data, off)
                entries.append((h, o))
                off += entry_size
            
            if off + 4 > len(data):
                 raise ValueError(f"文件 {path} 在数据块大小处被截断")
            
            blob_size = struct.unpack_from("<I", data, off)[0]
            blob_start = off + 4
            
            if blob_start + blob_size > len(data):
                print(f"警告: Blob size {blob_size} 超出文件大小，已自动调整...")
                blob_size = len(data) - blob_start
            
            blob = data[blob_start:blob_start + blob_size]
            
            self.data.clear()
            table_name = "whm_table"
            self.data[table_name] = {}
            
            for h, off in entries:
                if off < blob_size:
                    j = off
                    while j < blob_size and blob[j] != 0:
                        j += 1
                    bts = blob[off:j]
                    text = self.whm_exporter.decode_bytes(bts) 
                else:
                    text = "[BINARY]"
                
                key = f'0x{h:08X}'
                self.data[table_name][key] = text
                
            self.version = "IV"
            self.filepath = path
            self.file_type = 'dat'
            self.table_search.clear()
            self.filter_tables()
            if self.global_search_button.isChecked():
                self.search_key_value()
            if self.table_list.count() > 0: self.table_list.setCurrentRow(0)
            
            self.update_status(f"已打开DAT文件: {os.path.basename(path)}")
            QMessageBox.information(self, "成功", f"已成功打开 whm_table.dat 文件\n条目数量: {len(self.data[table_name])}")
            self._update_ui_for_version()
            self.set_modified(False)
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"打开文件失败: {str(e)}")

    def open_txt(self, files=None):
        is_merge_mode = self.version is not None
        
        if not is_merge_mode:
            if self.modified and not self.prompt_save():
                return
            
            dlg = VersionDialog(self, default="V")
            if dlg.exec() != QDialog.DialogCode.Accepted:
                return
            version = dlg.get_value()
        else:
            version = self.version

        if not files:
            files, _ = QFileDialog.getOpenFileNames(self, "打开TXT文件", "", "文本文件 (*.txt);;所有文件 (*.*)")
        if not files:
            return

        progress = QProgressDialog("正在准备导入...", "取消", 0, 1, self)
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.setWindowTitle("正在导入TXT文件")
        progress.setLabelText("正在解析文件...")
        progress.show()
        QApplication.processEvents()

        try:
            temp_data = {}
            all_errors = []

            if version == 'V':
                for file_path in files:
                    parsed_dict = gta5_gxt2.parse_txt(file_path)
                    table_name = Path(file_path).stem
                    if table_name not in temp_data:
                        temp_data[table_name] = {}
                    for h, v in parsed_dict.items():
                        temp_data[table_name][f'0x{h:08X}'] = v
            else:
                temp_data, all_errors = self._load_standard_txt(files, version)

            progress.setValue(1)

            if all_errors:
                self._show_txt_import_errors(all_errors, version)
                return

            if not is_merge_mode:
                self.data = temp_data
                self.version = version
                self.filepath = None
                self.file_type = 'gxt'
                self.set_modified(True)
                QMessageBox.information(self, "成功", f"已成功打开 {len(files)} 个TXT文件\n版本: {version}\n表数量: {len(self.data)}")
            else:
                self._merge_data_with_optimized_prompt(temp_data)

            self.table_search.clear()
            self.filter_tables()
            if self.global_search_button.isChecked():
                self.search_key_value()
            if self.table_list.count() > 0:
                self.table_list.setCurrentRow(0)
            self.update_status(f"已成功处理 {len(files)} 个TXT文件 (版本: {version})")
            self._update_ui_for_version()

        except Exception as e:
            progress.close()
            QMessageBox.critical(self, "错误", f"打开或合并文件时发生意外错误: {e}")

    def _show_txt_import_errors(self, all_errors, version):
        """显示一个包含所有TXT导入错误的、格式化的高亮对话框。"""
        dialog = QDialog(self)
        dialog.setWindowTitle("TXT 导入错误")
        dialog.setMinimumSize(800, 600)
        
        layout = QVBoxLayout(dialog)
        
        grouped_errors = defaultdict(lambda: defaultdict(list))
        for file_path, line_num, line_content, msg in all_errors:
            filename = Path(file_path).name
            grouped_errors[filename][msg].append((line_num, line_content))

        html_content = """
        <p>在导入的TXT文件中发现以下错误，操作已中止。请修正后重试。</p>
        """

        for filename, errors_by_msg in grouped_errors.items():
            html_content += f'<h3 style="color: #82b1ff; margin-top: 15px; margin-bottom: 5px;">文件: {html.escape(filename)}</h3>'
            for msg, lines in errors_by_msg.items():
                html_content += f'<p style="color: #ffcc80; margin-left: 15px;"><b>错误: {html.escape(msg)}</b> (共 {len(lines)} 处)</p>'
                html_content += '<div style="font-family: Consolas, monospace; background-color: #2a2a2a; border-left: 3px solid #f44336; padding: 10px; margin-left: 30px; border-radius: 4px;">'
                for line_num, line_content in lines[:20]:
                    html_content += f'<span style="color: #9e9e9e;">{line_num}: </span><span style="color: #ef9a9a;">{html.escape(line_content)}</span><br>'
                if len(lines) > 20:
                    html_content += f'<span style="color: #9e9e9e;">...等另外 {len(lines)-20} 个类似错误</span>'
                html_content += '</div>'
        
        html_content += '<hr style="border-color: #444; margin-top: 20px;">'
        html_content += '<h4>正确格式示例:</h4>'
        
        example_style = 'style="font-family: Consolas, monospace; background-color: #2a2a2a; border: 1px solid #444; padding: 10px; border-radius: 4px; margin-top: 5px;"'
        
        examples = {
            'IV': (
                "<b>对于 GTA IV (需要表):</b>",
                "[TABLE_NAME]\nPLAINTEXT_KEY=Some Text\n0x12345678=Text with hash key"
            ),
            'VC': (
                "<b>对于 GTA: Vice City (需要表):</b>",
                "[MAIN]\nKEY_1=Some Text\nUPPER_7=Max 7 chars, upper, digit, _"
            ),
            'SA': (
                "<b>对于 GTA: San Andreas (需要表):</b>",
                "[MAIN]\ndeadbeef=Some Text\n1a2b3c=Max 8 hex chars"
            ),
            'III': (
                "<b>对于 GTA III (不需要表):</b>",
                "Key_1=Some Text\nMax_7_Ch=Max 7 chars, alphanumeric, _"
            ),
            'V': (
                "<b>对于 GTA V (不需要表):</b>",
                "PLAINTEXT_KEY=Some Text\n0x12345678=Text with hash key"
            ),
        }
        
        title, example_text = examples.get(version, ("<b>通用格式:</b>", "[TABLE_IF_NEEDED]\nkey=value"))

        html_content += f"<p>{title}</p>"
        html_content += f"<pre {example_style}>{html.escape(example_text)}</pre>"

        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setHtml(html_content)
        
        layout.addWidget(text_edit)
        
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        buttons.accepted.connect(dialog.accept)
        layout.addWidget(buttons)
        
        dialog.exec()

    def _merge_data_with_optimized_prompt(self, temp_data):
        """优化的合并逻辑：先检查所有冲突，再进行一次性询问"""
        existing_keys = set((table, key) for table, keys in self.data.items() for key in keys)
        conflicts = []
        for table, keys in temp_data.items():
            for key in keys:
                if (table, key) in existing_keys:
                    conflicts.append((table, key))

        should_overwrite = False
        if conflicts:
            msg_box = QMessageBox(QMessageBox.Icon.Question, "确认覆盖",
                                  f"发现 {len(conflicts)} 个重复的键值对。是否要全部覆盖？",
                                  QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, self)
            msg_box.button(QMessageBox.StandardButton.Yes).setText("是")
            msg_box.button(QMessageBox.StandardButton.No).setText("否")
            msg_box.setDefaultButton(QMessageBox.StandardButton.No)
            if msg_box.exec() == QMessageBox.StandardButton.Yes:
                should_overwrite = True
        
        added_count = 0
        overwritten_count = 0
        for table_name, table_data in temp_data.items():
            if table_name not in self.data:
                self.data[table_name] = {}
                
            for key, value in table_data.items():
                if key in self.data[table_name]:
                    if should_overwrite:
                        self.data[table_name][key] = value
                        overwritten_count += 1
                else:
                    self.data[table_name][key] = value
                    added_count += 1
        
        if added_count > 0 or overwritten_count > 0:
            self.set_modified(True)
            QMessageBox.information(self, "合并完成", f"合并完成。\n\n- 新增键值: {added_count}\n- 覆盖键值: {overwritten_count}")

    def _update_ui_for_version(self):
        """根据当前版本更新UI状态"""
        is_dat = self.file_type == 'dat'
        is_gta5 = self.version == 'V'
        
        can_manage_tables = not is_dat and not is_gta5
        self.btn_add_table.setEnabled(can_manage_tables)
        self.btn_del_table.setEnabled(can_manage_tables)
        
        self.table_list.setContextMenuPolicy(
            Qt.ContextMenuPolicy.NoContextMenu if is_dat or is_gta5 else Qt.ContextMenuPolicy.DefaultContextMenu
        )
        
        self.font_generator_action.setEnabled(not is_gta5)

    def save_file(self):
        if not self.version: 
            QMessageBox.warning(self, "警告", "请先打开或新建一个文件")
            return
        if self.filepath: 
            self._save_to_path(self.filepath)
        else: 
            self.save_file_as()

    def save_file_as(self):
        if not self.version:
            QMessageBox.warning(self, "警告", "请先打开或新建一个文件")
            return

        if self.file_type == 'dat':
            default_name = os.path.basename(self.filepath) if self.filepath else "whm_table.dat"
            filter_str = "WHM Table (whm_table.dat)"
            expected_filename = 'whm_table.dat'
        elif self.version == 'V':
            default_name = os.path.basename(self.filepath) if self.filepath else "output.gxt2"
            filter_str = "GXT2文件 (*.gxt2)"
            expected_ext = '.gxt2'
        else:
            default_name = os.path.basename(self.filepath) if self.filepath else "output.gxt"
            filter_str = "GXT文件 (*.gxt)"
            expected_ext = '.gxt'

        path, _ = QFileDialog.getSaveFileName(self, "保存文件", default_name, filter_str)
        
        if not path:
            return

        if self.file_type == 'dat':
            if os.path.basename(path).lower() != expected_filename:
                QMessageBox.critical(self, "保存错误", f"文件类型不匹配。\n文件名必须是 '{expected_filename}'。")
                return
        else: 
            if not path.lower().endswith(expected_ext):
                QMessageBox.critical(self, "保存错误", f"文件类型不匹配。\n请使用 '{expected_ext}' 扩展名保存此文件类型。")
                return
            
        self._save_to_path(path)
        self.filepath = path

    def _save_to_path(self, path):
        if self.file_type == 'dat':
            try:
                table_content = self.data.get("whm_table", {})
                
                text_table: List[WhmTextData] = []
                text_data = bytearray()

                for key, text in table_content.items():
                    try:
                        hash_val = int(key, 16)
                    except ValueError:
                        print(f"警告：跳过无效的哈希键 '{key}'")
                        continue
                    
                    try:
                        encoded_str = text.encode('utf-8')
                    except UnicodeEncodeError:
                        encoded_str = text.encode('utf-8', errors='replace')
                    
                    offset = len(text_data)
                    text_data.extend(encoded_str)
                    text_data.append(0)
                    
                    bin_entry = WhmTextData()
                    bin_entry.hash = hash_val
                    bin_entry.offset = offset
                    text_table.append(bin_entry)

                with open(path, "wb") as out:
                    out.write(struct.pack("<I", len(text_table)))
                    
                    for bin_entry in text_table:
                        out.write(struct.pack("<II", bin_entry.hash, bin_entry.offset))
                    
                    out.write(struct.pack("<I", len(text_data)))
                    
                    out.write(text_data)
                    
                QMessageBox.information(self, "成功", f"whm_table.dat 文件已保存到 {path}")
                self.set_modified(False)
            except Exception as e:
                QMessageBox.critical(self, "错误", f"保存 whm_table.dat 文件失败: {str(e)}")
            return
        
        gen_extra = False
        if self.version != 'V':
            if self.remember_gen_extra_choice is None:
                msg_box = QMessageBox(QMessageBox.Icon.Question, "确认", "是否生成字符映射辅助文件？", 
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, self)
                msg_box.button(QMessageBox.StandardButton.Yes).setText("是")
                msg_box.button(QMessageBox.StandardButton.No).setText("否")
                check_box = QCheckBox("记住我的选择")
                msg_box.setCheckBox(check_box)
                reply = msg_box.exec()

                gen_extra = (reply == QMessageBox.StandardButton.Yes)
                if check_box.isChecked():
                    self.remember_gen_extra_choice = gen_extra
                    self._save_settings()
            else:
                gen_extra = self.remember_gen_extra_choice

        original_dir = os.getcwd()
        try:
            dir_name = os.path.dirname(path)
            if dir_name:
                os.chdir(dir_name)
            
            if self.version == 'V':
                strings_to_save = {}
                for table_content in self.data.values():
                    for key, value in table_content.items():
                        try:
                            if key.lower().startswith('0x'):
                                hash_val = int(key, 16)
                            else:
                                hash_val = gta5_gxt2.joaat(key)
                            strings_to_save[hash_val] = value
                        except ValueError:
                            print(f"警告：跳过无效的键 '{key}'")
                gta5_gxt2.save_gxt2(strings_to_save, os.path.basename(path))

            elif self.version == 'IV':
                m_Data = {}
                all_chars = set()
                for table_name, entries_dict in self.data.items():
                    m_Data[table_name] = []
                    for key_str, translated_text in entries_dict.items():
                        hash_str = f'0x{gta4_gxt_hash(key_str):08X}' if not key_str.lower().startswith('0x') else key_str
                        m_Data[table_name].append({'hash_string': hash_str, 'text': translated_text})
                        if gen_extra: all_chars.update(c for c in translated_text if ord(c) > 255)
                write_iv(m_Data, Path(os.path.basename(path)))
                if gen_extra: process_special_chars(all_chars)
            elif self.version == 'VC':
                g = VCGXT()
                sorted_items = sorted(self.data.items(), key=cmp_to_key(lambda a, b: -1 if g._table_sort_method(a[0], b[0]) else 1))
                sorted_data = OrderedDict(sorted_items)
                g.m_GxtData = {t: {k: g._utf8_to_utf16(v) for k, v in d.items()} for t, d in sorted_data.items()}
                if gen_extra: 
                    all_chars = {c for table in self.data.values() for value in table.values() for c in value}
                    g.m_WideCharCollection = {ord(c) for c in all_chars if ord(c) > 0x7F}
                    g.GenerateQCJWStuff()
                else:
                    if hasattr(g, 'm_WideCharCollection'): 
                        g.m_WideCharCollection.clear()
                g.SaveAsGXT(os.path.basename(path))
            elif self.version == 'SA':
                g = SAGXT()
                def table_sort_method(lhs, rhs):
                    if rhs == "MAIN":
                        return False
                    if lhs == "MAIN":
                        return True
                    return lhs < rhs
                
                sorted_items = sorted(self.data.items(), key=cmp_to_key(lambda a, b: -1 if table_sort_method(a[0], b[0]) else 1))
                sorted_data = OrderedDict(sorted_items)
                g.m_GxtData = {t: {int(k, 16): v for k, v in d.items()} for t, d in sorted_data.items()}
                if gen_extra: 
                    all_chars = {c for table in self.data.values() for value in table.values() for c in value}
                    g.m_WideCharCollection = {c for c in all_chars if ord(c) > 0x7F}
                    g.generate_qcjw_stuff()
                else:
                    if hasattr(g, 'm_WideCharCollection'): 
                        g.m_WideCharCollection.clear()
                g.save_as_gxt(os.path.basename(path))
            elif self.version == 'III':
                g = LCGXT()
                g.m_GxtData = {k: g.utf8_to_utf16(v) for k, v in self.data.get('MAIN', {}).items()}
                if gen_extra: 
                    all_chars = {c for v in self.data.get('MAIN', {}).values() for c in v}
                    g.m_WideCharCollection = {ord(c) for c in all_chars if ord(c) >= 0x80}
                    g.generate_qcjw_stuff()
                else:
                    if hasattr(g, 'm_WideCharCollection'): 
                        g.m_WideCharCollection.clear()
                g.save_as_gxt(os.path.basename(path))
            QMessageBox.information(self, "成功", f"文件已保存到 {path}")
            self.set_modified(False)
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存文件失败: {str(e)}")
        finally:
            os.chdir(original_dir)

    def export_txt(self, single=True):
        if not self.data: 
            QMessageBox.warning(self, "警告", "没有数据可导出")
            return
        try:
            if single:
                default_filename = self.version_filename_map.get(self.version, "merged.txt")
                filepath, _ = QFileDialog.getSaveFileName(self, "导出为单个TXT文件", default_filename, "文本文件 (*.txt)")
                if not filepath: return
                with open(filepath, 'w', encoding='utf-8') as f:
                    for i, (t, d) in enumerate(sorted(self.data.items())):
                        if i > 0: f.write("\n\n")
                        if self.version not in ['III', 'V'] and self.file_type != 'dat': f.write(f"[{t}]\n")
                        for k, v in sorted(d.items()): f.write(f"{k}={v}\n")
                QMessageBox.information(self, "导出成功", f"已导出到: {filepath}")
            else:
                if self.version == 'III' or self.version == 'V' or self.file_type == 'dat':
                    QMessageBox.warning(self, "提示", "该文件类型不支持导出为多个TXT。")
                    return
                
                parent_dir = QFileDialog.getExistingDirectory(self, "请选择保存导出文件夹的位置")
                if not parent_dir:
                    return

                default_dirname = {'IV': 'GTA4_txt', 'VC': 'GTAVC_txt', 'SA': 'GTASA_txt'}.get(self.version, "gxt_export")
                
                while True:
                    base_name, ok = QInputDialog.getText(self, "导出多个TXT", "请输入导出文件夹的名称：", text=default_dirname)
                    if not ok or not base_name.strip(): 
                        return
                    
                    base_name = base_name.strip()
                    
                    invalid_chars = r'[\\/:*?"<>|]'
                    if re.search(invalid_chars, base_name):
                        QMessageBox.warning(self, "名称无效", f"文件夹名称不能包含以下任何字符:\n{invalid_chars}")
                        default_dirname = base_name
                        continue
                    
                    break
                
                export_dir = os.path.join(parent_dir, base_name)
                
                if os.path.exists(export_dir):
                    msg_box = QMessageBox(QMessageBox.Icon.Question, "确认", f"目录 '{export_dir}' 已存在，是否覆盖？", 
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, self)
                    msg_box.button(QMessageBox.StandardButton.Yes).setText("是")
                    msg_box.button(QMessageBox.StandardButton.No).setText("否")
                    if msg_box.exec() != QMessageBox.StandardButton.Yes: return
                    shutil.rmtree(export_dir)
                os.makedirs(export_dir)
                for t, d in sorted(self.data.items()):
                    with open(os.path.join(export_dir, f"{t}.txt"), 'w', encoding='utf-8') as f:
                        f.write(f"[{t}]\n")
                        for k, v in sorted(d.items()): f.write(f"{k}={v}\n")
                QMessageBox.information(self, "导出成功", f"已导出 {len(self.data)} 个文件到:\n{export_dir}")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败: {str(e)}")

    def _load_standard_txt(self, files, version):
        data = {}
        all_errors = []
        has_tables = version not in ['III', 'V'] and self.file_type != 'dat'

        for file_path in files:
            current_table = "MAIN" if not has_tables else None
            if not has_tables and "MAIN" not in data:
                data["MAIN"] = {}
            
            try:
                with open(file_path, 'r', encoding='utf-8-sig') as f:
                    content = f.readlines()
            except UnicodeDecodeError:
                try:
                    with open(file_path, 'r', encoding='gbk') as f:
                        content = f.readlines()
                except Exception as e:
                    all_errors.append((file_path, 0, "", f"文件编码错误，无法读取: {e}"))
                    continue
            except Exception as e:
                all_errors.append((file_path, 0, "", f"文件读取失败: {e}"))
                continue

            for line_num, line in enumerate(content, 1):
                line_content = line.strip()
                if not line_content or line_content.startswith('//') or line_content.startswith('#'):
                    continue
                
                if has_tables and line_content.startswith('[') and line_content.endswith(']'):
                    current_table = line_content[1:-1].strip()
                    if current_table and current_table not in data:
                        data[current_table] = {}
                elif '=' in line_content:
                    if current_table is None:
                        all_errors.append((file_path, line_num, line_content, "格式错误: 在定义表([Table])之前出现了键值对"))
                        continue

                    key, value = line_content.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    is_valid, msg = _validate_key_for_import_optimized(key, version)
                    if not is_valid:
                        all_errors.append((file_path, line_num, line_content, msg))
                        continue
                        
                    if key:
                        if version == 'IV' and not key.lower().startswith('0x'):
                            final_key = f'0x{gta4_gxt_hash(key):08X}'
                        else:
                            final_key = key
                        
                        data[current_table][final_key] = value
                else:
                    all_errors.append((file_path, line_num, line_content, "格式错误: 行既不是表头也不是 'key=value' 格式"))

        return data, all_errors


    def open_codepage_converter(self):
        """打开码表转换工具"""
        if not self.data:
            QMessageBox.warning(self, "警告", "请先打开或新建一个文件。")
            return
        
        dialog = CodepageConverterDialog(self)
        dialog.exec()

    def open_whm_batch_tool(self):
        """打开WHM批量处理工具对话框"""
        if self.whm_batch_tool_instance is None or not self.whm_batch_tool_instance.isVisible():
            self.whm_batch_tool_instance = WhmBatchToolDialog(self)
            self.whm_batch_tool_instance.show()
        else:
            self.whm_batch_tool_instance.activateWindow()
            self.whm_batch_tool_instance.raise_()

    def collect_and_filter_chars(self):
        """根据当前版本对应的CHARACTERS.txt逻辑收集和筛选GXT中的特殊字符"""
        if not self.data:
            return ""
    
        if self.version == 'IV':
            all_chars = {char for table in self.data.values() for value in table.values() for char in value}
            special_chars = set()
            for char in all_chars:
                if ord(char) > 255:
                    special_chars.add(char)
        
            special_chars.discard(chr(0x2122))
            special_chars.discard(chr(0x3000))
            special_chars.discard(chr(0xFEFF))
        
        elif self.version == 'VC':
            all_chars = {char for table in self.data.values() for value in table.values() for char in value}
            special_chars = set()
            for char in all_chars:
                code_point = ord(char)
                if code_point > 0x7F:
                    special_chars.add(char)
                
        elif self.version == 'SA':
            all_chars = {char for table in self.data.values() for value in table.values() for char in value}
            special_chars = set()
            for char in all_chars:
                if ord(char) > 0x7F:
                    special_chars.add(char)
                
        elif self.version == 'III':
            all_chars = {char for table in self.data.values() for value in table.values() for char in value}
            special_chars = set()
            for char in all_chars:
                code_point = ord(char)
                if code_point >= 0x80:
                    special_chars.add(char)
                
        else:
            all_chars = {char for table in self.data.values() for value in table.values() for char in value}
            special_chars = {char for char in all_chars if ord(char) > 255}
            special_chars.discard(chr(0x2122))
            special_chars.discard(chr(0x3000))
            special_chars.discard(chr(0xFEFF))
    
        sorted_chars = sorted(special_chars, key=lambda c: ord(c))
        return "".join(sorted_chars)
        
    def open_font_generator(self):
        initial_chars = self.collect_and_filter_chars()
        current_version = self.version if self.version else "IV"
        dlg = FontGeneratorDialog(self, initial_chars, initial_version=current_version)
        
        if dlg.exec() != QDialog.DialogCode.Accepted: return
            
        settings = dlg.get_settings()
        if not settings["characters"]:
            QMessageBox.warning(self, "提示", "没有需要生成的字符，操作已取消。")
            return
            
        output_dir = QFileDialog.getExistingDirectory(self, "选择保存字体贴图的目录")
        if not output_dir: return
            
        try:
            self.update_status("正在生成字体贴图，请稍候...")
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)

            generator = FontTextureGenerator()
            version = settings["version"]

            path_font = os.path.join(output_dir, 'font.png')
            generator.generate_and_save(settings["characters"], path_font, version, settings["resolution"], settings["font_normal"])
            html_path = os.path.join(output_dir, 'font_preview.html')
            generator.generate_html_preview(settings, path_font, html_path)
            QMessageBox.information(self, "生成成功", f"已成功生成文件:\n- {path_font}\n- {html_path}")
            
            self.update_status(f"成功生成字体贴图到: {output_dir}")
        except Exception as e:
            QMessageBox.critical(self, "生成失败", f"生成字体贴图时发生错误: {e}")
            self.update_status(f"字体贴图生成失败: {e}")
        finally:
            QApplication.restoreOverrideCursor()

    def update_status(self, message):
        self.status.showMessage(message)

    def show_about(self):
        QMessageBox.information(self, "关于", 
            "倾城剑舞 GXT 编辑器 v2.2\n"
            "支持 V/IV/VC/SA/III 的 GXT/TXT 编辑、导入导出。\n"
            "新增功能：文件关联、新建GXT、导出单个表、生成png透明汉化字体贴图、支持whm_table.dat编辑、码表转换工具、WHM文本提取工具。")

    def show_help(self):
        QMessageBox.information(self, "使用帮助", 
            "1. 打开文件：菜单或将 .gxt / .gxt2 / whm_table.dat / .txt 文件或包含txt的文件夹拖入窗口，也可通过文件关联gxt文件打开。\n"
            "2. 新建文件：文件菜单→新建GXT文件，选择游戏版本。\n"
            "3. 编辑：双击右侧列表中的任意条目，或右键选择“编辑”。\n"
            "4. 多选编辑：选择多行后右键选择“批量编辑”。\n"
            "5. 添加/删除：使用左侧或按钮条中的按钮进行操作。\n"
            "6. 复制：选择多行后右键选择“复制”。\n"
            "7. 保存：支持生成字符映射辅助文件（可选），并可记住选择。\n"
            "8. 导出：支持导出整个GXT或单个表为TXT文件。\n"
            "9. TXT 导入：支持单个或多个TXT导入并直接生成GXT。如果已有GXT打开，则会进行合并。\n"
            "10. GTA IV/V 特别说明：键名可为明文（如 T1_NAME_82）或哈希（0xhash），保存时自动转换哈希。\n"
            "11. WHM Table 支持：可以打开和保存以及编辑 GTA4 民间汉化补丁的 whm_table.dat 文件。\n"
            "12. 字体生成器：工具菜单→GTA字体贴图生成器，用于创建游戏字体PNG文件。以及支持加载外部字体文件，点击预览图可放大查看。【仅限：汉化字体贴图】\n"
            "13. 码表转换工具：用于根据自定义码表文件，对GXT文本内容进行字符的批量替换或还原。\n"
            "14. WHM 文本提取工具：用于批量处理 GTA4 的网页（.whm）文件，支持批量导出为TXT或从TXT生成whm_table.dat。")

    def set_file_association(self):
        if sys.platform != 'win32':
            QMessageBox.information(self, "提示", "文件关联功能目前仅支持Windows系统")
            return
        try:
            import winreg

            if getattr(sys, 'frozen', False):
                exe_path = sys.executable
                command = f'"{exe_path}" "%1"'
                icon_path_reg = f'"{exe_path}",0'
            else:
                python_exe = sys.executable
                script_path = os.path.abspath(sys.argv[0])
                command = f'"{python_exe}" "{script_path}" "%1"'
                icon_path_reg = f'"{python_exe}",0'

            with winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"Software\Classes\.gxt") as key:
                winreg.SetValue(key, '', winreg.REG_SZ, 'GXTEditor.File')
            
            with winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"Software\Classes\.gxt2") as key:
                winreg.SetValue(key, '', winreg.REG_SZ, 'GXTEditor.File')
            
            with winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"Software\Classes\GXTEditor.File") as key:
                winreg.SetValue(key, '', winreg.REG_SZ, 'GTA文本文件')
            with winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"Software\Classes\GXTEditor.File\DefaultIcon") as key:
                winreg.SetValue(key, '', winreg.REG_SZ, icon_path_reg)
            with winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"Software\Classes\GXTEditor.File\shell\open\command") as key:
                winreg.SetValue(key, '', winreg.REG_SZ, command)
            
            import ctypes
            ctypes.windll.shell32.SHChangeNotify(0x08000000, 0, None, None)
            QMessageBox.information(self, "成功", "已设置.gxt和.gxt2文件关联! 可能需要重启资源管理器或电脑生效。")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"设置文件关联失败: {str(e)}")

    def set_modified(self, modified):
        """设置修改状态并更新窗口标题"""
        if self.modified == modified: return
        self.modified = modified
        title = " GTA文本对话表编辑器 v2.2 作者：倾城剑舞"
        if self.filepath:
            title = f"{os.path.basename(self.filepath)} - {title}"
        if modified:
            title = f"*{title}"
        self.setWindowTitle(title)

    def prompt_save(self):
        """提示用户保存未保存的更改。返回True表示可以继续，False表示取消操作。"""
        if self.save_prompt_choice == 'Save':
            self.save_file()
            return not self.modified
        if self.save_prompt_choice == 'Discard':
            return True

        msg_box = QMessageBox(QMessageBox.Icon.Question, "确认", "文件已被修改，是否保存更改？",
                             QMessageBox.StandardButton.Save | 
                             QMessageBox.StandardButton.Discard | 
                             QMessageBox.StandardButton.Cancel, self)
        msg_box.button(QMessageBox.StandardButton.Save).setText("保存")
        msg_box.button(QMessageBox.StandardButton.Discard).setText("不保存")
        msg_box.button(QMessageBox.StandardButton.Cancel).setText("取消")
        
        check_box = QCheckBox("记住我的选择")
        msg_box.setCheckBox(check_box)
        
        reply = msg_box.exec()
        
        if check_box.isChecked():
            if reply == QMessageBox.StandardButton.Save:
                self.save_prompt_choice = 'Save'
                self._save_settings()
            elif reply == QMessageBox.StandardButton.Discard:
                self.save_prompt_choice = 'Discard'
                self._save_settings()

        if reply == QMessageBox.StandardButton.Save:
            self.save_file()
            return not self.modified
        elif reply == QMessageBox.StandardButton.Discard:
            return True
        else:
            return False

    def closeEvent(self, event):
        """重写关闭事件，检查是否有未保存的修改"""
        if self.modified:
            if self.prompt_save():
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()


if __name__ == "__main__":
    import sys
    QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    app = QApplication(sys.argv)

    translator = QTranslator()

    if getattr(sys, 'frozen', False):
        base_dir = Path(sys._MEIPASS)
        custom_trans_path = base_dir / "translations" / "zh_CN.qm"
        qt_trans_path = base_dir / "translations" / "qt_zh_CN.qm"
    else:
        base_dir = Path(__file__).parent
        custom_trans_path = base_dir / "translations" / "zh_CN.qm"
        translations_path = QLibraryInfo.path(QLibraryInfo.LibraryPath.TranslationsPath)
        qt_trans_path = Path(translations_path) / "qt_zh_CN.qm"

    loaded = False
    if custom_trans_path.exists() and translator.load(str(custom_trans_path)):
        app.installTranslator(translator)
        print("✅ 已加载自定义翻译:", custom_trans_path)
        loaded = True
    elif qt_trans_path.exists() and translator.load(str(qt_trans_path)):
        app.installTranslator(translator)
        print("✅ 已加载 Qt 自带中文语言包:", qt_trans_path)
        loaded = True

    if not loaded:
        print("⚠️ 未找到任何翻译文件")

    file_to_open = None
    if len(sys.argv) > 1 and os.path.exists(sys.argv[1]):
        file_lower = sys.argv[1].lower()
        if file_lower.endswith(('.gxt', '.gxt2')) or os.path.basename(file_lower) == 'whm_table.dat':
            file_to_open = sys.argv[1]

    editor = GXTEditorApp(file_to_open)
    editor.show()
    sys.exit(app.exec())