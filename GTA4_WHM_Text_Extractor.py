import sys
import os
import zlib
import re
import ctypes
import struct
from pathlib import Path
from enum import Enum
from typing import List, Set, Optional, NamedTuple

FNV_PRIME_32 = 0x01000193
FNV_OFFSET_BASIS_32 = 0x811C9DC5

def fnv1a_32(data: bytes) -> int:
    hash_val = FNV_OFFSET_BASIS_32
    for byte in data:
        hash_val = (hash_val ^ byte) * FNV_PRIME_32
        hash_val &= 0xFFFFFFFF
    return hash_val

c_uint = ctypes.c_uint
c_ushort = ctypes.c_ushort
c_ubyte = ctypes.c_ubyte
c_float = ctypes.c_float
c_int = ctypes.c_int
c_char = ctypes.c_char
c_bool = ctypes.c_bool
DWORD = ctypes.c_uint
BYTE = ctypes.c_ubyte

class rsc_flag_bits(ctypes.LittleEndianStructure):
    _fields_ = [
        ("virtual1xPageFlag", c_uint, 1),
        ("virtual2xPageFlag", c_uint, 1),
        ("virtual4xPageFlag", c_uint, 1),
        ("virtual8xPageFlag", c_uint, 1),
        ("virtual16xPageCount", c_uint, 7),
        ("virtual1xPageSize", c_uint, 4),
        ("physical1xPageFlag", c_uint, 1),
        ("physical2xPageFlag", c_uint, 1),
        ("physical4xPageFlag", c_uint, 1),
        ("physical8xPageFlag", c_uint, 1),
        ("physical16xPageCount", c_uint, 7),
        ("physical1xPageSize", c_uint, 4),
        ("useless", c_uint, 2),
    ]

class rsc_flag_union(ctypes.Union):
    _fields_ = [
        ("bits", rsc_flag_bits),
        ("flags", c_uint),
    ]

class rsc_flag(ctypes.Structure):
    _fields_ = [
        ("u", rsc_flag_union),
    ]

    def GetVirtualPageSize(self) -> int:
        return 1 << (self.u.bits.virtual1xPageSize + 8)

    def GetPhysicalPageSize(self) -> int:
        return 1 << (self.u.bits.physical1xPageSize + 8)

    def GetVirtualSize(self) -> int:
        return self.GetVirtualPageSize() * (
            self.u.bits.virtual1xPageFlag * 1 +
            self.u.bits.virtual2xPageFlag * 2 +
            self.u.bits.virtual4xPageFlag * 4 +
            self.u.bits.virtual8xPageFlag * 8 +
            self.u.bits.virtual16xPageCount * 16
        )

    def GetPhysicalSize(self) -> int:
        return self.GetPhysicalPageSize() * (
            self.u.bits.physical1xPageFlag * 1 +
            self.u.bits.physical2xPageFlag * 2 +
            self.u.bits.physical4xPageFlag * 4 +
            self.u.bits.physical8xPageFlag * 8 +
            self.u.bits.physical16xPageCount * 16
        )

class rsc_header(ctypes.Structure):
    _fields_ = [
        ("magic", c_uint),
        ("type", c_uint),
        ("flags", rsc_flag),
    ]

class ptr_element_type(Enum):
    Cpu_Type = 5
    Gpu_Type = 6

class pgPtr(ctypes.Structure):
    _fields_ = [
        ("_value", c_uint),
    ]

    @property
    def o(self) -> int:
        return self._value & 0x0FFFFFFF

    @property
    def t(self) -> int:
        return (self._value >> 28) & 0xF

    def locate(self, block: memoryview, struct_type):
        if not block or self.t != ptr_element_type.Cpu_Type.value:
            return None
        
        if self.o >= len(block):
            return None

        if struct_type == c_char:
            offset = self.o
            end = -1
            for i in range(offset, len(block)):
                if block[i] == 0:
                    end = i
                    break
            
            if end == -1:
                return None
            
            return block[offset:end].tobytes()

        try:
            return struct_type.from_buffer(block, self.o)
        except ValueError:
            return None

class pgString(pgPtr):
    def locate_str(self, block: memoryview, encoding='windows-1252') -> Optional[str]:
        raw_bytes = self.locate(block, c_char)
        return raw_bytes.decode(encoding) if raw_bytes is not None else None

class pgObjectArray(ctypes.Structure):
    _fields_ = [
        ("_ptr_value", c_uint),
        ("count", c_ushort),
        ("size", c_ushort),
    ]

    @property
    def o(self) -> int:
        return self._ptr_value & 0x0FFFFFFF

    @property
    def t(self) -> int:
        return (self._ptr_value >> 28) & 0xF

    def get_span(self, block: memoryview, element_type):
        if not block or self.t != ptr_element_type.Cpu_Type.value:
            return []
        
        offset = self.o
        num_elements = max(self.count, self.size)
        element_size = ctypes.sizeof(element_type)
        span = []

        if offset + num_elements * element_size > len(block):
            num_elements = self.count
            if offset + num_elements * element_size > len(block):
                print(f"警告: pgObjectArray 超出边界。偏移量: {offset}, 数量: {self.count}, 大小: {self.size}, 元素大小: {element_size}, 块大小: {len(block)}", file=sys.stderr)
                return []

        for i in range(num_elements):
            try:
                element = element_type.from_buffer(block, offset + i * element_size)
                span.append(element)
            except ValueError:
                print(f"警告: pgObjectArray 元素读取错误，索引: {i}", file=sys.stderr)
                break
        
        return span

class pgObjectPtrArray(pgObjectArray):
    pass

class HtmlNodeType(Enum):
    Node_HtmlNode = 0
    Node_HtmlDataNode = 1
    Node_HtmlTableNode = 2
    Node_HtmlTableElementNode = 3

class HtmlTag(Enum):
    HTMLTAG_HTML = 0
    HTMLTAG_TITLE = 1
    HTMLTAG_A = 2
    HTMLTAG_BODY = 3
    HTMLTAG_B = 4
    HTMLTAG_BR = 5
    HTMLTAG_CENTER = 6
    HTMLTAG_CODE = 7
    HTMLTAG_DL = 8
    HTMLTAG_DT = 9
    HTMLTAG_DD = 10
    HTMLTAG_DIV = 11
    HTMLTAG_EMBED = 12
    HTMLTAG_EM = 13
    HTMLTAG_HEAD = 14
    HTMLTAG_H1 = 15
    HTMLTAG_H2 = 16
    HTMLTAG_H3 = 17
    HTMLTAG_H4 = 18
    HTMLTAG_H5 = 19
    HTMLTAG_H6 = 20
    HTMLTAG_IMG = 21
    HTMLTAG_I = 22
    HTMLTAG_LINK = 23
    HTMLTAG_LI = 24
    HTMLTAG_META = 25
    HTMLTAG_OBJECT = 26
    HTMLTAG_OL = 27
    HTMLTAG_P = 28
    HTMLTAG_PARAM = 29
    HTMLTAG_SPAN = 30
    HTMLTAG_STRONG = 31
    HTMLTAG_STYLE = 32
    HTMLTAG_TABLE = 33
    HTMLTAG_TR = 34
    HTMLTAG_TH = 35
    HTMLTAG_TD = 36
    HTMLTAG_UL = 37
    HTMLTAG_TEXT = 38
    HTMLTAG_SCRIPTOBJ = 39

class HtmlAttrValue(Enum):
    HTMLATTRVAL_LEFT = 0
    HTMLATTRVAL_RIGHT = 1
    HTMLATTRVAL_CENTER = 2
    HTMLATTRVAL_JUSTIFY = 3
    HTMLATTRVAL_TOP = 4
    HTMLATTRVAL_BOTTOM = 5
    HTMLATTRVAL_MIDDLE = 6
    HTMLATTRVAL_INHERIT = 7
    HTMLATTRVAL_XXSMALL = 8
    HTMLATTRVAL_XSMALL = 9
    HTMLATTRVAL_SMALL = 10
    HTMLATTRVAL_MEDIUM = 11
    HTMLATTRVAL_LARGE = 12
    HTMLATTRVAL_XLARGE = 13
    HTMLATTRVAL_XXLARGE = 14
    HTMLATTRVAL_BLOCK = 15
    HTMLATTRVAL_INLINE = 18
    HTMLATTRVAL_NONE = 19
    HTMLATTRVAL_SOLID = 20
    HTMLATTRVAL_UNDERLINE = 21
    HTMLATTRVAL_OVERLINE = 22
    HTMLATTRVAL_LINETHROUGH = 23
    HTMLATTRVAL_BLINK = 24
    HTMLATTRVAL_REPEAT = 25
    HTMLATTRVAL_NOREPEAT = 26
    HTMLATTRVAL_REPEATX = 27
    HTMLATTRVAL_REPEATY = 28
    HTMLATTRVAL_COLLAPSE = 29
    HTMLATTRVAL_SEPARATE = 30
    HTMLATTRVAL_UNDEFINED = 0xFFFFFFFF

class CssProperty(Enum):
    CSS_WIDTH = 0
    CSS_HEIGHT = 1
    CSS_DISPLAY = 2
    CSS_BACKGROUND_COLOR = 3
    CSS_BACKGROUND_REPEAT = 4
    CSS_BACKGROUND_POSITION = 5
    CSS_BACKGROUND_IMAGE = 6
    CSS_COLOR = 7
    CSS_TEXT_ALIGN = 8
    CSS_TEXT_DECORATION = 9
    CSS_VERTICAL_ALIGN = 10
    CSS_FONT = 11
    CSS_FONT_SIZE = 12
    CSS_FONT_STYLE = 13
    CSS_FONT_WEIGHT = 14
    CSS_BORDER_COLLAPSE = 15
    CSS_BORDER_STYLE = 16
    CSS_BORDER_BOTTOM_STYLE = 17
    CSS_BORDER_LEFT_STYLE = 18
    CSS_BORDER_RIGHT_STYLE = 19
    CSS_BORDER_TOP_STYLE = 20
    CSS_BORDER_COLOR = 21
    CSS_BORDER_BOTTOM_COLOR = 22
    CSS_BORDER_LEFT_COLOR = 23
    CSS_BORDER_RIGHT_COLOR = 24
    CSS_BORDER_TOP_COLOR = 25
    CSS_BORDER_WIDTH = 26
    CSS_BORDER_BOTTOM_WIDTH = 27
    CSS_BORDER_LEFT_WIDTH = 28
    CSS_BORDER_RIGHT_WIDTH = 29
    CSS_BORDER_TOP_WIDTHT = 30
    CSS_MARGIN_BOTTOM = 31
    CSS_MARGIN_LEFT = 32
    CSS_MARGIN_RIGHT = 33
    CSS_MARGIN_TOP = 34
    CSS_PADDING_BOTTOM = 35
    CSS_PADDING_LEFT = 36
    CSS_PADDING_RIGHT = 37
    CSS_PADDING_TOP = 38
    CSS_UNUSED = 39

class HtmlRenderState(ctypes.Structure):
    _fields_ = [
        ("eDisplay", c_uint),
        ("fWidth", c_float),
        ("fHeight", c_float),
        ("_fC", c_float),
        ("_f10", c_float),
        ("_f14", c_ubyte * 4),
        ("_f18", c_float),
        ("_f1C", c_float),
        ("dwBgColor", c_uint),
        ("pBackgroundImage", pgPtr),
        ("_f28h", c_uint),
        ("_f28l", c_uint),
        ("backgroundRepeat", c_uint),
        ("dwColor", c_uint),
        ("eAlign", c_uint),
        ("eValign", c_uint),
        ("eTextDecoration", c_uint),
        ("_f44", c_uint),
        ("eFontSize", c_uint),
        ("nFontStyle", c_int),
        ("nFontWeight", c_int),
        ("_f54", c_float),
        ("dwBorderBottomColor", c_uint),
        ("eBorderBottomStyle", c_uint),
        ("fBorderBottomWidth", c_float),
        ("dwBorderLeftColor", c_uint),
        ("eBorderLeftStyle", c_uint),
        ("dwBorderLeftWidth", c_float),  # 修正：应该是float类型
        ("dwBorderRightColor", c_uint),
        ("eBorderRightStyle", c_uint),
        ("fBorderRightWidth", c_float),
        ("dwBorderTopColor", c_uint),
        ("eBorderTopStyle", c_uint),
        ("fBorderTopWidth", c_float),
        ("fMarginBottom", c_float),
        ("fMarginLeft", c_float),
        ("fMarginRight", c_float),
        ("fMarginTop", c_float),
        ("fPaddingBottom", c_float),
        ("fPaddingLeft", c_float),
        ("fPaddingRight", c_float),
        ("fPaddingTop", c_float),
        ("fCellPadding", c_float),
        ("fCellSpacing", c_float),
        ("nColSpan", c_int),
        ("nRowSpan", c_int),
        ("hasBackground", c_bool),
        ("isLink", c_bool),
        ("_BA", c_ubyte * 2),
        ("dwLinkColor", c_uint),
        ("_fC0", c_uint),
    ]

class CHtmlCssDeclaration(ctypes.Structure):
    _fields_ = [
        ("m_eProperty", c_uint),
        ("_f4", c_uint),
        ("_f8", c_uint),
        ("m_eDataType", c_uint),
    ]

class CHtmlCssSelector(ctypes.Structure):
    _fields_ = [
        ("m_eTag", c_uint),
        ("m_aDeclarations", pgObjectArray),
        ("_fC", pgPtr),
    ]

class CHtmlStylesheet(ctypes.Structure):
    _fields_ = [
        ("_f0", DWORD),
        ("_f4", pgObjectPtrArray),
        ("_padC", BYTE * 3),
        ("_fF", BYTE),
        ("m_pNext", pgPtr),
    ]

class CHtmlNode(ctypes.Structure):
    _fields_ = [
        ("vtbl", c_uint),
        ("m_eNodeType", c_uint),
        ("m_pParentNode", pgPtr),
        ("m_children", pgObjectPtrArray),
        ("m_renderState", HtmlRenderState),
    ]

class CHtmlDataNode(ctypes.Structure):
    _fields_ = [
        ("node_base", CHtmlNode),
        ("m_pData", pgString),
    ]

class CHtmlElementNode(ctypes.Structure):
    _fields_ = [
        ("node_base", CHtmlNode),
        ("m_eHtmlTag", c_uint),
        ("m_pszTagName", pgString),
        ("m_nodeParam", pgObjectArray),
    ]

class CHtmlTableNode(ctypes.Structure):
    _fields_ = [
        ("element_base", CHtmlElementNode),
        ("_fE8", pgPtr),
        ("_fEC", pgPtr),
        ("_fF0", pgPtr),
        ("_fF4", pgPtr),
        ("_fF8", pgPtr),
        ("m_dwCellCount", DWORD),
        ("_f100", DWORD),
    ]

class CHtmlTableElementNode(ctypes.Structure):
    _fields_ = [
        ("element_base", CHtmlElementNode),
        ("_fE8", c_int),
        ("_fEC", c_int),
    ]

class CHtmlDocument(ctypes.Structure):
    _fields_ = [
        ("m_pRootElement", pgPtr),
        ("m_pBody", pgPtr),
        ("m_pszTitle", pgString),
        ("m_pTxd", pgPtr),
        ("_f10", pgObjectPtrArray),
        ("m_childNodes", pgObjectPtrArray),
        ("m_pStylesheet", pgObjectPtrArray),
        ("pad", c_ubyte * 3),
        ("_f2B", c_ubyte),
    ]

class ExportedTextEntry(NamedTuple):
    hash: int
    str: bytes

class WhmTextData(ctypes.Structure):
    _fields_ = [
        ("hash", c_uint),
        ("offset", c_uint),
    ]

class CHtmlTextExport:
    
    def __init__(self):
        pass

    def ExportHtml(self, input_folder: Path):
        hashes: Set[int] = set()
        
        print(f"正在从以下位置导出: {input_folder}")
        file_count = 0
        for filename in input_folder.rglob("*.whm"):
            print(f"正在处理: {filename.name}")
            container = self.ExtractWhmStrings(filename, hashes)
            if container:
                self.ExportText(filename.with_suffix(".txt"), container)
            file_count += 1
        print(f"完成。已处理 {file_count} 个文件。")

    def GenerateDataBase(self, input_folder: Path, output_file: Path):
        texts: List[ExportedTextEntry] = []

        print(f"正在从以下位置生成数据库: {input_folder}")
        file_count = 0
        for filename in input_folder.rglob("*.txt"):
            texts.extend(self.LoadText(filename))
            file_count += 1
        
        print(f"从 {file_count} 个文件加载了 {len(texts)} 个条目。")
        
        # 按照 whm_table.dat 格式构建数据
        text_table: List[WhmTextData] = []
        text_data = bytearray()

        # 首先构建数据块
        for entry in texts:
            try:
                # 使用 UTF-8 编码而不是 windows-1252
                encoded_str = entry.str.encode('utf-8')
            except UnicodeEncodeError as e:
                print(f"警告: 文本 '{entry.str[:20]}...' (哈希 {entry.hash:08X}) 无法用 utf-8 编码: {e}", file=sys.stderr)
                encoded_str = entry.str.encode('utf-8', errors='replace')

            # 记录当前偏移量
            offset = len(text_data)
            
            # 添加到数据块
            text_data.extend(encoded_str)
            text_data.append(0)  # null 终止符
            
            # 创建条目
            bin_entry = WhmTextData()
            bin_entry.hash = entry.hash
            bin_entry.offset = offset
            text_table.append(bin_entry)

        try:
            with open(output_file, "wb") as out:
                # 1. 写入条目数量
                out.write(struct.pack("<I", len(text_table)))
                
                # 2. 写入条目表
                for bin_entry in text_table:
                    out.write(struct.pack("<II", bin_entry.hash, bin_entry.offset))
                
                # 3. 写入数据块大小
                out.write(struct.pack("<I", len(text_data)))
                
                # 4. 写入数据块
                out.write(text_data)
                
            print(f"成功创建数据库: {output_file}")
        except IOError as e:
            print(f"创建输出文件 {output_file} 失败: {e}")
            return

    def ParseWhmTable(self, input_file: Path, output_file: Path):
        """解析 whm_table.dat 文件并输出为文本格式"""
        try:
            data = input_file.read_bytes()
        except IOError as e:
            print(f"读取输入文件 {input_file} 失败: {e}", file=sys.stderr)
            return

        # 读取条目数量
        if len(data) < 4:
            print(f"错误: 文件 {input_file} 太小", file=sys.stderr)
            return
            
        count = struct.unpack_from("<I", data, 0)[0]
        off = 4
        
        # 读取条目表
        entries = []
        entry_size = struct.calcsize("<II")
        for _ in range(count):
            if off + entry_size > len(data):
                print(f"错误: 文件 {input_file} 在条目表处被截断", file=sys.stderr)
                return
                
            h, o = struct.unpack_from("<II", data, off)
            entries.append((h, o))
            off += entry_size
        
        # 读取数据块大小
        if off + 4 > len(data):
            print(f"错误: 文件 {input_file} 在数据块大小处被截断", file=sys.stderr)
            return
            
        blob_size = struct.unpack_from("<I", data, off)[0]
        blob_start = off + 4
        
        # 检查数据块大小
        if blob_start + blob_size > len(data):
            print(f"警告: 数据块大小 {blob_size} 超过文件大小，正在调整...", file=sys.stderr)
            blob_size = len(data) - blob_start
        
        blob = data[blob_start:blob_start + blob_size]
        
        # 解析所有条目
        results = []
        for h, off in entries:
            # 偏移量相对于数据块起始位置
            if off < blob_size:
                j = off
                while j < blob_size and blob[j] != 0:
                    j += 1
                bts = blob[off:j]
                # 尝试多种编码解码
                text = self.decode_bytes(bts)
            else:
                # 越界，标记为二进制
                text = "[二进制数据]"
                bts = b''
                
            results.append({"hash": h, "text": text})
        
        # 保存为文本文件
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                for item in results:
                    hash_str = f"0x{item['hash']:08X}"
                    f.write(f"{hash_str}={item['text']}\n")
            print(f"成功解析数据库: {output_file}")
        except IOError as e:
            print(f"写入输出文件 {output_file} 失败: {e}", file=sys.stderr)

    def decode_bytes(self, bts: bytes):
        """尝试多种编码解码字节数据"""
        for enc in ("utf-8", "cp1252", "latin1"):
            try:
                return bts.decode(enc)
            except Exception:
                continue
        return bts.hex()

    def ExportText(self, filename: Path, container: List[ExportedTextEntry]):
        if not container:
            return

        try:
            with open(filename, "w", encoding="utf-8") as out:
                out.write('\ufeff')

                for entry in container:
                    utf8_str = self.Windows1252ToUtf8(entry.str)
                    
                    utf8_str_escaped = utf8_str.replace('\n', '\\n').replace('\r', '\\r')

                    out.write(f";0x{entry.hash:08X}={utf8_str_escaped}\n")
                    out.write(f"0x{entry.hash:08X}=\n\n")
        except IOError as e:
            print(f"写入 {filename} 失败: {e}", file=sys.stderr)

    def IsBlankText(self, text: str) -> bool:
        return all(c == ' ' or c == '\t' for c in text)

    def LoadText(self, filename: Path) -> List[ExportedTextEntry]:
        result: List[ExportedTextEntry] = []
        entry_regex = re.compile(r"(0[xX][0-9a-fA-F]{8})=(.*)")

        try:
            with open(filename, "r", encoding="utf-8-sig") as stream:
                for line_no, line in enumerate(stream, 1):
                    line = line.strip()

                    if not line or line.startswith(';'):
                        continue

                    matches = entry_regex.match(line)
                    if matches:
                        hash_val = int(matches.group(1), 16)
                        text_str = matches.group(2)
                        
                        text_str = text_str.replace('\\n', '\n').replace('\\r', '\r')

                        try:
                            text_bytes = text_str.encode('windows-1252')
                        except UnicodeEncodeError:
                             text_bytes = b''
                        
                        if not self.IsBlankText(text_str) and hash_val != fnv1a_32(text_bytes):
                            result.append(ExportedTextEntry(hash=hash_val, str=text_str))
                    else:
                        print(f"{filename.name}: 第 {line_no} 行无法识别。", file=sys.stderr)

        except IOError as e:
            print(f"打开输入文件 {filename} 失败: {e}", file=sys.stderr)
        
        return result

    def UnpackWhm(self, input_file: Path) -> Optional[memoryview]:
        
        uncompressed_bytes = None
        try:
            with open(input_file, "rb") as f:
                # 读取RSC头部
                header_size = ctypes.sizeof(rsc_header)
                header_data = f.read(header_size)
                if len(header_data) < header_size:
                    print(f"错误: 文件 {input_file} 太小，不是有效的 WHM 文件。", file=sys.stderr)
                    return None

                header = rsc_header.from_buffer_copy(header_data)
                
                # 计算压缩数据大小
                f.seek(0, os.SEEK_END)
                compressed_size = f.tell() - header_size
                f.seek(header_size)
                compressed_bytes = f.read(compressed_size)

                # 计算解压后大小
                virtual_size = header.flags.GetVirtualSize()
                physical_size = header.flags.GetPhysicalSize()
                uncompressed_size = virtual_size + physical_size
                
                # 使用zlib解压
                uncompressed_data_bytes = zlib.decompress(compressed_bytes)
                
                # 检查解压后大小
                if len(uncompressed_data_bytes) != uncompressed_size:
                    print(f"警告: {input_file.name} 的解压大小不匹配。预期 {uncompressed_size}，实际 {len(uncompressed_data_bytes)}", file=sys.stderr)
                    # 调整缓冲区大小以匹配实际数据
                    if len(uncompressed_data_bytes) < uncompressed_size:
                        # 如果解压数据小于预期，填充零
                        uncompressed_data_bytes += b'\x00' * (uncompressed_size - len(uncompressed_data_bytes))
                    else:
                        # 如果解压数据大于预期，截断
                        uncompressed_data_bytes = uncompressed_data_bytes[:uncompressed_size]
                
                writable_buffer = bytearray(uncompressed_data_bytes)
                
                return memoryview(writable_buffer)

        except zlib.error as e:
            print(f"{input_file} 的 Zlib 解压错误: {e}", file=sys.stderr)
        except IOError as e:
            print(f"{input_file} 的文件错误: {e}", file=sys.stderr)
        
        return None

    def ExtractNodeStrings(self, node, block: memoryview,
                             container: List[ExportedTextEntry], hashes: Set[int]):
        
        if node is None:
            return

        # 递归处理子节点
        for element_ptr in node.m_children.get_span(block, pgPtr):
            child_node = element_ptr.locate(block, CHtmlNode)
            if child_node:
                self.ExtractNodeStrings(child_node, block, container, hashes)

        # 根据节点类型处理
        try:
            node_type = HtmlNodeType(node.m_eNodeType)
        except ValueError:
            print(f"警告: 未知节点类型 {node.m_eNodeType}", file=sys.stderr)
            return

        if node_type == HtmlNodeType.Node_HtmlDataNode:
            # 处理数据节点
            try:
                data_node_ptr = ctypes.cast(ctypes.byref(node), ctypes.POINTER(CHtmlDataNode))
                data_node = data_node_ptr.contents
                
                data_bytes = data_node.m_pData.locate(block, c_char)
                
                if data_bytes:
                    self.TryAppendString(container, data_bytes, hashes)
            except Exception as e:
                print(f"转换为 CHtmlDataNode 时出错: {e}", file=sys.stderr)
        
        elif node_type in (HtmlNodeType.Node_HtmlNode, 
                           HtmlNodeType.Node_HtmlTableNode, 
                           HtmlNodeType.Node_HtmlTableElementNode):
            # 这些节点类型不包含文本数据，跳过
            pass
        else:
            print(f"警告: 未处理的节点类型 {node_type}", file=sys.stderr)

    def TryAppendString(self, container: List[ExportedTextEntry], ptr: bytes,
                          hashes: Set[int]):
        
        def validate_digit_char(c):
            return b'0'[0] <= c <= b'9'[0]

        def validate_english_char(c):
            return (b'a'[0] <= c <= b'z'[0]) or (b'A'[0] <= c <= b'Z'[0])

        def validate_efigs_char(c):
            return validate_english_char(c) or c >= 0xC0
        
        def validate_url_char(c):
            return (
                validate_english_char(c) or
                validate_digit_char(c) or
                c == b'.'[0] or c == b'%'[0] or c == b'@'[0] or
                c == b'-'[0] or c == b'_'[0]
            )

        def validate_url(view: bytes) -> bool:
            if not view:
                return False
            
            # 检查所有字符是否都是URL合法字符
            if not all(validate_url_char(c) for c in view):
                return False
            
            first_dot_pos = view.find(b'.')
            last_dot_pos = view.rfind(b'.')
            
            # 验证URL格式：必须包含点号，且不在开头或结尾
            return (
                first_dot_pos != -1 and
                first_dot_pos != 0 and
                last_dot_pos != len(view) - 1
            )

        def validate_string(view: bytes) -> bool:
            # 包含EFIGS字母且不是网址
            return (
                any(validate_efigs_char(c) for c in view) and 
                not validate_url(view)
            )

        if not ptr:
            return

        if validate_string(ptr):
            hash_val = fnv1a_32(ptr)
            if hash_val not in hashes:
                hashes.add(hash_val)
                container.append(ExportedTextEntry(hash=hash_val, str=ptr))

    def ExtractWhmStrings(self, filename: Path, hashes: Set[int]) -> List[ExportedTextEntry]:
        container: List[ExportedTextEntry] = []
        block = self.UnpackWhm(filename)

        if block:
            try:
                p_doc = CHtmlDocument.from_buffer(block)
                body_node = p_doc.m_pBody.locate(block, CHtmlNode)

                if body_node:
                    self.ExtractNodeStrings(body_node, block, container, hashes)
                else:
                    print(f"警告: 无法在 {filename.name} 中找到 body 节点", file=sys.stderr)
            except Exception as e:
                print(f"解析 {filename.name} 的文档结构时出错: {e}", file=sys.stderr)

        return container

    def Windows1252ToUtf8(self, str_bytes: bytes) -> str:
        """将Windows-1252编码的字节转换为UTF-8字符串"""
        try:
            return str_bytes.decode('windows-1252')
        except UnicodeDecodeError:
            # 如果解码失败，使用替换策略
            return str_bytes.decode('windows-1252', errors='replace')

def main():
    exporter = CHtmlTextExport()
    
    if len(sys.argv) < 2:
        print_usage()
        return
    
    if sys.argv[1] == "-gendb":
        if len(sys.argv) != 4:
            print("错误: -gendb 需要两个参数: <txt文件夹> <输出文件>")
            print_usage()
            return
            
        p1 = Path(sys.argv[2])
        p2 = Path(sys.argv[3])
        if not p1.is_dir():
            print(f"错误: 输入路径不是目录: {p1}", file=sys.stderr)
            return
            
        print(f"模式: 生成数据库")
        print(f"输入文件夹 (txt): {p1}")
        print(f"输出文件 (db): {p2}")
        exporter.GenerateDataBase(p1, p2)

    elif sys.argv[1] == "-export":
        if len(sys.argv) != 3:
            print("错误: -export 需要一个参数: <whm文件夹>")
            print_usage()
            return
            
        p1 = Path(sys.argv[2])
        if not p1.is_dir():
            print(f"错误: 输入路径不是目录: {p1}", file=sys.stderr)
            return

        print(f"模式: 导出文本")
        print(f"输入文件夹 (whm): {p1}")
        exporter.ExportHtml(p1)

    elif sys.argv[1] == "-parse":
        if len(sys.argv) != 4:
            print("错误: -parse 需要两个参数: <输入数据库文件> <输出文本文件>")
            print_usage()
            return
            
        p1 = Path(sys.argv[2])
        p2 = Path(sys.argv[3])
        if not p1.is_file():
            print(f"错误: 输入文件不存在: {p1}", file=sys.stderr)
            return

        print(f"模式: 解析数据库")
        print(f"输入文件 (db): {p1}")
        print(f"输出文件 (txt): {p2}")
        exporter.ParseWhmTable(p1, p2)
        
    else:
        print_usage()

def print_usage():
    print("GTA4 WHM 文本提取工具")
    print("使用方法:")
    print("  生成汉化文件: -gendb [txt文件夹] [输出文件]")
    print("  导出文本: -export [whm文件夹]")
    print("  解析数据库: -parse [输入数据库文件] [输出文本文件]")

if __name__ == "__main__":
    main()
