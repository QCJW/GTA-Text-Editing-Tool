import struct
import os
import sys
import mmap
import numpy as np
import io

# -----------------------
# 内部辅助函数
# -----------------------

def _peek(stream, size):
    """尝试读取指定大小的字节而不改变流位置。
    如果流支持peek方法（例如BufferedReader），则优先使用。
    否则通过读取并恢复当前位置（如果流可seek）。
    如果无法seek，流位置会前进。
    """
    if hasattr(stream, 'peek'):
        try:
            data = stream.peek(size)
            return data[:size]
        except Exception:
            pass

    # 回退：如果可能，读取并恢复位置
    try:
        cur = stream.tell()
    except Exception:
        cur = None
    data = stream.read(size)
    if cur is not None:
        try:
            stream.seek(cur, os.SEEK_SET)
        except Exception:
            pass
    return data


def _safe_tell(stream):
    """安全获取流的当前位置，如果不可seek则返回None"""
    try:
        return stream.tell()
    except Exception:
        return None


def _file_size_for(stream):
    """尝试通过多种方式获取文件大小"""
    try:
        if hasattr(stream, 'fileno'):
            return os.fstat(stream.fileno()).st_size
    except Exception:
        pass
    # 尝试使用内存映射包装器
    try:
        if hasattr(stream, '_mmap'):
            return len(stream._mmap)
    except Exception:
        pass
    # 回退使用seek/tell
    try:
        cur = stream.tell()
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        stream.seek(cur, os.SEEK_SET)
        return size
    except Exception:
        return None


def _decode_bytes(raw_bytes):
    """尝试多种编码方式将字节解码为字符串。
    按照社区常见GXT编码顺序尝试解码。
    """
    if not raw_bytes:
        return ''
    # 如果字节长度为偶数且可能是UTF-16数据，优先尝试UTF-16-LE
    try:
        if len(raw_bytes) % 2 == 0:
            s = raw_bytes.decode('utf-16-le', errors='strict')
            # 移除嵌入的空字符
            idx = s.find('\x00')
            if idx != -1:
                s = s[:idx]
            return s
    except Exception:
        pass
    # 回退编码链
    for enc in ('utf-8', 'gbk', 'cp1252', 'latin1'):
        try:
            s = raw_bytes.decode(enc, errors='strict')
            idx = s.find('\x00')
            if idx != -1:
                s = s[:idx]
            return s
        except Exception:
            continue
    # 最后手段：替换错误字符
    return raw_bytes.decode('latin1', errors='replace')


# -----------------------
# 查找块（TABL/TKEY/TDAT）
# -----------------------

def findBlock(stream, block):
    """从当前流位置向前搜索4字节块标签（例如'TABL'、'TKEY'、'TDAT'）。
    适用于原始文件对象和MemoryMappedFile包装器。
    将流定位到8字节块头之后，以便后续stream.read(size)返回块内容。
    返回：
        size (int)：紧随4字节标签后的32位大小字段。
    如果块未找到或头部不完整，抛出ValueError。
    """
    tag = block.encode('ascii')
    window = 4096

    file_size = _file_size_for(stream)

    while True:
        peek = _peek(stream, window)
        if not peek:
            raise ValueError(f"未找到块 '{block}'")
        idx = peek.find(tag)
        if idx != -1:
            # 从当前位置（未改变）前进到标签
            stream.seek(idx, os.SEEK_CUR)
            header = stream.read(8)
            if len(header) < 8:
                raise ValueError(f"块 '{block}' 的头部不完整")
            found_tag, size = struct.unpack('<4sI', header)
            if found_tag != tag:
                raise ValueError(f"找到的标签不匹配（期望 {tag!r}，实际 {found_tag!r}）")
            return size

        # 在当前窗口未找到
        cur = _safe_tell(stream)
        if file_size is not None and cur is not None and cur + window >= file_size:
            raise ValueError(f"未找到块 '{block}'")
        # 前进window-4以避免错过跨边界的标签
        stream.seek(window - 4, os.SEEK_CUR)


# -----------------------
# 版本识别
# -----------------------

def getVersion(stream):
    """识别GXT文件的版本"""
    hdr = _peek(stream, 8)
    if len(hdr) < 4:
        return None
    # 尝试小端序解析
    word1, word2 = struct.unpack('<HH', hdr[:4])
    # IV版本以(version=4, bits_per_char=16)开头
    if word1 == 4 and word2 == 16:
        return 'IV'
    # SA变体：头部以version=4开始，后跟bits-per-char和'TABL'
    if word1 == 4 and hdr[4:8] == b'TABL':
        if word2 == 8:
            return 'SA'
    # VC版本直接以'TABL'开头
    if hdr[:4] == b'TABL':
        return 'VC'
    # III版本以'TKEY'开头
    if hdr[:4] == b'TKEY':
        return 'III'
    return None


def getReader(version):
    """根据版本返回对应的解析器"""
    if version == 'VC':
        return VC()
    if version == 'SA':
        return SA()
    if version == 'III':
        return III()
    if version == 'IV':
        return IV()
    return None


# -----------------------
# 解析表
# -----------------------

def _parseTables(stream):
    """解析TABL块，返回表名和偏移量列表"""
    size = findBlock(stream, 'TABL')
    entry_count = int(size // 12)
    Tables = []
    for _ in range(entry_count):
        raw = stream.read(12)
        if len(raw) < 12:
            raise ValueError('TABL条目不完整')
        rawName, offset = struct.unpack('<8sI', raw)
        Tables.append((rawName.split(b'\x00')[0].decode(errors='ignore'), offset))
    return Tables


# -----------------------
# 各版本解析类
# -----------------------
class III:
    def hasTables(self):
        """检查是否支持表"""
        return False

    def parseTables(self, stream):
        """解析表（III版本无表）"""
        return []

    def parseTKeyTDat(self, stream):
        """解析TKEY和TDAT块"""
        size = findBlock(stream, 'TKEY')
        entry_count = size // 12
        tkey_data = stream.read(size)
        if len(tkey_data) != size:
            raise ValueError('TKEY块不完整')
        # 显式小端序
        tkey_np = np.frombuffer(tkey_data, dtype=[('offset', '<u4'), ('key', 'S8')])
        offsets = tkey_np['offset']
        keys = [sys.intern(k.split(b'\x00')[0].decode(errors='ignore')) for k in tkey_np['key']]

        datSize = findBlock(stream, 'TDAT')
        TDat = stream.read(datSize)
        arr = np.frombuffer(TDat, dtype=np.uint16)
        zero_idx = np.where(arr == 0)[0]
        starts = offsets // 2
        # 安全处理结束位置
        ends_idx = np.searchsorted(zero_idx, starts, side='left')
        ends = np.empty_like(ends_idx)
        mask = ends_idx < zero_idx.size
        ends[mask] = zero_idx[ends_idx[mask]]
        ends[~mask] = len(arr)
        values = []
        for i in range(entry_count):
            raw = arr[starts[i]:ends[i]].tobytes()
            v = _decode_bytes(raw)
            values.append(sys.intern(v))
        return list(zip(keys, values))


class VC:
    def hasTables(self):
        """检查是否支持表"""
        return True

    def parseTables(self, stream):
        """解析表"""
        return _parseTables(stream)

    def parseTKeyTDat(self, stream):
        """解析TKEY和TDAT块"""
        size = findBlock(stream, 'TKEY')
        entry_count = size // 12
        tkey_data = stream.read(size)
        if len(tkey_data) != size:
            raise ValueError('TKEY块不完整')
        tkey_np = np.frombuffer(tkey_data, dtype=[('offset', '<u4'), ('key', 'S8')])
        offsets = tkey_np['offset']
        keys = [sys.intern(k.split(b'\x00')[0].decode(errors='ignore')) for k in tkey_np['key']]

        datSize = findBlock(stream, 'TDAT')
        TDat = stream.read(datSize)
        arr = np.frombuffer(TDat, dtype=np.uint16)
        zero_idx = np.where(arr == 0)[0]
        starts = offsets // 2
        ends_idx = np.searchsorted(zero_idx, starts, side='left')
        ends = np.empty_like(ends_idx)
        mask = ends_idx < zero_idx.size
        ends[mask] = zero_idx[ends_idx[mask]]
        ends[~mask] = len(arr)
        values = []
        for i in range(entry_count):
            raw = arr[starts[i]:ends[i]].tobytes()
            v = _decode_bytes(raw)
            values.append(sys.intern(v))
        return list(zip(keys, values))


class SA:
    def hasTables(self):
        """检查是否支持表"""
        return True

    def parseTables(self, stream):
        """解析表"""
        return _parseTables(stream)

    def parseTKeyTDat(self, stream):
        """解析TKEY和TDAT块"""
        size = findBlock(stream, 'TKEY')
        entry_count = size // 8
        tkey_bytes = stream.read(size)
        if len(tkey_bytes) != size:
            raise ValueError('TKEY块不完整')
        tkey_np = np.frombuffer(tkey_bytes, dtype='<u4').reshape(-1, 2)
        offsets = tkey_np[:, 0].astype(np.int64)
        crcs = tkey_np[:, 1]

        datSize = findBlock(stream, 'TDAT')
        TDat = stream.read(datSize)
        arr = np.frombuffer(TDat, dtype=np.uint8)
        zero_idx = np.where(arr == 0)[0]
        starts = offsets
        ends_idx = np.searchsorted(zero_idx, starts, side='left')
        ends = np.empty_like(ends_idx)
        mask = ends_idx < zero_idx.size
        ends[mask] = zero_idx[ends_idx[mask]]
        ends[~mask] = len(arr)
        mv = memoryview(TDat)
        values = []
        for i in range(entry_count):
            start = int(starts[i])
            end = int(ends[i])
            raw = mv[start:end]
            # 按顺序尝试解码
            try:
                v = raw.tobytes().decode('utf-8', errors='strict')
            except Exception:
                try:
                    v = raw.tobytes().decode('gbk', errors='replace')
                except Exception:
                    v = raw.tobytes().decode('cp1252', errors='replace')
            idx = v.find('\x00')
            if idx != -1:
                v = v[:idx]
            values.append(sys.intern(v))
        keys = [f"{crc:08X}" for crc in crcs]
        return list(zip(keys, values))


class IV:
    def hasTables(self):
        """检查是否支持表"""
        return True

    def parseTables(self, stream):
        """解析表，如果没有TABL块则回退到MAIN表"""
        peek = _peek(stream, 16)
        if b'TABL' not in peek:
            return [("MAIN", 0)]
        return _parseTables(stream)

    def parseTKeyTDat(self, stream):
        """解析TKEY和TDAT块"""
        size = findBlock(stream, 'TKEY')
        tkey_bytes = stream.read(size)
        if len(tkey_bytes) != size:
            raise ValueError('TKEY块不完整')
        entry_count = size // 8
        tkey_np = np.frombuffer(tkey_bytes, dtype='<u4').reshape(-1, 2)
        offsets = tkey_np[:, 0].astype(np.int64)
        crcs = tkey_np[:, 1]

        datSize = findBlock(stream, 'TDAT')
        TDat = stream.read(datSize)
        arr = np.frombuffer(TDat, dtype=np.uint16)
        zero_idx = np.where(arr == 0)[0]

        values = []
        for i in range(entry_count):
            start = int(offsets[i]) // 2
            end_idx = np.searchsorted(zero_idx, start, side='left')
            end = zero_idx[end_idx] if end_idx < len(zero_idx) else len(arr)

            u16_list = arr[start:end].tolist()
            # 确保终止
            if not u16_list or u16_list[-1] != 0:
                u16_list.append(0)

            if u16_list and u16_list[-1] == 0:
                u16_list = u16_list[:-1]

            try:
                v = struct.pack('<' + 'H' * len(u16_list), *u16_list).decode('utf-16-le', errors='ignore')
            except Exception:
                v = _decode_bytes(struct.pack('<' + 'H' * len(u16_list), *u16_list))

            values.append(v)

        keys = [f"0x{crc:08X}" for crc in crcs]
        return list(zip(keys, values))

# -----------------------
# 通用解析器（保留可重用的逻辑）
# -----------------------

def parseTKeyTDat_common(stream, entry_size, key_format, value_encoding):
    """通用解析TKEY和TDAT块"""
    size = findBlock(stream, 'TKEY')
    entry_count = int(size / entry_size)
    key_struct = struct.Struct('<' + key_format)
    tkey_data = stream.read(size)
    if len(tkey_data) != size:
        raise ValueError('TKEY块不完整')
    TKey = [key_struct.unpack_from(tkey_data, i * entry_size) for i in range(entry_count)]
    datSize = findBlock(stream, 'TDAT')
    TDat = stream.read(datSize)
    mv = memoryview(TDat)
    Entries = []
    append_entry = Entries.append
    tdat_len = len(TDat)

    if key_format == 'I8s':
        key_decode = lambda b: b.split(b'\x00')[0].decode(errors='ignore')
        offsets = [entry[0] for entry in TKey]
        offsets.append(tdat_len)
        for i, entry in enumerate(TKey):
            offset = int(entry[0])
            key = key_decode(entry[1])
            if offset >= tdat_len:
                value = ""
            else:
                next_offset = offsets[i + 1]
                end = next_offset
                raw = mv[offset:end]
                try:
                    value = raw.tobytes().decode(value_encoding, errors='ignore')
                    idx = value.find('\x00')
                    if idx != -1:
                        value = value[:idx]
                except UnicodeDecodeError:
                    value = raw.tobytes().decode('cp1252', errors='ignore')
                    idx = value.find('\x00')
                    if idx != -1:
                        value = value[:idx]
            append_entry((key, value))
    else:
        offsets = [int(entry[0]) for entry in TKey]
        offsets.append(tdat_len)
        for i, entry in enumerate(TKey):
            offset = int(entry[0])
            key = f'{entry[1]:08X}'
            if offset >= tdat_len:
                value = ""
            else:
                next_offset = offsets[i + 1]
                end = next_offset
                raw = mv[offset:end]
                try:
                    value = raw.tobytes().decode(value_encoding, errors='ignore')
                    idx = value.find('\x00')
                    if idx != -1:
                        value = value[:idx]
                except UnicodeDecodeError:
                    value = raw.tobytes().decode('cp1252', errors='ignore')
                    idx = value.find('\x00')
                    if idx != -1:
                        value = value[:idx]
            append_entry((key, value))
    return Entries


# -----------------------
# 内存映射文件包装器
# -----------------------
class MemoryMappedFile:
    def __init__(self, filename):
        """初始化内存映射文件"""
        self._file = open(filename, 'rb')
        self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self._pos = 0

    def read(self, size=None):
        """从当前位置读取指定大小的数据"""
        if size is None or size < 0:
            size = len(self._mmap) - self._pos
        end = min(self._pos + size, len(self._mmap))
        data = self._mmap[self._pos:end]
        self._pos = end
        return data

    def seek(self, offset, whence=os.SEEK_SET):
        """设置文件指针位置"""
        if whence == os.SEEK_SET:
            self._pos = max(0, int(offset))
        elif whence == os.SEEK_CUR:
            self._pos = max(0, int(self._pos + offset))
        elif whence == os.SEEK_END:
            self._pos = max(0, int(len(self._mmap) + offset))
        # 限制位置
        self._pos = min(len(self._mmap), self._pos)

    def peek(self, size):
        """预览指定大小的数据而不移动指针"""
        end = min(self._pos + size, len(self._mmap))
        return self._mmap[self._pos:end]

    def tell(self):
        """返回当前文件指针位置"""
        return self._pos

    def close(self):
        """关闭文件和内存映射"""
        try:
            self._mmap.close()
        except Exception:
            pass
        try:
            self._file.close()
        except Exception:
            pass

    # 上下文管理器
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


# -----------------------
# 如果作为脚本运行 - 简单测试/说明
# -----------------------
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='快速转储GXT文件')
    parser.add_argument('gxt', help='GXT文件路径')
    args = parser.parse_args()
    with MemoryMappedFile(args.gxt) as mm:
        ver = getVersion(mm)
        print('检测到的版本:', ver)
        reader = getReader(ver)
        if reader is None:
            print('不支持或未知的GXT格式')
            sys.exit(1)
        if reader.hasTables():
            print('正在解析表...')
            tables = reader.parseTables(mm)
            print('表:', tables)
        mm.seek(0)
        entries = reader.parseTKeyTDat(mm)
        print('条目数量:', len(entries))
