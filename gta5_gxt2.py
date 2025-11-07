import struct
import os
import re
from typing import Dict

# ==============================
#   JOAAT 哈希（用于GXT2键）
# ==============================
def joaat(key: str) -> int:
    key = key.encode("utf-8")
    hash_val = 0
    for b in key:
        hash_val += b
        hash_val &= 0xFFFFFFFF
        hash_val += (hash_val << 10)
        hash_val &= 0xFFFFFFFF
        hash_val ^= (hash_val >> 6)
    hash_val += (hash_val << 3)
    hash_val &= 0xFFFFFFFF
    hash_val ^= (hash_val >> 11)
    hash_val += (hash_val << 15)
    hash_val &= 0xFFFFFFFF
    return hash_val


# ==============================
#   GXT2 解析函数
# ==============================
def parse_gxt2(path: str) -> Dict[int, str]:
    with open(path, "rb") as f:
        data = f.read()

    if len(data) < 8 or data[:4] != b"2TXG":
        raise ValueError("不是有效的GXT2文件")

    num_entries = struct.unpack("<I", data[4:8])[0]
    pos = 8
    entries = {}

    for _ in range(num_entries):
        h, offset = struct.unpack("<II", data[pos:pos + 8])
        pos += 8
        entries[h] = offset

    # 第二头部 "2TXG"
    if data[pos:pos + 4] != b"2TXG":
        raise ValueError("文件结构异常：未找到第二个 '2TXG'")
    pos += 8  # 跳过 '2TXG' + uint32

    strings = {}
    for h, offset in entries.items():
        end = offset
        s_bytes = bytearray()
        while end < len(data):
            c = data[end]
            end += 1
            if c == 0:
                break
            s_bytes.append(c)

        try:
            text = s_bytes.decode("utf-8")
        except UnicodeDecodeError:
            text = s_bytes.decode("cp1252", errors="replace")
        strings[h] = text

    return strings


# ==============================
#   保存为 GXT2 格式（修复版）
# ==============================
def save_gxt2(strings: Dict[int, str], path: str,
              align_strings: int = 4,
              end_offset_is_size_minus_one: bool = False,
              encoding: str = "utf-8"):

    if not strings:
        raise ValueError("没有可保存的条目")

    sorted_hashes = sorted(strings.keys())
    num = len(sorted_hashes)

    # Header + entry table (offset 先填0)
    header1 = b"2TXG" + struct.pack("<I", num)
    entry_table = bytearray()
    for h in sorted_hashes:
        entry_table += struct.pack("<II", h, 0)

    # 构建字符串区
    string_data = bytearray()
    rel_offset_map = {}
    current_rel = 0
    for h in sorted_hashes:
        s = strings[h].encode(encoding) + b"\x00"
        rel_offset_map[h] = current_rel
        string_data.extend(s)
        current_rel += len(s)

    # 计算对齐 & 偏移
    second_header_len = 8
    pre_string_len = len(header1) + len(entry_table) + second_header_len

    padding = 0
    if align_strings and align_strings > 1:
        rem = pre_string_len % align_strings
        if rem != 0:
            padding = align_strings - rem

    string_start = pre_string_len + padding
    total_size = string_start + len(string_data)
    second_end_value = total_size - 1 if end_offset_is_size_minus_one else total_size

    second_header = b"2TXG" + struct.pack("<I", second_end_value)

    # 填入最终偏移
    final_entry_table = bytearray()
    for h in sorted_hashes:
        abs_offset = string_start + rel_offset_map[h]
        final_entry_table += struct.pack("<II", h, abs_offset)

    # 写入文件
    with open(path, "wb") as f:
        f.write(header1)
        f.write(final_entry_table)
        f.write(second_header)
        if padding:
            f.write(b"\x00" * padding)
        f.write(string_data)

    print(f"[GXT2] 已生成文件: {os.path.basename(path)}")
    print(f" - 条目数量: {num}")
    print(f" - 文件长度: {total_size} bytes")
    print(f" - 对齐填充: {padding} bytes")
    print(f" - end_offset = {second_end_value} ({'size-1' if end_offset_is_size_minus_one else 'size'})")
    print(f" - 编码方式: {encoding}")
    print(f" - 写入完成 ✅")


# ==============================
#   从TXT导入与导出
# ==============================
def parse_txt(path: str) -> Dict[int, str]:
    # 移除了错误的正则表达式
    # pattern = re.compile(r"^([0-9A-Fa-fx]+)\s*=\s*(.*)$")
    result = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            
            # 使用更健壮的检查方式
            if not line or line.startswith("#") or '=' not in line:
                continue

            # 使用 .split() 替换 regex
            try:
                key_raw, value = line.split('=', 1)
                key = key_raw.strip()
                value = value.strip()
                
                if not key:
                    continue
            except ValueError:
                continue

            # 现在这个逻辑块将按预期工作
            if key.lower().startswith("0x"):
                try:
                    h = int(key, 16)
                except ValueError:
                    print(f"[GXT5 TXT 导入警告] 跳过无效的十六进制键: {key}")
                    continue
            elif key.isdigit():
                h = int(key)
            else:
                # 明文键名（如 CELL_EMAIL_BCON）将在这里被正确哈希
                h = joaat(key)
                
            result[h] = value
    return result


def export_txt(strings: Dict[int, str], path: str):
    with open(path, "w", encoding="utf-8") as f:
        for h, s in strings.items():
            f.write(f"0x{h:08X}={s}\n")
    print(f"[TXT] 已导出到 {path}")


# ==============================
#   主程序入口
# ==============================
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("用法：")
        print("  解析GXT2: python gta5_gxt2.py input.gxt2 output.txt")
        print("  生成GXT2: python gta5_gxt2.py input.txt output.gxt2")
        sys.exit(0)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    if input_path.lower().endswith(".gxt2"):
        data = parse_gxt2(input_path)
        export_txt(data, output_path)
    elif input_path.lower().endswith(".txt"):
        strings = parse_txt(input_path)
        save_gxt2(strings, output_path,
                  align_strings=4,
                  end_offset_is_size_minus_one=False,
                  encoding="utf-8")
    else:
        print("输入文件必须是 .gxt2 或 .txt")