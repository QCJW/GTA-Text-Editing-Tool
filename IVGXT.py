import re
import struct
import sys
from pathlib import Path

# ---------- 配置 ----------
INPUT_TXT = Path('GTA4.txt')
OUTPUT_GXT = Path('chinese.gxt')

# ---------- 文本格式 ----------
TABLE_RE = re.compile(r'^\[([0-9a-zA-Z_]{1,7})\]\s*$')
ENTRY_RE = re.compile(r'^\s*((?:0[xX][0-9A-Fa-f]{8}|[A-Za-z0-9_]+))\s*=\s*(.*)\s*$')

# ---------- GTA4 GXT 哈希 ----------
def gta4_gxt_hash(key: str) -> int:
    ret_hash = 0
    for c in key:
        if 'A' <= c <= 'Z':
            c = chr(ord(c) + 32)
        elif c == '\\':
            c = '/'
        c_val = ord(c) & 0xFF
        tmp = (ret_hash + c_val) & 0xFFFFFFFF
        mult = (1025 * tmp) & 0xFFFFFFFF
        ret_hash = ((mult >> 6) ^ mult) & 0xFFFFFFFF

    a = (9 * ret_hash) & 0xFFFFFFFF
    a_x = (a ^ (a >> 11)) & 0xFFFFFFFF
    ret_hash = (32769 * a_x) & 0xFFFFFFFF
    return ret_hash

# ---------- 帮助函数 ----------
def name_to_8_bytes(name: str) -> bytes:
    b = name.encode('utf-8')[:8]
    return b + b'\x00' * (8 - len(b))

def u8_to_u16_list(u8_string: str):
    if not u8_string:
        return [0]
    utf16le = u8_string.encode('utf-16-le')
    u16 = list(struct.unpack('<' + 'H' * (len(utf16le) // 2), utf16le))
    if not u16 or u16[-1] != 0:
        u16.append(0)
    return u16

def warn(msg):
    print("警告:", msg)

def load_txt(filepath: Path, special_chars=None, validate_callback=None):
    if special_chars is None:
        special_chars = set()

    m_Data = {}
    invalid_keys = []
    current_table = None

    raw = filepath.read_bytes()
    if raw.startswith(b'\xEF\xBB\xBF'):
        raw = raw[3:]
    text = raw.decode('utf-8', errors='replace')
    lines = text.splitlines()

    for line_no, raw_line in enumerate(lines, 1):
        line = raw_line.strip()
        if not line:
            continue

        for char in raw_line:
            if ord(char) > 255:
                special_chars.add(char)

        m_tab = TABLE_RE.match(line)
        if m_tab:
            current_table = m_tab.group(1)
            if current_table not in m_Data:
                m_Data[current_table] = []
            continue

        if raw_line.lstrip().startswith(';'):
            continue

        m_entry = ENTRY_RE.match(line)
        if m_entry:
            key_left = m_entry.group(1).strip()
            b_string = m_entry.group(2)

            if validate_callback:
                is_valid, msg = validate_callback(key_left, 'IV')
                if not is_valid:
                    invalid_keys.append((key_left, line_no, msg))
                    continue

            if current_table is None:
                warn(f"{filepath}: 第 {line_no} 行条目没有所属表; 将分配到 'MAIN'")
                current_table = 'MAIN'
                if current_table not in m_Data:
                    m_Data[current_table] = []

            try:
                if key_left.lower().startswith('0x'):
                    int(key_left, 16)
                    hash_str = key_left
                else:
                    raise ValueError
            except Exception:
                h = gta4_gxt_hash(key_left)
                hash_str = f'0x{h:08X}'

            if current_table not in m_Data:
                m_Data[current_table] = []

            m_Data[current_table].append({'hash_string': hash_str, 'text': b_string})
        else:
            warn(f"{filepath}: 第 {line_no} 行无法识别。")

    if 'MAIN' not in m_Data:
        m_Data['MAIN'] = []

    return m_Data, invalid_keys, special_chars

# ---------- 写 GXT ----------
def generate_binary(m_Data, output_path: Path):
    table_names = ['MAIN'] + sorted([name for name in m_Data.keys() if name != 'MAIN'])

    with open(output_path, 'wb') as f:
        f.write(struct.pack('<H', 4))
        f.write(struct.pack('<H', 16))

        table_count = len(table_names)
        f.write(b'TABL')
        f.write(struct.pack('<I', table_count * 12))

        table_entries_pos = f.tell()
        f.write(b'\x00' * (table_count * 12))

        table_entries = []
        for idx, table_name in enumerate(table_names):
            table_offset = f.tell()
            table_entries.append((table_name, table_offset))

            entries = m_Data.get(table_name, [])

            key_entries = []
            datas = []

            for entry in entries:
                hash_str = entry.get('hash_string', '')
                try:
                    h_val = int(hash_str, 16) if isinstance(hash_str, str) and hash_str.lower().startswith('0x') else int(hash_str)
                except Exception:
                    h_val = 0
                    warn(f"表 {table_name} 存在无效哈希 '{hash_str}'")

                offset_bytes = len(datas) * 2
                key_entries.append((offset_bytes, h_val))

                text_to_write = entry.get('text', '')
                w_u16 = u8_to_u16_list(text_to_write)
                datas.extend(w_u16)

            if table_name == 'MAIN':
                f.write(b'TKEY')
            else:
                f.write(name_to_8_bytes(table_name))
                f.write(b'TKEY')

            key_block_size = len(key_entries) * struct.calcsize('<II')
            f.write(struct.pack('<I', key_block_size))

            for off_b, hash_v in key_entries:
                f.write(struct.pack('<II', off_b, hash_v))

            data_block_size = len(datas) * 2
            f.write(b'TDAT')
            f.write(struct.pack('<I', data_block_size))

            if datas:
                f.write(struct.pack('<' + 'H' * len(datas), *datas))

            if idx < (len(table_names) - 1):
                pad_len = (4 - (f.tell() % 4)) % 4
                if pad_len:
                    f.write(b'\x00' * pad_len)

        f.seek(table_entries_pos, 0)
        for name, offset in table_entries:
            f.write(name_to_8_bytes(name))
            f.write(struct.pack('<I', offset))

    print(f"已生成GXT文件: {output_path} (表数量: {len(table_names)})")

# ---------- 特殊字符 ----------
def process_special_chars(special_chars):
    special_chars.discard(chr(0x2122))
    special_chars.discard(chr(0x3000))
    special_chars.discard(chr(0xFEFF))

    with open('CHARACTERS.txt', 'w', encoding='utf-8') as f:
        for i, char in enumerate(sorted(special_chars, key=lambda c: ord(c))):
            f.write(char)
            if (i + 1) % 64 == 0:
                f.write('\n')
    print("已生成 'CHARACTERS.txt'")

    with open('char_table.dat', 'wb') as f:
        f.write(len(special_chars).to_bytes(4, 'little'))
        for char in sorted(special_chars, key=lambda c: ord(c)):
            f.write(ord(char).to_bytes(4, 'little'))
    print("已生成 'char_table.dat'")

# ---------- 主流程 ----------
def main():
    input_file = INPUT_TXT
    if len(sys.argv) > 1:
        input_file = Path(sys.argv[1])

    if not input_file.exists():
        print(f"输入文件 {input_file} 未找到")
        return

    m_Data, _, special_chars = load_txt(input_file)
    generate_binary(m_Data, OUTPUT_GXT)
    process_special_chars(special_chars)

if __name__ == '__main__':
    main()
