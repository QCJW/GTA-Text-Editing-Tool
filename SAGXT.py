import os
import re
import struct
import sys

# GTA San Andreas JAMCRC 表 (多项式: 0xEDB88320)
CRC32_TABLE = [
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
    0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,
    0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
    0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,
    0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
    0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
    0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
    0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,
    0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,
    0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E, 0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
    0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
    0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB,
    0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,
    0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
    0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD,
    0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A, 0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683,
    0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
    0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7,
    0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,
    0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
    0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79,
    0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236, 0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F,
    0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
    0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
    0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38, 0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21,
    0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,
    0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB,
    0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
    0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF,
    0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94, 0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D
]

def gta_sa_hash(key: str) -> int:
    hash_val = 0xFFFFFFFF
    for char in key:
        # SA 哈希不区分大小写 (强制转为大写)
        c_upper = char.upper()
        c_byte = ord(c_upper) & 0xFF
        
        idx = (hash_val ^ c_byte) & 0xFF
        hash_val = CRC32_TABLE[idx] ^ (hash_val >> 8)
        
    return hash_val # 与标准 CRC32 不同，末尾不进行位反转

class SAGXT:
    SizeOfTABL = 12
    SizeOfTKEY = 8

    def __init__(self):
        self.m_GxtData = dict()  # 表名 -> {哈希值: 文本}
        self.m_WideCharCollection = set()

    def load_text(self, path: str) -> bool:
        # 正则调整为支持纯文本键。
        table_format = re.compile(r"^\[([0-9a-zA-Z_]{1,7})\]\s*$")
        entry_format = re.compile(r"^\s*([^=\s]+)\s*=\s*(.*)\s*$")
        hex_check = re.compile(r"^[0-9A-Fa-f]{1,8}$")

        current_table = None
        self.m_GxtData.clear()
        self.m_WideCharCollection.clear()

        try:
            if not os.path.exists(path):
                print(f"输入文件 {path} 未找到")
                return False

            with open(path, encoding='utf-8') as f:
                raw_content = f.read()
                
            # 如果存在 BOM 则处理
            if raw_content.startswith('\ufeff'):
                raw_content = raw_content[1:]
                
            lines = raw_content.splitlines()

            for line_no, line in enumerate(lines, 1):
                line = line.strip()
                if not line or line.startswith(';'):
                    continue

                table_match = table_format.match(line)
                entry_match = entry_format.match(line)

                if table_match:
                    # 解析表名
                    table_name = table_match.group(1).upper()
                    if table_name not in self.m_GxtData:
                        self.m_GxtData[table_name] = dict()
                    current_table = self.m_GxtData[table_name]
                
                elif entry_match:
                    if current_table is None:
                        # 如果尚未定义表，则回退到 MAIN
                        print(f"警告: 第 {line_no} 行的条目没有表，将分配到 'MAIN'")
                        if 'MAIN' not in self.m_GxtData:
                            self.m_GxtData['MAIN'] = dict()
                        current_table = self.m_GxtData['MAIN']

                    raw_key = entry_match.group(1)
                    text_value = entry_match.group(2)

                    # --- 哈希逻辑 ---
                    # 1. 如果看起来像十六进制且长度 <= 8，则尝试解释为原始十六进制
                    # 注意: SA GXT 通常使用原始十六进制。但 "FACE" 可能是一个单词。
                    # 启发式: 如果是有效的十六进制，则视为 ID。否则，对其进行哈希。
                    hash_key = 0
                    is_hex = hex_check.match(raw_key)
                    
                    if is_hex:
                        try:
                            hash_key = int(raw_key, 16)
                        except ValueError:
                            # 由于正则检查，不应发生，但安全回退
                            hash_key = gta_sa_hash(raw_key)
                    else:
                        # 不是十六进制 (包含 _, G-Z 等)，因此进行哈希
                        hash_key = gta_sa_hash(raw_key)

                    # 重复检查
                    if hash_key in current_table:
                        # 如果值不同，警告用户
                        if current_table[hash_key] != text_value:
                            print(f"警告: 第 {line_no} 行检测到哈希冲突或重复键!")
                            print(f"键: {raw_key} -> 哈希: 0x{hash_key:08X}")
                            print(f"已存在: {current_table[hash_key]}")
                            print(f"新值: {text_value}\n")
                    
                    current_table[hash_key] = text_value
                    
                    # 收集字符用于字体生成
                    for ch in text_value:
                        self.m_WideCharCollection.add(ch)
                else:
                    print(f"无法识别的行 (第 {line_no} 行): {line}")

            return True
        except Exception as e:
            print(f"读取文件出错: {e}")
            return False

    def save_as_gxt(self, path: str):
        try:
            with open(path, 'wb') as f:
                # 头部: 版本 4, 8 位 (SA 标准)
                f.write(struct.pack('<H', 4)) 
                f.write(struct.pack('<H', 8)) 

                f.write(b"TABL")
                table_block_size = len(self.m_GxtData) * self.SizeOfTABL
                f.write(struct.pack('<I', table_block_size))

                fo_table_block = 12
                fo_key_block = 12 + table_block_size
                key_block_offset = fo_key_block

                # MAIN 表根据游戏引擎偏好通常在最前或最后，
                # 但按名称排序是标准结构。
                sorted_tables = sorted(self.m_GxtData.items(), key=self._table_sort)

                for table_name, entries in sorted_tables:
                    key_block_size = len(entries) * self.SizeOfTKEY
                    data_block_size = self._get_data_block_size(entries)

                    # 写入 TABL 条目
                    f.seek(fo_table_block)
                    name_bytes = table_name.encode('ascii', errors='ignore')[:7]
                    f.write(name_bytes.ljust(8, b'\x00'))
                    f.write(struct.pack('<I', key_block_offset))
                    fo_table_block += self.SizeOfTABL

                    # 写入 TKEY 头部
                    f.seek(fo_key_block)
                    if table_name != "MAIN":
                        f.write(name_bytes.ljust(8, b'\x00'))
                    f.write(b"TKEY")
                    f.write(struct.pack('<I', key_block_size))
                    
                    # 移动指针
                    current_tkey_data_pos = f.tell()
                    fo_key_block = current_tkey_data_pos + key_block_size

                    # 写入 TDAT 头部
                    f.seek(fo_key_block)
                    tdat_offset = f.tell()
                    f.write(b"TDAT")
                    f.write(struct.pack('<I', data_block_size))
                    current_tdat_data_pos = f.tell()

                    # 写入条目 (TKEY 和 TDAT)
                    # 按哈希值排序条目 (GXT 二进制搜索的标准要求)
                    for hash_key, value in sorted(entries.items()):
                        # 写入文本数据
                        f.seek(current_tdat_data_pos)
                        # SA 通常使用 UTF-8 或 ANSI。保持原始的 UTF-8 编码逻辑。
                        text_bytes = value.encode('utf-8') + b'\x00'
                        f.write(text_bytes)
                        
                        # 计算相对偏移量
                        data_offset = current_tdat_data_pos - tdat_offset - 8
                        current_tdat_data_pos = f.tell()

                        # 写入键数据
                        f.seek(current_tkey_data_pos)
                        f.write(struct.pack('<II', data_offset, hash_key))
                        current_tkey_data_pos += self.SizeOfTKEY

                    # 更新主偏移量，用于下一个表
                    fo_key_block = current_tdat_data_pos
                    key_block_offset = fo_key_block

            print(f"已生成GXT文件: {path} (表数量: {len(self.m_GxtData)})")
        except Exception as e:
            print(f"写入GXT失败: {e}")

    def generate_qcjw_stuff(self):
        try:
            with open("TABLE.txt", "w", encoding='utf-8') as conv_code, \
                open("CHARACTERS.txt", "wb") as characters_set:

                characters_set.write(b"\xFF\xFE")  # UTF-16LE BOM

                row, column = 0, 0
                for char in sorted(self.m_WideCharCollection):
                    if ord(char) <= 0x7F:
                        continue  # 跳过 ASCII

                    conv_code.write(f"m_Table[0x{ord(char):X}] = {{{row},{column}}};\n")
                    characters_set.write(char.encode('utf-16le'))

                    if column < 63:
                        column += 1
                    else:
                        row += 1
                        characters_set.write('\n'.encode('utf-16le'))
                        column = 0
            print("已生成辅助映射文件 (TABLE.txt, CHARACTERS.txt)")
        except Exception as e:
            print(f"生成辅助映射文件输出失败: {e}")


    def _get_data_block_size(self, table: dict) -> int:
        return sum(len(v.encode('utf-8')) + 1 for v in table.values())

    def _table_sort(self, item):
        # MAIN 表在某些工具中通常在最前面，但按字母顺序是标准
        # 原始逻辑: MAIN，然后按字母顺序
        return (item[0] != 'MAIN', item[0]) 

def main():
    input_txt = "GTASA.txt"
    output_gxt = "wm_sachs.gxt"
    
    # 允许命令行参数
    if len(sys.argv) > 1:
        input_txt = sys.argv[1]
    if len(sys.argv) > 2:
        output_gxt = sys.argv[2]

    sagxt = SAGXT()
    print(f"正在处理: {input_txt} -> {output_gxt}")
    if sagxt.load_text(input_txt):
        sagxt.save_as_gxt(output_gxt)
        sagxt.generate_qcjw_stuff()
        print("构建完成。")
    else:
        print("加载失败。")

if __name__ == "__main__":
    main()