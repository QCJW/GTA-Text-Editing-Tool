#尝试使用Python复刻Clans用C++写的GTA4的GXT工具
import sys
import os
import re
import json
import struct
import ctypes
from pathlib import Path
from dataclasses import dataclass
from typing import TextIO, Set, List, Dict, Callable

@dataclass
class TextEntry:
    hash: int = 0
    original: str = ""
    translated: str = ""

class GXTHeader(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("Version", ctypes.c_uint16),
        ("CharBits", ctypes.c_uint16),
    ]

class TableEntry(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("Name", ctypes.c_char * 8),
        ("Offset", ctypes.c_int32),
    ]

class TableBlock(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("TABL", ctypes.c_char * 4),
        ("Size", ctypes.c_int32),
    ]

class KeyEntry(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("Offset", ctypes.c_int32),
        ("Hash", ctypes.c_uint32),
    ]

class KeyBlockMAIN(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("TKEY", ctypes.c_char * 4),
        ("Size", ctypes.c_int32),
    ]

class KeyBlockOthers(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("Name", ctypes.c_char * 8),
        ("Body", KeyBlockMAIN),
    ]

class DataBlock(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("TDAT", ctypes.c_char * 4),
        ("Size", ctypes.c_int32),
    ]

class IVText:
    def __init__(self):
        self.m_data: Dict[str, List[TextEntry]] = {}
        self.table_regex = re.compile(r'\[([0-9a-zA-Z_]{1,7})\]')
        self.entry_regex = re.compile(r'(0[xX][0-9a-fA-F]{8})=(.*)')

    def ProcessT2B(self, in_folder: Path, out_folder: Path):
        out_folder_path = Path(out_folder)
        out_folder_path.mkdir(parents=True, exist_ok=True)
        self.LoadTexts(Path(in_folder))
        self.GenerateBinary(out_folder_path / "chinese.gxt")

    def ProcessJ2B(self, in_folder: Path, out_folder: Path):
        out_folder_path = Path(out_folder)
        out_folder_path.mkdir(parents=True, exist_ok=True)
        self.LoadJsons(Path(in_folder))
        self.GenerateBinary(out_folder_path / "chinese.gxt")

    def ProcessT2J(self, in_folder: Path):
        in_folder_path = Path(in_folder)
        self.LoadTexts(in_folder_path)
        self.GenerateJsons(in_folder_path)

    def ProcessB2T(self, in_file: Path, out_folder: Path):
        out_folder_path = Path(out_folder)
        out_folder_path.mkdir(parents=True, exist_ok=True)
        self.LoadBinary(Path(in_file))
        self.GenerateTexts(out_folder_path)

    def ProcessB2J(self, in_file: Path, out_folder: Path):
        out_folder_path = Path(out_folder)
        out_folder_path.mkdir(parents=True, exist_ok=True)
        self.LoadBinary(Path(in_file))
        self.GenerateJsons(out_folder_path)

    def ProcessCollect(self, in_folder: Path, out_folder: Path):
        chars: Set[int] = set()
        out_folder_path = Path(out_folder)
        out_folder_path.mkdir(parents=True, exist_ok=True)

        self.ProcessTexts(Path(in_folder), True,
                          lambda filename, stream: self.CollectCharsFunc(stream, chars))

        self.GenerateCollection(out_folder_path / "characters.txt", chars)
        self.GenerateTable(out_folder_path / "char_table.dat", chars)

    @staticmethod
    def IsNativeCharacter(character: int) -> bool:
        if character > 0xFFFF:
            print(f"Invalid code point: {character} larger than 0xFFFF.")
            return False
        return (character < 0x100 or character == 0x2122)

    @staticmethod
    def ProcessTexts(in_folder: Path, recursive: bool, func: Callable[[str, TextIO], None]):
        filenames = []
        if recursive:
            filenames = list(in_folder.rglob("*.txt"))
        else:
            filenames = list(in_folder.glob("*.txt"))

        for filename in filenames:
            try:
                with open(filename, 'r', encoding='utf-8-sig') as ifs:
                    func(str(filename), ifs)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                continue

    @staticmethod
    def ProcessJsons(in_folder: Path, recursive: bool, func: Callable[[str, TextIO], None]):
        filenames = []
        if recursive:
            filenames = list(in_folder.rglob("*.json"))
        else:
            filenames = list(in_folder.glob("*.json"))

        for filename in filenames:
            try:
                with open(filename, 'r', encoding='utf-8') as ifs:
                    func(str(filename), ifs)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                continue

    def LoadTextFunc(self, filename: str, stream: TextIO):
        table_iter_name: str = None
        
        for line_no, line in enumerate(stream, 1):
            line = line.strip()

            if not line:
                continue

            table_match = self.table_regex.match(line)
            if table_match:
                table_iter_name = table_match.group(1)
                if table_iter_name not in self.m_data:
                    self.m_data[table_iter_name] = []
                continue

            is_original = line.startswith(';')
            
            entry_line = line[1:] if is_original else line
            match_result = self.entry_regex.match(entry_line)

            if match_result:
                if table_iter_name:
                    table_cont = self.m_data[table_iter_name]
                    hash_val = int(match_result.group(1), 16)
                    b_string = match_result.group(2)

                    if not table_cont or table_cont[-1].hash != hash_val:
                        table_cont.append(TextEntry(hash=hash_val))

                    p_entry = table_cont[-1]

                    if is_original:
                        p_entry.original = b_string
                    else:
                        p_entry.translated = b_string

                    if not is_original and b_string.count('~') % 2 != 0:
                        print(f"{filename}: 第{line_no}行的'~'个数不是偶数!")
                else:
                    print(f"{filename}: 第{line_no}行没有所属的表。")
            else:
                print(f"{filename}: 第{line_no}行无法识别。")

    def LoadJsonFunc(self, filename: str, stream: TextIO):
        try:
            doc = json.load(stream)
        except json.JSONDecodeError:
            print(f"{filename}: json解析失败。")
            return

        if not isinstance(doc, dict):
            print(f"{filename}: json类型错误。")
            return

        for table_name, text_entries_array in doc.items():
            if table_name not in self.m_data:
                self.m_data[table_name] = []
                
            table_iter = self.m_data[table_name]

            for entry in text_entries_array:
                table_iter.append(TextEntry(
                    hash=entry.get("hash", 0),
                    original=entry.get("original", ""),
                    translated=entry.get("translated", "")
                ))

    def CollectCharsFunc(self, stream: TextIO, chars: Set[int]):
        u32_buffer = stream.read()
        for chr_val in u32_buffer:
            codepoint = ord(chr_val)
            if not self.IsNativeCharacter(codepoint) and codepoint != 0x3000 and codepoint != 0xFEFF:
                chars.add(codepoint)

    def LoadTexts(self, in_folder: Path):
        self.ProcessTexts(in_folder, False, self.LoadTextFunc)

    def LoadJsons(self, in_folder: Path):
        self.ProcessJsons(in_folder, False, self.LoadJsonFunc)

    def GenerateBinary(self, output_binary: Path):
        tables: List[TableEntry] = []
        keys: List[KeyEntry] = []
        datas: List[int] = []

        try:
            with open(output_binary, "wb") as file:
                gxt_header = GXTHeader(Version=4, CharBits=16)
                file.write(gxt_header)

                table_block = TableBlock(TABL=b'TABL', Size=len(self.m_data) * ctypes.sizeof(TableEntry))
                file.write(table_block)

                table_entries_start = file.tell()
                file.write(b'\x00' * table_block.Size)
                
                write_position = file.tell()

                sorted_table_names = sorted(self.m_data.keys(), key=lambda k: (k != 'MAIN', k))

                for table_name_str in sorted_table_names:
                    table = self.m_data[table_name_str]
                    table_name_bytes = table_name_str.encode('ascii')
                    
                    tables.append(TableEntry(Name=table_name_bytes, Offset=write_position))

                    keys.clear()
                    datas.clear()

                    key_block_body_size = len(table) * ctypes.sizeof(KeyEntry)

                    for entry in table:
                        if not entry.original or not entry.translated:
                            print(f"遇到缺失的文本项:\nhash: {entry.hash}/0x{entry.hash:08X}\n")

                        if not self.CompareTokens(entry.original, entry.translated):
                            print(f"遇到Token与原文不一致的译文:\nhash: {entry.hash}/0x{entry.hash:08X}\n")

                        key_entry = KeyEntry(
                            Hash=entry.hash,
                            Offset=len(datas) * 2
                        )
                        
                        w_string_to_write = self.U8ToWide(entry.translated)
                        datas.extend(w_string_to_write)
                        keys.append(key_entry)
                    
                    data_block = DataBlock(TDAT=b'TDAT', Size=len(datas) * 2)

                    file.seek(write_position)

                    if table_name_str == "MAIN":
                        key_block_main = KeyBlockMAIN(TKEY=b'TKEY', Size=key_block_body_size)
                        file.write(key_block_main)
                    else:
                        key_block_others = KeyBlockOthers(
                            Name=table_name_bytes,
                            Body=KeyBlockMAIN(TKEY=b'TKEY', Size=key_block_body_size)
                        )
                        file.write(key_block_others)
                    
                    for key in keys:
                        file.write(key)
                    
                    file.write(data_block)
                    if datas:
                        file.write(struct.pack(f'<{len(datas)}H', *datas))

                    write_position = file.tell()

                file.seek(table_entries_start)
                for table_entry in tables:
                    file.write(table_entry)

        except IOError:
            print(f"创建输出文件 {output_binary} 失败。")
            return

    @staticmethod
    def GenerateCollection(out_file: Path, chars: Set[int]):
        u8_text = ""
        sorted_chars = sorted(list(chars))
        
        for count, character in enumerate(sorted_chars):
            if count > 0 and count % 80 == 0:
                u8_text += '\n'
            u8_text += chr(character)

        with open(out_file, 'w', encoding='utf-8') as stream:
            stream.write(u8_text)

    @staticmethod
    def GenerateTable(out_file: Path, chars: Set[int]):
        sorted_chars = sorted(list(chars))
        with open(out_file, "wb") as stream:
            stream.write(struct.pack(f'<{len(sorted_chars)}I', *sorted_chars))

    @classmethod
    def U8ToWide(cls, u8_string: str) -> List[int]:
        utf16_bytes = u8_string.encode('utf-16-le')
        result = list(struct.unpack(f'<{len(utf16_bytes) // 2}H', utf16_bytes))
        result.append(0)
        cls.LiteralToGame(result)
        return result

    @classmethod
    def WideToU8(cls, wide_string: List[int]) -> str:
        if not wide_string:
            return ""
            
        if wide_string[-1] == 0:
            wide_string_no_null = wide_string[:-1]
        else:
            wide_string_no_null = wide_string[:]
        
        if not wide_string_no_null:
            return ""

        utf16_bytes = struct.pack(f'<{len(wide_string_no_null)}H', *wide_string_no_null)
        return utf16_bytes.decode('utf-16-le')

    @staticmethod
    def FixCharacters(wtext: List[int]):
        for i, character in enumerate(wtext):
            if character == 0x85:
                wtext[i] = 0x20
            elif character == 0x92 or character == 0x94:
                wtext[i] = 0x27
            elif character == 0x93:
                pass
            elif character == 0x96:
                wtext[i] = 0x2D
            elif character == 0x97 or character == 0xA0:
                wtext[i] = 0x20

    @staticmethod
    def LiteralToGame(wtext: List[int]):
        for i, character in enumerate(wtext):
            if character == 0x2122:
                wtext[i] = 0x99

    @staticmethod
    def GameToLiteral(wtext: List[int]):
        for i, character in enumerate(wtext):
            if character == 0x99:
                wtext[i] = 0x2122

    @staticmethod
    def CollectTokens(s: str) -> Set[str]:
        return set(re.findall(r'(~.*?~|<.*?>)', s))

    @classmethod
    def CompareTokens(cls, s1: str, s2: str) -> bool:
        token1 = cls.CollectTokens(s1)
        token2 = cls.CollectTokens(s2)
        return token1 == token2

    def LoadBinary(self, in_file: Path):
        self.m_data.clear()

        try:
            with open(in_file, "rb") as file:
                header_data = file.read(ctypes.sizeof(GXTHeader))
                gxt_header = GXTHeader.from_buffer_copy(header_data)

                table_block_data = file.read(ctypes.sizeof(TableBlock))
                table_block = TableBlock.from_buffer_copy(table_block_data)

                tables: List[TableEntry] = []
                num_tables = table_block.Size // ctypes.sizeof(TableEntry)
                for _ in range(num_tables):
                    table_entry_data = file.read(ctypes.sizeof(TableEntry))
                    tables.append(TableEntry.from_buffer_copy(table_entry_data))

                for table in tables:
                    table_name = table.Name.decode('ascii').rstrip('\x00')
                    table_iter = self.m_data.setdefault(table_name, [])
                    
                    file.seek(table.Offset)
                    
                    if table_name == "MAIN":
                        key_block_data = file.read(ctypes.sizeof(KeyBlockMAIN))
                        key_block_body = KeyBlockMAIN.from_buffer_copy(key_block_data)
                    else:
                        key_block_data = file.read(ctypes.sizeof(KeyBlockOthers))
                        key_block_others = KeyBlockOthers.from_buffer_copy(key_block_data)
                        key_block_body = key_block_others.Body

                    keys: List[KeyEntry] = []
                    num_keys = key_block_body.Size // ctypes.sizeof(KeyEntry)
                    for _ in range(num_keys):
                        key_entry_data = file.read(ctypes.sizeof(KeyEntry))
                        keys.append(KeyEntry.from_buffer_copy(key_entry_data))

                    tdat_header_data = file.read(ctypes.sizeof(DataBlock))
                    tdat_header = DataBlock.from_buffer_copy(tdat_header_data)

                    num_datas = tdat_header.Size // 2
                    if num_datas > 0:
                        datas_bytes = file.read(tdat_header.Size)
                        datas = list(struct.unpack(f'<{num_datas}H', datas_bytes))
                    else:
                        datas = []

                    for key in keys:
                        w_string: List[int] = []
                        entry = TextEntry(hash=key.Hash)
                        
                        offset = key.Offset // 2
                        
                        while offset < len(datas) and datas[offset] != 0:
                            w_string.append(datas[offset])
                            offset += 1
                        w_string.append(0)

                        self.FixCharacters(w_string)
                        self.GameToLiteral(w_string)
                        entry.translated = self.WideToU8(w_string)
                        
                        table_iter.append(entry)

        except IOError as e:
            print(f"打开输入文件 {in_file} 失败: {e}")
            return
        except Exception as e:
            print(f"处理文件 {in_file} 时发生错误: {e}")

    def GenerateTexts(self, output_texts: Path):
        for table_name, table in self.m_data.items():
            out_path = output_texts / (table_name + ".txt")
            try:
                with open(out_path, 'w', encoding='utf-8-sig') as stream:
                    stream.write(f"[{table_name}]\n")
                    
                    for entry in table:
                        line = f"0x{entry.hash:08X}={entry.translated}\n"
                        stream.write(f";{line}")
                        stream.write(f"{line}\n")
            except IOError:
                print(f"创建输出文件失败 {out_path}")

    def GenerateJsons(self, output_texts: Path):
        for table_name, table in self.m_data.items():
            out_path = output_texts / (table_name + ".json")
            
            json_data = {table_name: []}
            for entry in table:
                json_data[table_name].append({
                    "hash": entry.hash,
                    "original": entry.original,
                    "translated": entry.translated,
                    "desc": ""
                })

            try:
                with open(out_path, 'w', encoding='utf-8') as stream:
                    json.dump(json_data, stream, ensure_ascii=False, indent=4)
            except IOError:
                print(f"创建输出文件失败 {out_path}")


def print_usage():
    print("使用方法:\n"
          "gxt转txt: GTA4_gxt_Tool.py -b2t [gxt文件] [txt文件夹]\n"
          "gxt转json: GTA4_gxt_Tool.py -b2j [gxt文件] [json文件夹]\n"
          "txt转gxt: GTA4_gxt_Tool.py -t2b [txt文件夹] [gxt文件夹]\n"
          "json转gxt: GTA4_gxt_Tool.py -j2b [json文件夹] [gxt文件夹]\n"
          "txt转json: GTA4_gxt_Tool.py -t2j [txt文件夹]\n"
          "生成字库: GTA4_gxt_Tool.py -collect [(任意文本格式)文件夹] [输出文件夹]\n")

def main():
    instance = IVText()
    error = False
    args = sys.argv
    argc = len(args)

    if argc == 4:
        flag, arg2, arg3 = args[1], args[2], args[3]
        if flag == "-b2t":
            instance.ProcessB2T(arg2, arg3)
        elif flag == "-b2j":
            instance.ProcessB2J(arg2, arg3)
        elif flag == "-t2b":
            instance.ProcessT2B(arg2, arg3)
        elif flag == "-j2b":
            instance.ProcessJ2B(arg2, arg3)
        elif flag == "-collect":
            instance.ProcessCollect(arg2, arg3)
        else:
            error = True
    elif argc == 3:
        flag, arg2 = args[1], args[2]
        if flag == "-t2j":
            instance.ProcessT2J(arg2)
        else:
            error = True
    else:
        error = True

    if error:
        print_usage()

if __name__ == "__main__":
    main()
