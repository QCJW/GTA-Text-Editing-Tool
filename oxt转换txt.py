import os
import re

def convert_oxt_to_txt(file_path):
    """
    将单个 oxt 文件转换为 txt 文件。
    源文件格式：带BOM的UTF-8
    目标文件格式：不带BOM的UTF-8
    """
    # 构建输出文件名
    base_name = os.path.splitext(file_path)[0]
    output_path = base_name + ".txt"

    print(f"正在处理: {os.path.basename(file_path)} -> {os.path.basename(output_path)}")

    try:
        # 使用 'utf-8-sig' 编码读取，它会自动处理并移除BOM
        with open(file_path, 'r', encoding='utf-8-sig') as infile:
            lines = infile.readlines()

        # 检查文件是否过短
        if len(lines) < 5:
            print(f"  -> 文件过短，已跳过。")
            return

        # 跳过文件头 (前4行) 并拼接剩余内容
        content_str = "".join(lines[4:])

        # 使用正则表达式查找所有 "TABLE { ... }" 结构
        # \w+ 匹配表名
        # [\s\S]*? 非贪婪匹配花括号内的所有内容（包括换行符）
        pattern = re.compile(r'(\w+)\s*\{\s*([\s\S]*?)\s*\}', re.MULTILINE)
        matches = pattern.findall(content_str)

        if not matches:
            print(f"  -> 未找到有效的表结构，已跳过。")
            return
            
        final_output = []
        # 遍历所有找到的表
        for table_name, table_content in matches:
            # 添加表名
            final_output.append(f"[{table_name}]")
            
            # 处理表内的每一行
            key_value_lines = table_content.strip().split('\n')
            for line in key_value_lines:
                stripped_line = line.strip()
                if '=' in stripped_line:
                    # 分割键和值
                    key, value = stripped_line.split('=', 1)
                    # 格式化并去除多余的空格
                    formatted_line = f"{key.strip()}={value.strip()}"
                    final_output.append(formatted_line)
            
            # 在每个表之后加一个空行，让格式更清晰
            final_output.append('')

        # 使用标准的 'utf-8' 编码写入，默认不带BOM
        with open(output_path, 'w', encoding='utf-8') as outfile:
            outfile.write('\n'.join(final_output))
        
        print(f"  -> 转换成功！")

    except Exception as e:
        print(f"  -> 处理文件时发生错误: {e}")

def main():
    """
    主函数，处理当前目录下的所有 .oxt 文件。
    """
    # 获取脚本所在的目录
    try:
        # __file__ 在某些运行环境（如打包后）可能不存在
        current_directory = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_directory = os.getcwd()
        
    print(f"脚本正在扫描目录: {current_directory}\n")
    
    file_found = False
    # 遍历目录中的所有文件
    for filename in os.listdir(current_directory):
        if filename.lower().endswith(".oxt"):
            file_found = True
            file_path = os.path.join(current_directory, filename)
            convert_oxt_to_txt(file_path)
    
    if not file_found:
        print("未在该目录下找到任何 .oxt 文件。")

if __name__ == "__main__":
    main()
    # 在 Windows 系统上运行时，暂停命令行窗口，以便用户能看到输出信息
    os.system("pause")