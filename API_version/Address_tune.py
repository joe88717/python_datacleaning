import re
import csv
import json
import pyodbc
import sys
from dotenv import load_dotenv
import os


# 載入 .env 檔案
load_dotenv()

# 從 .env 檔案中讀取資訊
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
SYSTEM_ID = os.getenv("SYSTEM_ID")
DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def connect_to_sql_server(server, database, username, password):
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    connection = pyodbc.connect(connection_string)
    return connection

def read_data(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT SNO, ADDR FROM NCRM_STAGE_CIF_ADDR_API WHERE ADDR <> '' AND ADDR_PYTHON = '' ORDER BY CAST(SNO AS INT)")
    data = cursor.fetchall()
    return data

def update_data(connection, updates):
    cursor = connection.cursor()
    for sno, addr_python in updates:
        cursor.execute("UPDATE NCRM_STAGE_CIF_ADDR_API SET ADDR_PYTHON = ? WHERE SNO = ?", (addr_python, sno)) 
    connection.commit()

# --- 中文數字轉換相關 (chinese_to_arabic 保持) ---
# 阿拉伯數字轉中文（num_to_chinese 不再需要用於 "段"，但可能用於其他地方，暫時保留）
def num_to_chinese(num_str):
    try:
        num = int(num_str)
    except ValueError:
        return num_str
    if not 0 <= num <= 99:
        return num_str
    digit_map = {
        '0': '零', '1': '一', '2': '二', '3': '三', '4': '四',
        '5': '五', '6': '六', '7': '七', '8': '八', '9': '九'
    }
    unit_map = {10: '十'}
    if num < 10:
        return digit_map[str(num)]
    if num == 10:
        return unit_map[10]
    if num < 20:
        return unit_map[10] + digit_map[str(num % 10)]
    result = digit_map[str(num // 10)] + unit_map[10]
    if num % 10 != 0:
        result += digit_map[str(num % 10)]
    return result

# 中文數字轉阿拉伯數字（保持不變）
chinese_digits = {'零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
chinese_units = {'十': 10, '百': 100}

def chinese_to_arabic(text):
    text = text.strip()
    if not text:
        return ''
    try:
        return str(int(text))
    except ValueError:
        pass
    total, current_num, current_unit, temp_val = 0, 0, 1, 0
    if text.startswith('十'):
        if len(text) == 1:
            return '10'
        digit = chinese_digits.get(text[1])
        return str(10 + digit) if digit is not None else text
    for char in text:
        digit = chinese_digits.get(char)
        unit = chinese_units.get(char)
        if digit is not None:
            current_num, temp_val = digit, digit
        elif unit is not None:
            multiplier = current_num if current_num > 0 else (1 if temp_val == 0 and total == 0 else 0)
            total += multiplier * unit
            current_num, temp_val, current_unit = 0, 0, unit
        else:
            return text
    total += current_num
    if total == 0 and text != '零':
        single_digit = chinese_digits.get(text)
        return str(single_digit) if single_digit is not None else text
    return str(total)


def load_zipcode_index(json_file_path):
    """載入郵遞區號JSON檔案並建立優化的查詢索引"""
    with open(json_file_path, 'r', encoding='utf-8') as file:
        raw_data = json.load(file)
    location_index = {}
    for location, zipcode in raw_data.items():
        location_index[location] = zipcode
    return location_index

def add_zipcode_to_address(address, city_district_index):
    """將郵遞區號添加到地址前面的核心功能，使用預建索引"""
    for location, zipcode in city_district_index.items():
        if location in address:
            return f"{zipcode}{address}"
     

    print(f"找不到郵遞區號對應：{address}")
    return address  # 或 return None
   

# --- 主要轉換函數 ---
def convert_address(address):
    """
    標準化地址格式：
    1. 強制使用查找表得到的郵遞區號。
    2. 保持「段」的數字為阿拉伯數字。
    3. 進行其他格式轉換 (全形、F樓、之、樓號中轉阿)。
    4. 移除地址中的XX鄰格式。
    """
    if not isinstance(address, str):
        return "輸入格式錯誤"

    address_content = address.strip().replace('台', '臺')

    # 移除地址開頭的郵遞區號
    address_content = re.sub(r'^\d{3}(\d{2})?\s*', '', address_content).strip()

    # 加入郵遞區號
    zipcode_indices = load_zipcode_index("郵遞區號.json")
    address_content = add_zipcode_to_address(address_content,zipcode_indices)

    # 全形轉半形
    full_width_chars = '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－～（）　，．'
    half_width_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz--() ,.'
    address_content = address_content.translate(str.maketrans(full_width_chars, half_width_chars))

    # F/f 換成 樓
    address_content = re.sub(r'[Ff]', '樓', address_content)

    # 特殊符號統一為「之」
    address_content = re.sub(r'[-~]', '之', address_content)

    # 移除 XX鄰
    address_content = re.sub(r'\d{1,2}鄰', '', address_content)

    # 中文數字轉阿拉伯數字
    address_content = re.sub(r'([零一二三四五六七八九十百]+?)段',
                             lambda m: chinese_to_arabic(m.group(1)) + '段', address_content)
    address_content = re.sub(r'([零一二三四五六七八九十百]+?)樓',
                             lambda m: chinese_to_arabic(m.group(1)) + '樓', address_content)
    address_content = re.sub(r'([零一二三四五六七八九十百]+?)號',
                             lambda m: chinese_to_arabic(m.group(1)) + '號', address_content)
    address_content = re.sub(r'之([零一二三四五六七八九十百]+?)(?![樓號])',
                             lambda m: '之' + chinese_to_arabic(m.group(1)), address_content)

    return address_content


def main():
    # 檢查是否提供參數
    if len(sys.argv) == 2:
        total_records = int(sys.argv[1])  # 如果提供參數，使用指定的筆數
    else:
        total_records = None  # 如果未提供參數，處理所有資料

    connection = connect_to_sql_server(DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD) 
    data = read_data(connection)

    batch_size = 1000  # 調整批次大小為1000筆
    processed_records = 0

    for i in range(0, len(data), batch_size):
        if total_records is not None and processed_records >= total_records:
            break
        batch = data[i:i + batch_size]
        print(f" 正在處理批次 {i // batch_size + 1}，共 {len(batch)} 筆資料")

        
        
        new_addrs = []
        for sno, addr in batch:
            if addr is not None:
                converted = convert_address(addr)
                addr_python = converted if converted and converted.strip() else addr
                if addr_python is not None:
                    new_addrs.append((sno, addr_python))

        updates = [(sno, addr_python) for sno, addr_python in new_addrs]
            
        
        #  確保每一批都執行更新
        
        if updates:
            update_data(connection, updates)
            processed_records += len(updates)
            max_sno = max(sno for sno, _ in updates)
           
        current_record = i + len(batch)
        print(f"已處理到第 {current_record} 筆資料（批次 {i // batch_size + 1}）")


    connection.close()
    print(f"已處理完 {processed_records} 筆資料")

if __name__ == "__main__":
    main()
