import pyodbc
import requests
import json
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
    cursor.execute("SELECT SNO, ADDR FROM NCRM_STAGE_CIF_ADDR_API WHERE ADDR <> '' AND ADDR_GAI = '' ORDER BY 1")
    data = cursor.fetchall()
    return data

def update_data(connection, updates):
    cursor = connection.cursor()
    for sno, addr_gai in updates:
        cursor.execute("UPDATE NCRM_STAGE_CIF_ADDR_API SET ADDR_GAI = ? WHERE SNO = ?", (addr_gai, sno))
        print(f"更新資料：SNO={sno}, 新地址={addr_gai}")
    connection.commit()

def call_api(batch):
    headers = {
        "api-key": API_KEY,  # 從 .env 檔案中讀取
        "systemId": SYSTEM_ID,  # 從 .env 檔案中讀取
        "Content-Type": "application/json"
    }
    messages = [
        {
            "role": "system",
            "content": (
                    "你是一個協助統一地址格式的幫手。請依照以下步驟執行：\n"

                    "一、預處理\n"
                    "將全形文字及全形數字轉成半形。\n"
                    "將「F」轉換為「樓」。\n"
                    "樓層及門牌號碼與段中的數字以半形數字呈現；而縣轄市、鄉/鎮/區、里/村、路/街/大道中的數字以中文呈現。\n"
                    "將「-」、「~」、「之」統一轉成「之」。\n"
                    "移除無關的特殊字元。\n"
                    "當縣市部分出現「台」改為「臺」。\n"
                    "補上缺失的「區」或「縣、市」字樣（但不變更原有內容）。\n"

                    "二、郵遞區號添加\n"
                    "移除地址開頭原有的郵遞區號。\n"
                    "根據地址中的縣市資訊，查找正確的三位數郵遞區號並加在最前面，地址開頭只保留這三位數，與地址內容間無空格。\n"

                    "三、欄位書寫順序\n"
                    "請依照以下格式排列地址各欄位：\n"
                    "[郵遞區號][縣/直轄市][縣轄市][鄉/鎮/區][里/村][鄰][路/街/大道][段][巷][弄][號][樓]\n"

                    "四、輸出\n"
                    "請確保回覆的格式如下：\n"
                    "SNO=數字, ADDR_GAI=轉換後的地址"

                    "【範例】\n"
                    "輸入：SNO=1, ADDR=臺北市大安區復興南路1段279號3樓\n"
                    "輸出：SNO=1, ADDR_GAI=106臺北市大安區復興南路一段279號3樓\n"
                    "輸入：SNO=2, ADDR=新北市新莊區建國2路72號-7\n"
                    "輸出：SNO=2, ADDR_GAI=241新北市新莊區建國二路72號之7\n"

                    "請等待指令，收到地址後直接依照上述規則轉換並回應轉換後的地址。"
            )
        }
    ]
    for sno, addr in batch:
        messages.append({
            "role": "user",
            "content": f"SNO={sno}, 地址={addr}"
        })
    payload = {
        "messages": messages,
        "temperature": 0.0,
        "n": 1,
        "max_tokens": 3000,  
        "presence_penalty": 0.0,
        "frequency_penalty": 0.0
    }

    response = requests.post(API_URL, headers=headers, data=json.dumps(payload))  
    if response.status_code == 200:
        response_json = response.json()
        if response_json.get('success'):
            raw_result = response_json['result']['choices'][0]['message']['content']
            print(f"API 原始回應：{raw_result}")  
            # 解析回應，確保正確對應 SNO 和 ADDR_GAI
            parsed_results = []
            for line in raw_result.split('\n'):
                if line.strip():  
                    if line.startswith('SNO='):  
                        sno, addr_gai = line.split(', ADDR_GAI=', 1)
                        sno = sno.replace('SNO=', '').strip()
                        parsed_results.append((sno, addr_gai.strip()))
            return parsed_results
        else:
            print(f"API回應失敗，returnCode：{response_json.get('returnCode')}")
            return None
    else:
        print(f"API請求失敗，狀態碼：{response.status_code}")
        return None

def main():
    # 檢查是否提供參數
    if len(sys.argv) == 2:
        total_records = int(sys.argv[1])  # 如果提供參數，使用指定的筆數
    else:
        total_records = None  # 如果未提供參數，處理所有資料

    connection = connect_to_sql_server(DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD) 
    data = read_data(connection)

    batch_size = 10  # 調整批次大小為10筆
    processed_records = 0

    for i in range(0, len(data), batch_size):
        if total_records is not None and processed_records >= total_records:
            break
        batch = data[i:i + batch_size]
        print(f"正在處理批次：{i // batch_size + 1}, 共 {len(batch)} 筆資料")
        new_addrs = call_api(batch)  # 將批次資料傳遞給API
        if new_addrs:
            if len(new_addrs) != len(batch):
                print(f"批次 {i // batch_size + 1} 回應數量不匹配，預期 {len(batch)} 筆，實際 {len(new_addrs)} 筆")
            updates = [(sno, addr_gai if addr_gai.strip() else None) for sno, addr_gai in new_addrs]
            print(f"更新資料：{updates}")  
            update_data(connection, updates)  
            processed_records += len(batch)
        else:
            print(f"批次 {i // batch_size + 1} 處理失敗")

    connection.close()
    print(f"已處理完 {processed_records} 筆資料")

if __name__ == "__main__":
    main()
