import airbyte as ab
import os
from dotenv import load_dotenv
import json


load_dotenv()

# --- CẤU HÌNH CHUẨN (ĐÃ SỬA) ---
config = {
    "host": os.getenv("ORACLE_HOST"),
    "port": int(os.getenv("ORACLE_PORT")),
    "username": os.getenv("ORACLE_USER"),
    "password": os.getenv("ORACLE_PASSWORD"),
    
    # --- ĐIỂM SỬA QUAN TRỌNG NHẤT ---
    # Phải gom vào 'connection_data'. 
    # Airbyte chỉ cho phép chọn 1 trong 2: Service Name HOẶC SID.
    # Ưu tiên Service Name (TOPPROD) như cấu hình bạn đã gửi lúc đầu.
    "connection_data": {
        "service_name": os.getenv("ORACLE_SERVICE_NAME")
    },
    
    "schemas": ["DCC_TAISUN"], 
    
    # Cái này bạn làm đúng rồi, giữ nguyên
    "tunnel_method": {
        "tunnel_method": "NO_TUNNEL"
    },

    # Cái này bạn làm đúng rồi, giữ nguyên
    "encryption": {
        "encryption_method": "unencrypted"
    }
}

print(">>> Đang gửi cấu hình sau tới Airbyte Connector:")
# print(json.dumps(config, indent=2)) # Tạm ẩn để bảo mật pass khi chụp ảnh

# --- PHẦN KHỞI TẠO (GIỮ NGUYÊN) ---
source = ab.get_source(
    name="source-oracle",
    connector="airbyte/source-oracle:latest", 
    config=config
)

print("\n>>> Đang thực hiện Check Connection...")

try:
    source.check()
    print("\n" + "="*50)
    print(">>> KẾT NỐI THÀNH CÔNG! (SUCCESS)")
    print("="*50)
    
    # Chỉ chạy tìm bảng khi kết nối đã OK
    print(">>> [2/5] Đang tìm bảng...")
    available_streams = source.get_available_streams()
    print(f">>> Tìm thấy {len(available_streams)} bảng.")
    
except Exception as e:
    print("\n" + "="*50)
    print(">>> LỖI KẾT NỐI (CHECK CONFIG):")
    print(e)
    print("="*50)