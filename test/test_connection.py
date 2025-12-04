import airbyte as ab
import os
from dotenv import load_dotenv
import json

load_dotenv()

# --- CẤU HÌNH "BAO VÂY" ---
config = {
    "host": os.getenv("ORACLE_HOST"),
    "port": int(os.getenv("ORACLE_PORT")),
    "username": os.getenv("ORACLE_USER"),
    "password": os.getenv("ORACLE_PASSWORD"),
    
    # CHIẾN THUẬT 1: Cung cấp cả SID lẫn Service Name để tránh lỗi NullPointer
    # (Dù DB bạn dùng Service Name, ta cứ đẩy giá trị đó vào key 'sid' để lừa Connector)
    "sid": os.getenv("ORACLE_SERVICE_NAME"),          
    "service_name": os.getenv("ORACLE_SERVICE_NAME"), 
    
    "schemas": ["DCC_TAISUN"], 
    
    # CHIẾN THUẬT 2: Thêm cấu hình SSH Tunnel (BẮT BUỘC với nhiều version mới)
    # Nếu thiếu cái này, code Java thường bị crash khi check tunnel.
    "tunnel_method": {
        "tunnel_method": "NO_TUNNEL"
    },

    # CHIẾN THUẬT 3: Unencrypted chuẩn
    "encryption": {
        "encryption_method": "unencrypted"
    }
}

print(">>> Đang gửi cấu hình sau tới Airbyte Connector:")
print(json.dumps(config, indent=2)) 

# Khởi tạo source
source = ab.get_source("source-oracle", config=config)

print("\n>>> Đang thực hiện Check Connection...")

try:
    source.check()
    print("\n" + "="*50)
    print(">>> KẾT NỐI THÀNH CÔNG! (SUCCESS)")
    print("="*50)
except Exception as e:
    print("\n" + "="*50)
    print(">>> VẪN LỖI. ĐÂY LÀ LOG:")
    print(e)
    print("="*50)

stream_names = ["2022_DOMESTIC_SALES"]
print("get results")
results = source.read(streams=stream_names, force_full_refresh=True)


# source.select_streams(stream_names)
# selected_stream_names = source.get_selected_streams()
# for name in selected_stream_names:
#     stream = source.streams[name]
    
#     # Ép cứng về chế độ Full Refresh Overwrite
#     # Điều này giúp Connector không đòi hỏi Cursor Field nữa
#     stream.sync_mode = "full_refresh_overwrite"
#     print(f"   - ✅ Bảng '{name}': Đã set sang 'full_refresh_overwrite'")