import urllib.request
import json
import sys
import getpass

API_BASE = "http://127.0.0.1:8005"

def api_post(path, data):
    req = urllib.request.Request(
        f"{API_BASE}{path}",
        data=json.dumps(data).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    try:
        with urllib.request.urlopen(req) as res:
            return json.loads(res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        try:
            err_json = json.loads(e.read().decode('utf-8'))
            detail = err_json.get("detail", str(e))
            print(f"❌ 请求失败: {detail}")
        except:
            print(f"Error {e.code}: {e.read().decode('utf-8')}")
        return None
    except Exception as e:
        print(f"Request failed: {e}")
        return None

def main():
    print("=== 手动登录助手 ===")
    default_phone = "+85585334633"
    phone_input = input(f"请输入手机号 (默认 {default_phone}): ").strip()
    # 自动去除空格
    phone = (phone_input if phone_input else default_phone).replace(" ", "")
    
    # 使用默认的 Magic ID，这会触发后端从数据库中随机选择一个可用的 Key
    default_api_id = 35019294
    default_api_hash = "9e2d91fe6876d834bae4707b0875e2d7"
    
    print(f"正在发送验证码到 {phone} (自动轮换API Key) ...")
    
    # 尝试发送
    res = api_post("/auth/send_code", {
        "phone": phone,
        "api_id": default_api_id,
        "api_hash": default_api_hash
    })
    
    if not res:
        print("\n=== 可能的解决方案 ===")
        print("1. 如果你在国内，请确保使用了全局VPN或在前端配置了正确的代理")
        print("2. 尝试更换一个API Key (脚本会自动轮换)")
        print("3. 检查手机号是否正确 (包括区号)")
        return

    # 从响应中获取实际使用的 API ID (如果后端进行了轮换)
    used_api_id = res.get("api_id", default_api_id)
    used_api_hash = res.get("api_hash", default_api_hash)
    temp_session = res.get("temp_session")
    
    phone_code_hash = res.get("phone_code_hash")
    print(f"✅ 验证码发送成功! (使用API ID: {used_api_id})")
    
    code = input("请输入验证码: ").strip()
    password = getpass.getpass("请输入二级密码 (无密码直接回车): ").strip()
    
    print("正在登录...")
    login_data = {
        "phone": phone,
        "code": code,
        "phone_code_hash": phone_code_hash,
        "api_id": used_api_id,
        "api_hash": used_api_hash,
        "temp_session": temp_session
    }
    if password:
        login_data["password"] = password
        
    login_res = api_post("/auth/login", login_data)
    if login_res and login_res.get("status") == "success":
        print("\n✅ 登录成功! Session 已保存到数据库。")
    else:
        print("\n❌ 登录失败。")

if __name__ == "__main__":
    main()
