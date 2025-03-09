import requests
import json
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import threading
import time

# 카카오 개발자 애플리케이션 정보
REST_API_KEY = "69e68d2a908d12343b10fac65e0ce2e8"  # 여기에 REST API 키 입력
REDIRECT_URI = "http://localhost:8888/oauth"
TOKEN_FILE = "kakao_token.json"

# 인증 코드를 받기 위한 서버
class KakaoAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/oauth'):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            # URL에서 인증 코드 추출
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)
            auth_code = params.get('code', [''])[0]
            
            if auth_code:
                # 성공 메시지
                self.wfile.write(b"<html><body><h1>Success!</h1><p>You can close this window now.</p></body></html>")
                # 코드 저장
                self.server.auth_code = auth_code
            else:
                self.wfile.write(b"<html><body><h1>Error!</h1><p>Authorization failed.</p></body></html>")
        else:
            self.send_response(404)
            self.end_headers()

def start_auth_server():
    server = HTTPServer(('localhost', 8888), KakaoAuthHandler)
    server.auth_code = None
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    return server

def get_auth_code():
    # 인증 서버 시작
    server = start_auth_server()
    
    # 카카오 인증 페이지 열기
    auth_url = f"https://kauth.kakao.com/oauth/authorize?client_id={REST_API_KEY}&redirect_uri={REDIRECT_URI}&response_type=code&scope=talk_message"
    webbrowser.open(auth_url)
    
    # 인증 코드 대기
    timeout = 300  # 5분 타임아웃
    start_time = time.time()
    while not server.auth_code and time.time() - start_time < timeout:
        time.sleep(1)
    
    auth_code = server.auth_code
    server.shutdown()
    
    if not auth_code:
        raise Exception("인증 코드를 받지 못했습니다.")
    
    return auth_code

def get_tokens(auth_code):
    # 액세스 토큰 요청
    token_url = "https://kauth.kakao.com/oauth/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": REST_API_KEY,
        "redirect_uri": REDIRECT_URI,
        "code": auth_code
    }
    response = requests.post(token_url, data=data)
    tokens = response.json()
    
    # 만료 시간 추가
    tokens["expires_at"] = time.time() + tokens.get("expires_in", 21600)
    
    # 토큰 저장
    with open(TOKEN_FILE, 'w') as f:
        json.dump(tokens, f)
    
    print(f"토큰이 {TOKEN_FILE}에 저장되었습니다.")
    return tokens

def main():
    try:
        print("카카오 인증을 시작합니다...")
        auth_code = get_auth_code()
        print(f"인증 코드: {auth_code}")
        tokens = get_tokens(auth_code)
        print("인증 성공!")
    except Exception as e:
        print(f"오류: {e}")

if __name__ == "__main__":
    main()