import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
from urllib.parse import urljoin

# 1. HTML 파일 읽기 또는 웹 페이지에서 데이터 가져오기
def get_ticket_info_from_file(file_path):
    """HTML 파일에서 티켓 정보를 추출"""
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
    return extract_ticket_info(html_content)

def get_ticket_info_from_url(url, page=1):
    """웹사이트에서 티켓 정보를 추출"""
    # URL에 페이지 번호 설정
    url = url.replace('pageno=1', f'pageno={page}')
    
    # Chrome 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 헤드리스 모드
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920x1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    # WebDriver 초기화
    driver = webdriver.Chrome(options=chrome_options)
    
    # requests 세션 설정 (상세 페이지 처리용)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    session = requests.Session()
    session.headers.update(headers)
    
    try:
        # 페이지 로드
        driver.get(url)
        
        # 페이지 로딩 대기
        time.sleep(2)
        
        # 현재 페이지의 HTML 가져오기
        html_content = driver.page_source
        return extract_ticket_info(html_content, session)
        
    finally:
        driver.quit()

def get_all_pages_ticket_info(base_url, max_pages=3):
    """여러 페이지에서 티켓 정보를 가져옴"""
    all_tickets = []
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    
    for page in range(1, max_pages + 1):
        print(f"페이지 {page} 정보 가져오는 중...")
        tickets = get_ticket_info_from_url(base_url, page)
        if not tickets:  # 더 이상 티켓 정보가 없으면 종료
            break
            
        # 내일 날짜가 있는지 확인
        found_tomorrow = False
        for ticket in tickets:
            # 날짜 문자열에서 연도, 월, 일 추출
            match = re.search(r'(\d{2})\.(\d{2})\.(\d{2})', ticket['open_date'])
            if match:
                year, month, day = match.groups()
                year = int('20' + year)  # '25' -> 2025
                month = int(month)
                day = int(day)
                
                ticket_date = datetime(year, month, day).date()
                if ticket_date == tomorrow:
                    found_tomorrow = True
                    break
        
        all_tickets.extend(tickets)
        
        # 내일 날짜가 발견되면 더 이상 페이지를 가져오지 않음
        if found_tomorrow:
            print("내일 날짜의 티켓이 발견되어 페이지 탐색을 중단합니다.")
            break
    
    return all_tickets

def is_today_ticket(date_str):
    """티켓 오픈 날짜가 오늘인지 확인"""
    # 날짜 형식 예: '25.02.26(화) 14:00'
    today = datetime.now()
    
    # 날짜 문자열에서 연도, 월, 일 추출
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{2})', date_str)
    if not match:
        return False
    
    year, month, day = match.groups()
    year = int('20' + year)  # '25' -> 2025
    month = int(month)
    day = int(day)
    
    ticket_date = datetime(year, month, day)
    
    # 오늘 날짜와 비교
    return ticket_date.date() == today.date()

# 2. HTML 내용에서 티켓 정보 추출
def extract_ticket_info(html_content, session=None):
    """HTML 내용에서 티켓 정보(제목과 날짜) 추출"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    ticket_info = []
    
    # 모든 행(tr) 탐색
    rows = soup.select('table tbody tr')
    for row in rows:
        # 제목 추출
        title_element = row.select_one('td.subject a')
        date_element = row.select_one('td.date')
        type_element = row.select_one('td.type')
        count_element = row.select_one('td.count')
        
        detail_url = None
        detail_url2 = None
        booking_code = None
        error_info = None
        
        if title_element and title_element.has_attr('href'):
            detail_url = 'https://ticket.interpark.com/webzine/paper/' + title_element['href']
            title_text = title_element.text.strip()
            
            # requests로 상세 페이지 접속하여 예매 링크(detail_url2) 가져오기
            if session:
                try:
                    detail_response = session.get(detail_url, timeout=10)
                    detail_response.raise_for_status()
                    
                    detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                    
                    # 예매 링크 찾기 (여러 방법 시도)
                    # 1. a.btn_book 찾기
                    book_button = detail_soup.select_one('a.btn_book')
                    if book_button and book_button.get('href'):
                        detail_url2 = book_button.get('href')
                    
                    # 2. tickets.interpark.com 또는 contents/bridge가 포함된 링크 찾기
                    if not detail_url2:
                        for link in detail_soup.find_all('a', href=True):
                            href = link.get('href')
                            if href and ('tickets.interpark.com' in href or 'contents/bridge' in href):
                                detail_url2 = href
                                break
                    
                    # 3. 예매 관련 텍스트가 포함된 링크 찾기
                    if not detail_url2:
                        for link in detail_soup.find_all('a', href=True):
                            link_text = link.text.strip().lower()
                            if link_text and ('예매' in link_text or '구매' in link_text or '티켓' in link_text):
                                detail_url2 = link.get('href')
                                break
                    
                    # URL이 상대 경로인 경우 절대 경로로 변환
                    if detail_url2 and not detail_url2.startswith(('http://', 'https://')):
                        detail_url2 = urljoin(detail_url, detail_url2)
                    
                    # 예매 코드 추출 (마지막 슬래시 이후의 문자)
                    if detail_url2:
                        booking_code = detail_url2.split('/')[-1]
                    
                except Exception as e:
                    error_info = str(e)
                    print(f"오류 ({title_text}): {e}")
        
        if title_element and date_element:
            ticket_info.append({
                'title': title_element.text.strip(),
                'open_date': date_element.text.strip(),
                'type': type_element.text.strip() if type_element else '',
                'count': count_element.text.strip() if count_element else '',
                'booking_code': booking_code,
                'detail_url': detail_url,
                'error_info': error_info
            })
    
    return ticket_info

# 3. 텔레그램 메시지 보내기
class TelegramBot:
    def __init__(self, config_path='telegram_config.json'):
        """텔레그램 봇 초기화"""
        self.config_path = config_path
        self.config = self.load_config()
        
    def load_config(self):
        """설정 로드"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                return json.load(f)
        return {"bot_token": "", "subscribers": []}
    
    def save_config(self):
        """설정 저장"""
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f)
    
    def setup_bot(self):
        """봇 설정"""
        # 환경변수에서 토큰 가져오기
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        print(f"환경 변수 TELEGRAM_BOT_TOKEN: {bot_token}")
        
        # 환경변수에 토큰이 없으면 설정 파일에서 가져오기
        if not bot_token:
            bot_token = self.config.get("bot_token")
            print(f"설정 파일에서 가져온 bot_token: {bot_token}")
            if not bot_token:
                print("오류: TELEGRAM_BOT_TOKEN 환경변수 또는 설정 파일에 봇 토큰이 없습니다.")
                return False
        else:
            # 환경변수에서 가져온 토큰을 설정에 저장
            self.config["bot_token"] = bot_token
            
        # 관리자 채팅 ID 설정
        admin_chat_id = os.getenv('ADMIN_CHAT_ID')
        print(f"환경 변수 ADMIN_CHAT_ID: {admin_chat_id}")
        if admin_chat_id:
            self.config["admin_chat_id"] = admin_chat_id
        elif not self.config.get("admin_chat_id"):
            print("경고: ADMIN_CHAT_ID 환경변수 또는 설정 파일에 관리자 채팅 ID가 없습니다.")
            self.config["admin_chat_id"] = ""
        
        # 설정 저장
        self.save_config()
        
        return True
    
    def set_bot_commands(self):
        """봇 명령어 설정"""
        url = f"https://api.telegram.org/bot{self.config['bot_token']}/setMyCommands"
        commands = [
            {"command": "start", "description": "봇 시작 및 구독"},
            {"command": "stop", "description": "구독 취소"},
            {"command": "help", "description": "도움말"}
        ]
        data = {"commands": json.dumps(commands)}
        requests.post(url, data=data)
    
    def add_subscriber(self, chat_id):
        """구독자 추가"""
        if "subscribers" not in self.config:
            self.config["subscribers"] = []
            
        # 문자열로 변환하여 저장 (일관성 유지)
        chat_id = str(chat_id)
            
        if chat_id not in self.config["subscribers"]:
            self.config["subscribers"].append(chat_id)
            self.save_config()
            print(f"구독자 추가됨: {chat_id}")
            return True
        return False
    
    def remove_subscriber(self, chat_id):
        """구독자 제거"""
        # 문자열로 변환하여 비교 (일관성 유지)
        chat_id = str(chat_id)
        
        if "subscribers" in self.config and chat_id in self.config["subscribers"]:
            self.config["subscribers"].remove(chat_id)
            self.save_config()
            print(f"구독자 제거됨: {chat_id}")
            return True
        return False
    
    def get_updates(self):
        """봇 업데이트 확인 및 처리"""
        url = f"https://api.telegram.org/bot{self.config['bot_token']}/getUpdates"
        response = requests.get(url)
        updates = response.json()
        
        if updates.get('ok') and updates.get('result'):
            for update in updates['result']:
                if 'message' in update and 'text' in update['message']:
                    chat_id = str(update['message']['chat']['id'])
                    text = update['message']['text']
                    
                    # /start 명령어 처리
                    if text == '/start':
                        if self.add_subscriber(chat_id):
                            self.send_message_to_chat("구독이 완료되었습니다. 티켓 오픈 정보를 받아보실 수 있습니다.", chat_id)
                        else:
                            self.send_message_to_chat("이미 구독 중입니다.", chat_id)
                    
                    # /stop 명령어 처리
                    elif text == '/stop':
                        if self.remove_subscriber(chat_id):
                            self.send_message_to_chat("구독이 취소되었습니다.", chat_id)
                        else:
                            self.send_message_to_chat("구독 중이 아닙니다.", chat_id)
                    
                    # /help 명령어 처리
                    elif text == '/help':
                        help_text = (
                            "<b>인터파크 티켓 오픈 알림 봇 도움말</b>\n\n"
                            "/start - 봇 구독 시작\n"
                            "/stop - 봇 구독 취소\n"
                            "/help - 도움말 보기\n\n"
                            "이 봇은 인터파크의 오늘 티켓 오픈 정보를 알려드립니다."
                        )
                        self.send_message_to_chat(help_text, chat_id)
    
    def send_message_to_chat(self, message, chat_id):
        """특정 채팅에 메시지 보내기"""
        url = f"https://api.telegram.org/bot{self.config['bot_token']}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        
        try:
            response = requests.post(url, data=data)
            result = response.json()
            if result.get('ok'):
                print(f"메시지 전송 성공 (chat_id: {chat_id})")
            else:
                print(f"메시지 전송 실패 (chat_id: {chat_id}): {result.get('description')}")
            return result
        except Exception as e:
            print(f"메시지 전송 중 오류 발생 (chat_id: {chat_id}): {e}")
            return {"ok": False, "description": str(e)}
    
    def send_message_to_all(self, message):
        """모든 구독자에게 메시지 보내기"""
        results = []
        
        if not self.config.get("subscribers"):
            print("구독자가 없습니다.")
            return {"ok": False, "description": "No subscribers"}
        
        print(f"\n=== 메시지 전송 시작 ===")
        print(f"총 구독자 수: {len(self.config['subscribers'])}")
        print(f"구독자 목록: {self.config['subscribers']}")
        
        for chat_id in self.config["subscribers"]:
            print(f"메시지 전송 중... (chat_id: {chat_id})")
            result = self.send_message_to_chat(message, chat_id)
            results.append(result)
            # 텔레그램 API 제한을 피하기 위한 짧은 대기
            time.sleep(0.5)
        
        print(f"=== 메시지 전송 완료 ===\n")
        return {"ok": True, "results": results}

# 4. 실행 함수
def main():
    # 환경 변수 확인
    print("=== 환경 변수 확인 ===")
    print(f"TELEGRAM_BOT_TOKEN 환경 변수 존재: {'있음' if os.getenv('TELEGRAM_BOT_TOKEN') else '없음'}")
    print(f"ADMIN_CHAT_ID 환경 변수 존재: {'있음' if os.getenv('ADMIN_CHAT_ID') else '없음'}")
    print("=====================")
    
    # 텔레그램 봇 초기화
    telegram = TelegramBot()
    
    # 봇 설정이 없으면 설정
    if not telegram.config.get("bot_token") or not telegram.config.get("admin_chat_id"):
        if not telegram.setup_bot():
            print("봇 설정에 실패했습니다. 프로그램을 종료합니다.")
            return
    
    # 봇 명령어 설정
    telegram.set_bot_commands()
    
    # 봇 업데이트 확인 및 처리
    telegram.get_updates()
    
    # 구독자 목록 확인
    print(f"구독자 수: {len(telegram.config.get('subscribers', []))}")
    
    # 티켓 정보 가져오기 - 여러 페이지에서 가져오기
    iframe_url = 'https://ticket.interpark.com/webzine/paper/TPNoticeList_iFrame.asp?bbsno=34&pageno=1&KindOfGoods=TICKET&Genre=&sort=opendate&stext='
    all_ticket_info = get_all_pages_ticket_info(iframe_url, max_pages=3)
    
    # 오늘 날짜에 해당하는 티켓만 필터링
    today_ticket_info = [ticket for ticket in all_ticket_info if is_today_ticket(ticket['open_date'])]
    
    print(f"전체 티켓 수: {len(all_ticket_info)}, 오늘 오픈 티켓 수: {len(today_ticket_info)}")
    
    if not today_ticket_info:
        message = "<b>오늘 오픈하는 티켓이 없습니다.</b>"
    else:
        # 메시지 형식 만들기
        today_date = datetime.now().strftime('%Y년 %m월 %d일')
        message = f"<b>=== {today_date} 티켓 오픈 정보 ===</b>\n\n"
        for i, info in enumerate(today_ticket_info, 1):
            booking_info = f"\n   예매코드: {info['booking_code']}" if info['booking_code'] else ""
            count_info = f" (조회수: {info['count']})" if info['count'] else ""
            message += f"{i}. <b>[{info['type']}]</b> {info['title']}{count_info}\n   오픈: {info['open_date']}{booking_info}\n\n"
    
    # 모든 구독자에게 메시지 보내기
    result = telegram.send_message_to_all(message)
    
    if result.get('ok'):
        print("메시지 전송 성공!")
    else:
        print(f"메시지 전송 실패: {result}")

if __name__ == "__main__":
    main()


