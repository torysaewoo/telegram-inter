import os
import requests
import json
import time
from dotenv import load_dotenv
from datetime import datetime
from interpark import main as get_interpark_tickets

# .env 파일 로드
load_dotenv()

# 환경 변수에서 텔레그램 봇 토큰과 채팅 ID 가져오기
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID')

def send_message(chat_id, text):
    """텔레그램 메시지 전송 함수"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    response = requests.post(url, data=data)
    return response.json()

def create_ticket_message(ticket_info):
    """티켓 정보를 메시지로 변환"""
    if not ticket_info:
        return "<b>오늘 오픈하는 티켓이 없습니다.</b>"
    
    today_date = datetime.now().strftime('%Y년 %m월 %d일')
    message = f"<b>=== {today_date} 티켓 오픈 정보 ===</b>\n\n"
    
    for i, info in enumerate(ticket_info, 1):
        # 제목 정보
        message += f"{i}. <b>[{info['type']}]</b> {info['title']}\n"
        
        # 부가 정보 구성
        goods_info = f"예매코드: {info['goods_code']}" if info['goods_code'] else ""
        count_info = f"조회수: {info['count']}" if info['count'] else ""
        
        # 시간 정보
        message += f"   오픈: {info['open_date']}\n"
        
        # 부가 정보가 있으면 추가
        if goods_info or count_info:
            message += f"   ({count_info}{' / ' if count_info and goods_info else ''}{goods_info})\n"
        
        message += "\n"
    
    return message

# 테스트 및 실행
if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        print("오류: 텔레그램 봇 토큰이 설정되지 않았습니다.")
    elif not ADMIN_CHAT_ID:
        print("오류: 관리자 채팅 ID가 설정되지 않았습니다.")
    else:
        print("인터파크에서 티켓 정보를 가져오는 중...")
        ticket_info = get_interpark_tickets()
        
        # 티켓 정보로 메시지 생성
        message = create_ticket_message(ticket_info)
        
        print("텔레그램으로 메시지 전송 중...")
        result = send_message(ADMIN_CHAT_ID, message)
        
        if result.get('ok'):
            print("메시지 전송 성공!")
        else:
            print(f"메시지 전송 실패: {json.dumps(result, indent=2, ensure_ascii=False)}")