import os
import requests
import json
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pytz


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

def create_ticket_message():
    """티켓 정보를 메시지로 변환"""
    
    url = "https://tickets.interpark.com/contents/api/open-notice/notice-list"

    params = {
        "goodsGenre": "ALL",
        "goodsRegion": "ALL",
        "offset": 0,
        "pageSize": 50,
        "sorting": "OPEN_ASC"
    }

    headers = {
        "host": "tickets.interpark.com",
        "sec-ch-ua-platform": "Windows",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": "https://tickets.interpark.com/contents/notice",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        
        today_date = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y년 %m월 %d일')
        tomorrow = (datetime.now(pytz.timezone('Asia/Seoul')) + timedelta(days=1)).date()
        
        print(f"=== {today_date} 티켓 오픈 정보 ===")
        message = f"<b>🎫 {today_date} 티켓 오픈 정보 🎫</b>\n\n"
        
        for ticket in data:
            open_time = ticket['openDateStr'][11:16]
            title = ticket['title']
            if len(title) > 40:
                title = title[:40] + "..."
            view_count = ticket['viewCount']
            goods_code = ticket['goodsCode']
            open_type = ticket['openTypeStr']
            
            # 날짜 문자열에서 연도, 월, 일 추출
            open_date_str = ticket['openDateStr'][:10]
            year, month, day = map(int, open_date_str.split('-'))
            ticket_date = datetime(year, month, day).date()
            
            # 날짜별 구분 및 서식 추가
            today = datetime.now(pytz.timezone('Asia/Seoul')).date()
            # 내일 날짜인지 확인 (오늘과 내일 티켓만 표시)
            if ticket_date > today + timedelta(days=1):
                break
            if ticket_date == today:
                date_emoji = "🔴 오늘"
            elif ticket_date == today + timedelta(days=1):
                date_emoji = "🟠 내일"
            else:
                date_emoji = f"⚪ {month}월 {day}일"
            
            # 각 티켓 정보를 깔끔하게 포맷팅
            message += f"<b>{date_emoji} [{open_time}]</b>\n"
            message += f"<b>{title}</b>\n"
            message += f"👁 조회수: {view_count}  |  🎟 예매코드: <code>{goods_code}</code>  |  📌{open_type}\n"
            message += "───────────────────\n"
            
            
                
    except Exception as e:
        print(f"API 요청 중 오류 발생: {e}")
        message = f"<b>❌ 티켓 정보를 가져오는데 실패했습니다.</b>\n오류: {str(e)}"
    
    return message

# 테스트 및 실행
if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        print("오류: 텔레그램 봇 토큰이 설정되지 않았습니다.")
    elif not ADMIN_CHAT_ID:
        print("오류: 관리자 채팅 ID가 설정되지 않았습니다.")
    else:
        print("인터파크에서 티켓 정보를 가져오는 중...")
        
        
        # 티켓 정보로 메시지 생성
        message = create_ticket_message()
        
        print("텔레그램으로 메시지 전송 중...")
        result = send_message('-4798861513', message)
        
        if result.get('ok'):
            print("메시지 전송 성공!")
        else:
            print(f"메시지 전송 실패: {json.dumps(result, indent=2, ensure_ascii=False)}")