import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import re

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
        time.sleep(1)
        
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
        goods_code = None
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
                    
                    # "goodsCode":"25005000" 문자열을 찾아서 숫자 추출
                    goods_code = None
                    goods_code_pattern = re.search(r'"goodsCode":"(\d+)"', detail_response.text)
                    if goods_code_pattern:
                        goods_code = goods_code_pattern.group(1)
                    
                    
                except Exception as e:
                    error_info = str(e)
                    print(f"오류 ({title_text}): {e}")
        
        if title_element and date_element:
            ticket_info.append({
                'title': title_element.text.strip(),
                'open_date': date_element.text.strip(),
                'type': type_element.text.strip() if type_element else '',
                'count': count_element.text.strip() if count_element else '',
                'goods_code': goods_code,
                'detail_url': detail_url,
                'error_info': error_info
            })
    
    return ticket_info

# 4. 실행 함수
def main():
    # 티켓 정보 가져오기 - 여러 페이지에서 가져오기
    iframe_url = 'https://ticket.interpark.com/webzine/paper/TPNoticeList_iFrame.asp?bbsno=34&pageno=1&KindOfGoods=TICKET&Genre=&sort=opendate&stext='
    all_ticket_info = get_all_pages_ticket_info(iframe_url, max_pages=3)
    
    # 오늘 날짜에 해당하는 티켓만 필터링
    today_ticket_info = [ticket for ticket in all_ticket_info if is_today_ticket(ticket['open_date'])]
    
    print(f"전체 티켓 수: {len(all_ticket_info)}, 오늘 오픈 티켓 수: {len(today_ticket_info)}")
    
    if not today_ticket_info:
        print("오늘 오픈하는 티켓이 없습니다.")
    else:
        # 오늘 티켓 정보 출력
        today_date = datetime.now().strftime('%Y년 %m월 %d일')
        print(f"=== {today_date} 티켓 오픈 정보 ===")
        for i, info in enumerate(today_ticket_info, 1):
            # 제목 정보 출력
            print(f"{i}. [{info['type']}] {info['title']}")
            
            # 부가 정보 구성
            booking_info = f"{info['goods_code']} ) " if info['goods_code'] else ""
            count_info = f"{info['count']} / " if info['count'] else ""
            
            # 부가 정보 및 오픈 시간 출력
            print(f"    ( 조회수: {count_info}오픈: {info['open_date']} / 예매코드: {booking_info}")
            print()
    
    return today_ticket_info

if __name__ == "__main__":
    main()


