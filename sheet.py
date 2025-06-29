import os
import requests
import json
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from pathlib import Path

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InterparkTicketCrawler:
    def __init__(self, credentials_path: str = 'google.json', spreadsheet_name: str = '감사한 티켓팅 신청서', image_folder: str = 'Image'):
        self.credentials_path = credentials_path
        self.spreadsheet_name = spreadsheet_name
        self.image_folder = Path(image_folder)
        self.base_url = "https://tickets.interpark.com/contents/api/open-notice/notice-list"
        self.worksheet = None
        self._setup_google_sheets()
        self.image_folder.mkdir(exist_ok=True)
        
    def _setup_google_sheets(self):
        """구글 시트 API 설정"""
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_path, scope
        )
        gc = gspread.authorize(credentials)
        self.worksheet = gc.open(self.spreadsheet_name).worksheet('Hot')
        
    def _get_request_headers(self):
        """API 요청 헤더 반환"""
        return {
            "host": "tickets.interpark.com",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "accept": "application/json, text/plain, */*",
            "referer": "https://tickets.interpark.com/contents/notice"
        }
    
    def _get_request_params(self):
        """API 요청 파라미터 반환"""
        return {
            "goodsGenre": "ALL", 
            "goodsRegion": "ALL",
            "offset": 0,
            "pageSize": 400,
            "sorting": "OPEN_ASC"
        }
    
    def fetch_ticket_data(self):
        """인터파크 API에서 티켓 데이터 가져오기"""
        response = requests.get(
            self.base_url, 
            params=self._get_request_params(), 
            headers=self._get_request_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def filter_hot_tickets(self, raw_data):
        """HOT 티켓만 필터링"""
        hot_tickets = []
        for ticket in raw_data:
            if not ticket.get('isHot', False):
                continue
                
            ticket_info = {
                '오픈시간': ticket.get('openDateStr', ''),
                '조회수': ticket.get('viewCount', 0),
                '예매타입': ticket.get('openTypeStr', ''),
                '제목': ticket.get('title', ''),
                '예매코드': ticket.get('goodsCode', ''),
                '멀티오픈': ticket.get('hasMultipleOpenDates', False),
                '장르': ticket.get('goodsGenreStr', ''),
                '지역': ticket.get('goodsRegionStr', ''),
                '공연장': ticket.get('venueName', ''),
                'Image': ticket.get('posterImageUrl', '')
            }
            hot_tickets.append(ticket_info)
        
        return hot_tickets
    
    def clear_sheet(self):
        """시트 전체 삭제"""
        self.worksheet.clear()
        logger.info("시트 초기화 완료")
    
    def add_tickets_to_sheet(self, tickets_df):
        """티켓 데이터를 시트에 추가"""
        if tickets_df.empty:
            logger.info("추가할 티켓이 없습니다.")
            return
            
        # 헤더 추가
        headers = list(tickets_df.columns)
        self.worksheet.append_row(headers)
        
        # 데이터 추가 (원래 데이터 타입 유지)
        for _, row in tickets_df.iterrows():
            values = row.tolist()  # 데이터 타입 유지
            self.worksheet.append_row(values, value_input_option='USER_ENTERED')  # 구글 시트가 데이터 타입을 자동으로 인식하도록 설정
            
        logger.info(f"{len(tickets_df)}개의 티켓 추가 완료")
    
    def run(self):
        """크롤링 실행"""
        logger.info("티켓 크롤링 시작")
        
        # 시트 초기화
        self.clear_sheet()
        
        # 데이터 가져오기
        raw_data = self.fetch_ticket_data()
        
        # HOT 티켓 필터링
        hot_tickets = self.filter_hot_tickets(raw_data)
        
        # 데이터프레임 생성
        df = pd.DataFrame(hot_tickets)
        
        if df.empty:
            logger.info("HOT 티켓이 없습니다.")
            return df
        
        # 오픈시간으로 정렬
        df['_temp_datetime'] = pd.to_datetime(df['오픈시간'], format='%Y-%m-%d %H:%M', errors='coerce')
        df = df.sort_values('_temp_datetime')
        df = df.drop(columns=['_temp_datetime'])
        
        # 시트에 추가
        self.add_tickets_to_sheet(df)
        
        logger.info("티켓 크롤링 완료")
        return df

def main():
    """메인 실행 함수"""
    try:
        crawler = InterparkTicketCrawler()
        result_df = crawler.run()
        
        if not result_df.empty:
            print("\n=== HOT 티켓 목록 ===")
            print(result_df.to_string(index=False))
        else:
            print("현재 HOT 티켓이 없습니다.")
            
    except Exception as e:
        logger.error(f"크롤링 실행 중 오류 발생: {e}")
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main() 