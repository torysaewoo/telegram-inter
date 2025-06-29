import os
import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import List, Dict, Any, Set, Optional
import logging
from urllib.parse import urlparse
from pathlib import Path
import hashlib
import tweepy
import re
import random
from dataclasses import dataclass
import schedule
import threading

# 환경변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class PostingConfig:
    """게시 설정 클래스"""
    # 게시 간격 (분)
    peak_interval_min: int = 5    # 피크시간 최소 간격
    peak_interval_max: int = 10   # 피크시간 최대 간격
    normal_interval_min: int = 15 # 일반시간 최소 간격
    normal_interval_max: int = 30 # 일반시간 최대 간격
    night_interval_min: int = 60  # 심야시간 최소 간격
    night_interval_max: int = 120 # 심야시간 최대 간격
    
    # 피크시간대 (시간)
    peak_hours: List[tuple] = None
    
    # API 제한
    max_tweets_per_15min: int = 50  # 15분당 최대 트윗 수 (보수적)
    max_tweets_per_day: int = 500   # 하루 최대 트윗 수 (보수적)
    
    def __post_init__(self):
        if self.peak_hours is None:
            self.peak_hours = [
                (9, 11),   # 오전
                (12, 13),  # 점심
                (15, 17),  # 오후
                (19, 22)   # 저녁
            ]

class GoogleSheetsManager:
    """구글 시트 관리 클래스"""
    
    def __init__(self, credentials_path: str, spreadsheet_name: str):
        self.credentials_path = credentials_path
        self.spreadsheet_name = spreadsheet_name
        self.gc = None
        self.hot_worksheet = None
        self.posting_worksheet = None
        self._setup_google_sheets()
    
    def _setup_google_sheets(self):
        """구글 시트 API 설정"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.credentials_path, scope
            )
            self.gc = gspread.authorize(credentials)
            
            spreadsheet = self.gc.open(self.spreadsheet_name)
            self.hot_worksheet = spreadsheet.worksheet('Hot')
            
            # PostingQueue 시트 생성/접근
            try:
                self.posting_worksheet = spreadsheet.worksheet('PostingQueue')
            except gspread.WorksheetNotFound:
                # PostingQueue 시트가 없으면 생성
                self.posting_worksheet = spreadsheet.add_worksheet(
                    title="PostingQueue", rows="1000", cols="15"
                )
                self._initialize_posting_queue_headers()
            
            logger.info("구글 시트 연결 성공")
            
        except Exception as e:
            logger.error(f"구글 시트 설정 실패: {e}")
            raise
    
    def _initialize_posting_queue_headers(self):
        """PostingQueue 시트 헤더 초기화"""
        headers = [
            '티켓ID', '제목', '예매코드', '오픈시간', '장르', '조회수',
            '이미지경로', '게시상태', '우선순위', '예약시간', '게시시간',
            '에러메시지', '재시도횟수', '생성시간', '트윗URL'
        ]
        self.posting_worksheet.append_row(headers)
        logger.info("PostingQueue 시트 헤더 초기화 완료")
    
    def add_to_posting_queue(self, ticket_data: Dict[str, Any], local_image_path: str = "") -> bool:
        """게시 대기열에 티켓 추가"""
        try:
            # 티켓 ID 생성 (예매코드 + 제목 해시)
            ticket_id = self._generate_ticket_id(ticket_data)
            
            # 이미 존재하는지 확인
            if self._is_ticket_in_queue(ticket_id):
                logger.info(f"이미 대기열에 있는 티켓: {ticket_data['제목']}")
                return False
            
            # 우선순위 계산
            priority = self._calculate_priority(ticket_data)
            
            # 예약시간 계산
            scheduled_time = self._calculate_scheduled_time(priority)
            
            row_data = [
                ticket_id,
                ticket_data['제목'],
                ticket_data.get('예매코드', ''),
                ticket_data.get('오픈시간', ''),
                ticket_data.get('장르', ''),
                ticket_data.get('조회수', 0),
                local_image_path,
                '대기',  # 게시상태
                priority,
                scheduled_time.strftime('%Y-%m-%d %H:%M:%S') if scheduled_time else '',
                '',  # 게시시간
                '',  # 에러메시지
                0,   # 재시도횟수
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # 생성시간
                ''   # 트윗URL
            ]
            
            self.posting_worksheet.append_row(row_data)
            logger.info(f"게시 대기열에 추가: {ticket_data['제목']} (우선순위: {priority})")
            return True
            
        except Exception as e:
            logger.error(f"게시 대기열 추가 실패: {e}")
            return False
    
    def _generate_ticket_id(self, ticket_data: Dict[str, Any]) -> str:
        """티켓 고유 ID 생성"""
        unique_string = f"{ticket_data.get('예매코드', '')}{ticket_data['제목']}{ticket_data.get('오픈시간', '')}"
        return hashlib.md5(unique_string.encode('utf-8')).hexdigest()[:12]
    
    def _is_ticket_in_queue(self, ticket_id: str) -> bool:
        """티켓이 이미 대기열에 있는지 확인"""
        try:
            all_records = self.posting_worksheet.get_all_records()
            return any(record.get('티켓ID') == ticket_id for record in all_records)
        except:
            return False
    
    def _calculate_priority(self, ticket_data: Dict[str, Any]) -> int:
        """우선순위 계산 (높을수록 우선)"""
        priority = 50  # 기본값
        
        # 오픈시간 임박도
        open_time_str = ticket_data.get('오픈시간', '')
        if open_time_str:
            try:
                # 오픈시간 파싱 시도
                now = datetime.now()
                # 다양한 형식의 오픈시간 처리
                if '시간' in open_time_str and '(' in open_time_str:
                    # "2025.01.15 (수) 20:00" 형식
                    date_part = open_time_str.split('(')[0].strip()
                    time_part = open_time_str.split(')')[1].strip() if ')' in open_time_str else '20:00'
                    datetime_str = f"{date_part} {time_part}"
                    open_time = datetime.strptime(datetime_str, '%Y.%m.%d %H:%M')
                    
                    hours_diff = (open_time - now).total_seconds() / 3600
                    if hours_diff <= 24:
                        priority += 30  # 24시간 이내
                    elif hours_diff <= 72:
                        priority += 20  # 3일 이내
                    elif hours_diff <= 168:
                        priority += 10  # 1주일 이내
            except:
                pass
        
        # 조회수 기반
        view_count = ticket_data.get('조회수', 0)
        if view_count > 10000:
            priority += 20
        elif view_count > 5000:
            priority += 15
        elif view_count > 1000:
            priority += 10
        
        # 아티스트 인기도
        title = ticket_data.get('제목', '').upper()
        high_priority_artists = ['BTS', '세븐틴', 'SEVENTEEN', '블랙핑크', 'BLACKPINK', '뉴진스', 'NEWJEANS']
        medium_priority_artists = ['IVE', 'AESPA', '에스파', '르세라핌', 'LE SSERAFIM']
        
        for artist in high_priority_artists:
            if artist in title:
                priority += 25
                break
        else:
            for artist in medium_priority_artists:
                if artist in title:
                    priority += 15
                    break
        
        # 장르별 가중치
        genre = ticket_data.get('장르', '')
        if '콘서트' in genre or 'CONCERT' in genre.upper():
            priority += 15
        elif '뮤지컬' in genre:
            priority += 10
        elif '페스티벌' in genre:
            priority += 12
        
        return min(priority, 100)  # 최대 100
    
    def _calculate_scheduled_time(self, priority: int) -> Optional[datetime]:
        """우선순위 기반 예약시간 계산"""
        now = datetime.now()
        
        # 우선순위가 높을수록 빨리 게시
        if priority >= 80:
            delay_minutes = random.randint(5, 15)  # 5-15분 후
        elif priority >= 60:
            delay_minutes = random.randint(15, 60)  # 15분-1시간 후
        else:
            delay_minutes = random.randint(60, 180)  # 1-3시간 후
        
        scheduled = now + timedelta(minutes=delay_minutes)
        
        # 피크시간대로 조정
        scheduled = self._adjust_to_peak_hours(scheduled)
        
        return scheduled
    
    def _adjust_to_peak_hours(self, scheduled_time: datetime) -> datetime:
        """피크시간대로 시간 조정"""
        config = PostingConfig()
        hour = scheduled_time.hour
        
        # 이미 피크시간이면 그대로
        for start, end in config.peak_hours:
            if start <= hour < end:
                return scheduled_time
        
        # 피크시간이 아니면 가장 가까운 피크시간으로 조정
        next_peak_hour = None
        min_diff = float('inf')
        
        for start, end in config.peak_hours:
            diff = abs(hour - start)
            if diff < min_diff:
                min_diff = diff
                next_peak_hour = start
        
        if next_peak_hour:
            adjusted = scheduled_time.replace(hour=next_peak_hour, minute=random.randint(0, 59))
            # 과거 시간이면 다음날로
            if adjusted <= datetime.now():
                adjusted += timedelta(days=1)
            return adjusted
        
        return scheduled_time
    
    def get_pending_posts(self, limit: int = 10) -> List[Dict[str, Any]]:
        """게시 대기 중인 항목들 조회"""
        try:
            all_records = self.posting_worksheet.get_all_records()
            
            # 대기 상태이고 예약시간이 된 항목들
            now = datetime.now()
            pending = []
            
            for record in all_records:
                if record.get('게시상태') == '대기':
                    scheduled_str = record.get('예약시간', '')
                    if scheduled_str:
                        try:
                            scheduled_time = datetime.strptime(scheduled_str, '%Y-%m-%d %H:%M:%S')
                            if scheduled_time <= now:
                                pending.append(record)
                        except:
                            pending.append(record)  # 시간 파싱 실패시 즉시 게시
                    else:
                        pending.append(record)  # 예약시간 없으면 즉시 게시
            
            # 우선순위순 정렬
            pending.sort(key=lambda x: x.get('우선순위', 0), reverse=True)
            
            return pending[:limit]
            
        except Exception as e:
            logger.error(f"대기 항목 조회 실패: {e}")
            return []
    
    def update_posting_status(self, ticket_id: str, status: str, tweet_url: str = '', error_msg: str = ''):
        """게시 상태 업데이트"""
        try:
            all_values = self.posting_worksheet.get_all_values()
            if not all_values:
                return False
            
            headers = all_values[0]
            data_rows = all_values[1:]
            
            # 컬럼 인덱스 찾기
            try:
                ticket_id_col = headers.index('티켓ID') + 1
                status_col = headers.index('게시상태') + 1
                post_time_col = headers.index('게시시간') + 1
                error_col = headers.index('에러메시지') + 1
                retry_col = headers.index('재시도횟수') + 1
                tweet_url_col = headers.index('트윗URL') + 1
            except ValueError as e:
                logger.error(f"컬럼을 찾을 수 없습니다: {e}")
                return False
            
            # 해당 티켓 찾아서 업데이트
            for row_index, row_data in enumerate(data_rows, start=2):
                if len(row_data) >= len(headers) and row_data[headers.index('티켓ID')] == ticket_id:
                    # 상태 업데이트
                    self.posting_worksheet.update_cell(row_index, status_col, status)
                    
                    if status == '완료':
                        self.posting_worksheet.update_cell(row_index, post_time_col, 
                                                         datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                        if tweet_url:
                            self.posting_worksheet.update_cell(row_index, tweet_url_col, tweet_url)
                    elif status == '실패':
                        if error_msg:
                            self.posting_worksheet.update_cell(row_index, error_col, error_msg)
                        # 재시도 횟수 증가
                        current_retry = int(row_data[headers.index('재시도횟수')] or 0)
                        self.posting_worksheet.update_cell(row_index, retry_col, current_retry + 1)
                    
                    logger.info(f"게시 상태 업데이트: {ticket_id} -> {status}")
                    return True
            
            logger.warning(f"티켓 ID를 찾을 수 없습니다: {ticket_id}")
            return False
            
        except Exception as e:
            logger.error(f"게시 상태 업데이트 실패: {e}")
            return False

class TwitterBot:
    """향상된 트위터 자동 게시 봇"""
    
    def __init__(self):
        self.api = None
        self.client = None
        self.last_post_time = None
        self.posts_in_15min = []
        self.posts_today = 0
        self.config = PostingConfig()
        self._setup_twitter_api()
    
    def _setup_twitter_api(self):
        """트위터 API 설정"""
        try:
            # API v1.1 (이미지 업로드용)
            auth = tweepy.OAuthHandler(
                os.getenv('TWITTER_API_KEY'),
                os.getenv('TWITTER_API_SECRET')
            )
            auth.set_access_token(
                os.getenv('TWITTER_ACCESS_TOKEN'),
                os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
            )
            self.api = tweepy.API(auth)
            
            # API v2 (트윗 게시용)
            self.client = tweepy.Client(
                consumer_key=os.getenv('TWITTER_API_KEY'),
                consumer_secret=os.getenv('TWITTER_API_SECRET'),
                access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
                access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET'),
                wait_on_rate_limit=True
            )
            
            logger.info("트위터 API 설정 완료")
            
        except Exception as e:
            logger.error(f"트위터 API 설정 실패: {e}")
            raise
    
    def _can_post_now(self) -> bool:
        """현재 게시 가능한지 확인"""
        now = datetime.now()
        
        # 15분 내 게시 수 확인
        self.posts_in_15min = [post_time for post_time in self.posts_in_15min 
                              if (now - post_time).total_seconds() < 900]
        
        if len(self.posts_in_15min) >= self.config.max_tweets_per_15min:
            logger.warning("15분 내 게시 제한 도달")
            return False
        
        # 하루 게시 수 확인
        if self.posts_today >= self.config.max_tweets_per_day:
            logger.warning("일일 게시 제한 도달")
            return False
        
        # 최소 간격 확인
        if self.last_post_time:
            min_interval = self._get_current_min_interval()
            time_diff = (now - self.last_post_time).total_seconds() / 60
            if time_diff < min_interval:
                logger.info(f"최소 간격 미충족: {time_diff:.1f}분 < {min_interval}분")
                return False
        
        return True
    
    def _get_current_min_interval(self) -> int:
        """현재 시간대의 최소 게시 간격 반환"""
        current_hour = datetime.now().hour
        
        # 피크시간 확인
        for start, end in self.config.peak_hours:
            if start <= current_hour < end:
                return self.config.peak_interval_min
        
        # 심야시간 (23-7시)
        if current_hour >= 23 or current_hour < 7:
            return self.config.night_interval_min
        
        # 일반시간
        return self.config.normal_interval_min
    
    def _extract_artist_keywords(self, title: str) -> List[str]:
        """제목에서 아티스트 키워드 추출"""
        artist_keywords = {
            '세븐틴': ['세븐틴', 'SEVENTEEN', '에스쿱스', '정한', '조슈아', '준', '호시', '원우', '우지', '디에잇', '민규', '도겸', '승관', '버논', '디노'],
            'BTS': ['BTS', '방탄소년단', 'RM', '진', '슈가', '제이홉', '지민', '뷔', '정국'],
            '블랙핑크': ['블랙핑크', 'BLACKPINK', '지수', '제니', '로제', '리사'],
            '뉴진스': ['뉴진스', 'NewJeans', '민지', '하니', '다니엘', '해린', '혜인'],
            '아이브': ['아이브', 'IVE', '유진', '가을', '레이', '원영', '리즈', '이서'],
            '르세라핌': ['르세라핌', 'LE SSERAFIM', '김채원', '사쿠라', '허윤진', '카즈하', '홍은채'],
            '(여자)아이들': ['(여자)아이들', 'G-IDLE', '미연', '민니', '소연', '우기', '슈화'],
            '에스파': ['에스파', 'aespa', '카리나', '지젤', '윈터', '닝닝'],
            '트와이스': ['트와이스', 'TWICE', '나연', '정연', '모모', '사나', '지효', '미나', '다현', '채영', '쯔위']
        }
        
        title_upper = title.upper()
        found_keywords = []
        
        for group, keywords in artist_keywords.items():
            for keyword in keywords:
                if keyword.upper() in title_upper:
                    found_keywords.append(f"#{keyword}")
                    if group not in [k.replace('#', '') for k in found_keywords]:
                        found_keywords.append(f"#{group}")
                    break
        
        return found_keywords[:5]
    
    def _generate_hashtags(self, ticket_info: Dict[str, Any]) -> str:
        """티켓 정보를 바탕으로 해시태그 생성"""
        hashtags = []
        
        # 아티스트 관련 해시태그
        artist_tags = self._extract_artist_keywords(ticket_info['제목'])
        hashtags.extend(artist_tags)
        
        # 장르별 해시태그
        genre = ticket_info.get('장르', '')
        if '콘서트' in genre or 'CONCERT' in genre.upper():
            hashtags.append('#콘서트')
        elif '뮤지컬' in genre:
            hashtags.append('#뮤지컬')
        elif '연극' in genre:
            hashtags.append('#연극')
        elif '페스티벌' in genre:
            hashtags.append('#페스티벌')
        
        # 기본 해시태그
        base_tags = ['#티켓팅', '#대리티켓팅', '#선착순할인']
        hashtags.extend(base_tags)
        
        # 중복 제거 및 길이 제한
        unique_hashtags = list(dict.fromkeys(hashtags))[:10]
        return ' '.join(unique_hashtags)
    
    def _create_tweet_text(self, ticket_info: Dict[str, Any]) -> str:
        """트윗 텍스트 생성"""
        title = ticket_info['제목']
        open_time = ticket_info.get('오픈시간', '')
        
        # 기본 템플릿
        tweet_template = f"""{title}

대리 티켓팅 진행
최근 세븐틴 / BTS / 블랙핑크 댈티 성공경력

선착순 할인 이벤트:
VIP 잡아도 수고비 5만원 선입금, 실패시 수고비 전액환불

🕐 오픈시간: {open_time}

친절한 상담: https://open.kakao.com/o/sAJ8m2Ah"""
        
        # 해시태그 추가
        hashtags = self._generate_hashtags(ticket_info)
        
        # 트위터 길이 제한 (280자) 고려
        base_text = f"{tweet_template}\n\n{hashtags}"
        
        if len(base_text) > 270:
            # 제목이 너무 길면 줄임
            if len(title) > 50:
                title = title[:47] + "..."
                base_text = f"""{title}

대리 티켓팅 진행
성공경력 다수

선착순 할인: VIP 수고비 5만원
실패시 전액환불

🕐 {open_time}

상담: https://open.kakao.com/o/sAJ8m2Ah

{hashtags}"""
        
        return base_text[:280]
    
    def post_tweet(self, ticket_info: Dict[str, Any], image_path: str = None) -> tuple[bool, str, str]:
        """트윗 게시"""
        if not self._can_post_now():
            return False, "게시 제한", ""
        
        try:
            # 트윗 텍스트 생성
            tweet_text = self._create_tweet_text(ticket_info)
            
            media_ids = []
            
            # 이미지가 있으면 업로드
            if image_path and os.path.exists(image_path):
                try:
                    media = self.api.media_upload(image_path)
                    media_ids.append(media.media_id)
                    logger.info(f"이미지 업로드 성공: {image_path}")
                except Exception as e:
                    logger.warning(f"이미지 업로드 실패: {e}")
            
            # 트윗 게시
            if media_ids:
                response = self.client.create_tweet(text=tweet_text, media_ids=media_ids)
            else:
                response = self.client.create_tweet(text=tweet_text)
            
            # 게시 기록 업데이트
            now = datetime.now()
            self.last_post_time = now
            self.posts_in_15min.append(now)
            self.posts_today += 1
            
            tweet_url = f"https://twitter.com/gamsahanticket/status/{response.data['id']}"
            logger.info(f"트윗 게시 성공: {response.data['id']}")
            
            return True, "게시 완료", tweet_url
            
        except tweepy.Forbidden as e:
            if "duplicate content" in str(e).lower():
                return False, "중복 콘텐츠", ""
            else:
                return False, f"권한 오류: {str(e)}", ""
        except Exception as e:
            return False, f"게시 실패: {str(e)}", ""

class PostingScheduler:
    """게시 스케줄러"""
    
    def __init__(self, sheets_manager: GoogleSheetsManager, twitter_bot: TwitterBot):
        self.sheets_manager = sheets_manager
        self.twitter_bot = twitter_bot
        self.config = PostingConfig()
        self.is_running = False
    
    def start_scheduler(self):
        """스케줄러 시작"""
        self.is_running = True
        logger.info("게시 스케줄러 시작")
        
        # 3분마다 게시 대기열 확인
        schedule.every(3).minutes.do(self.check_and_post)
        
        # 별도 스레드에서 스케줄러 실행
        def run_scheduler():
            while self.is_running:
                schedule.run_pending()
                time.sleep(30)  # 30초마다 체크
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        
        return scheduler_thread
    
    def stop_scheduler(self):
        """스케줄러 중지"""
        self.is_running = False
        schedule.clear()
        logger.info("게시 스케줄러 중지")
    
    def check_and_post(self):
        """게시 대기열 확인 및 게시 실행"""
        try:
            # 게시 가능한지 확인
            if not self.twitter_bot._can_post_now():
                return
            
            # 대기 중인 게시물 조회
            pending_posts = self.sheets_manager.get_pending_posts(limit=1)
            
            if not pending_posts:
                return
            
            post = pending_posts[0]
            ticket_id = post.get('티켓ID')
            
            # 티켓 정보 구성
            ticket_info = {
                '제목': post.get('제목', ''),
                '예매코드': post.get('예매코드', ''),
                '오픈시간': post.get('오픈시간', ''),
                '장르': post.get('장르', ''),
                '조회수': post.get('조회수', 0)
            }
            
            image_path = post.get('이미지경로', '')
            
            # 트윗 게시 시도
            success, message, tweet_url = self.twitter_bot.post_tweet(ticket_info, image_path)
            
            if success:
                self.sheets_manager.update_posting_status(ticket_id, '완료', tweet_url)
                logger.info(f"게시 완료: {ticket_info['제목']}")
            else:
                # 재시도 횟수 확인
                retry_count = int(post.get('재시도횟수', 0))
                if retry_count < 3 and message != "중복 콘텐츠":
                    self.sheets_manager.update_posting_status(ticket_id, '재시도', '', message)
                    logger.warning(f"게시 실패, 재시도 예정: {message}")
                else:
                    self.sheets_manager.update_posting_status(ticket_id, '실패', '', message)
                    logger.error(f"게시 최종 실패: {message}")
        
        except Exception as e:
            logger.error(f"스케줄러 실행 오류: {e}")

class InterparkTicketCrawler:
    """인터파크 HOT 티켓 크롤러 (스마트 게시 시스템 통합)"""
    
    def __init__(self, credentials_path: str = 'google.json', 
                 spreadsheet_name: str = '감사한 티켓팅 신청서', 
                 image_folder: str = 'Image', 
                 enable_auto_posting: bool = True):
        
        self.credentials_path = credentials_path
        self.spreadsheet_name = spreadsheet_name
        self.image_folder = Path(image_folder)
        self.base_url = "https://tickets.interpark.com/contents/api/open-notice/notice-list"
        
        self.downloaded_urls: Set[str] = set()
        self.downloaded_hashes: Set[str] = set()
        self.enable_auto_posting = enable_auto_posting
        
        # 컴포넌트 초기화
        self.sheets_manager = None
        self.twitter_bot = None
        self.scheduler = None
        
        self._setup_directories()
        self._setup_components()
        self._load_existing_images()
        
        if self.enable_auto_posting:
            self._start_scheduler()
    
    def _setup_directories(self):
        """이미지 저장 폴더 생성"""
        try:
            self.image_folder.mkdir(exist_ok=True)
            logger.info(f"이미지 폴더 준비 완료: {self.image_folder}")
        except Exception as e:
            logger.error(f"이미지 폴더 생성 실패: {e}")
            raise
    
    def _setup_components(self):
        """시스템 컴포넌트 초기화"""
        try:
            self.sheets_manager = GoogleSheetsManager(self.credentials_path, self.spreadsheet_name)
            
            if self.enable_auto_posting:
                self.twitter_bot = TwitterBot()
                self.scheduler = PostingScheduler(self.sheets_manager, self.twitter_bot)
            
            logger.info("시스템 컴포넌트 초기화 완료")
            
        except Exception as e:
            logger.error(f"컴포넌트 초기화 실패: {e}")
            raise
    
    def _start_scheduler(self):
        """스케줄러 시작"""
        if self.scheduler:
            self.scheduler.start_scheduler()
            logger.info("자동 게시 스케줄러 시작됨")
    
    def _load_existing_images(self):
        """기존 이미지 파일들의 해시값 로드"""
        try:
            for image_file in self.image_folder.glob('*'):
                if image_file.is_file() and image_file.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                    try:
                        with open(image_file, 'rb') as f:
                            content = f.read()
                            image_hash = hashlib.md5(content).hexdigest()
                            self.downloaded_hashes.add(image_hash)
                    except Exception as e:
                        logger.warning(f"기존 이미지 해시 계산 실패 {image_file}: {e}")
            logger.info(f"기존 이미지 {len(self.downloaded_hashes)}개의 해시값 로드 완료")
        except Exception as e:
            logger.warning(f"기존 이미지 로드 실패: {e}")
    
    def _get_request_headers(self) -> Dict[str, str]:
        """API 요청 헤더 반환"""
        return {
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
    
    def _get_request_params(self) -> Dict[str, Any]:
        """API 요청 파라미터 반환"""
        return {
            "goodsGenre": "ALL", 
            "goodsRegion": "ALL",
            "offset": 0,
            "pageSize": 50,
            "sorting": "OPEN_ASC"
        }
    
    def fetch_ticket_data(self) -> List[Dict[str, Any]]:
        """인터파크 API에서 티켓 데이터 가져오기"""
        try:
            response = requests.get(
                self.base_url, 
                params=self._get_request_params(), 
                headers=self._get_request_headers()
            )
            response.raise_for_status()
            logger.info("티켓 데이터 API 호출 성공")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API 호출 실패: {e}")
            raise
    
    def _get_image_extension(self, url: str) -> str:
        """URL에서 이미지 확장자 추출"""
        try:
            parsed_url = urlparse(url)
            path = parsed_url.path.lower()
            if path.endswith(('.jpg', '.jpeg')):
                return '.jpg'
            elif path.endswith('.png'):
                return '.png'
            elif path.endswith('.gif'):
                return '.gif'
            elif path.endswith('.webp'):
                return '.webp'
            else:
                return '.jpg'
        except:
            return '.jpg'
    
    def _generate_url_hash(self, url: str) -> str:
        """URL을 기반으로 고유한 해시 생성"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()[:12]
    
    def _generate_filename(self, goods_code: str, image_url: str, title: str = "", index: int = 0) -> str:
        """파일명 생성"""
        extension = self._get_image_extension(image_url)
        
        if goods_code and goods_code.strip():
            safe_code = "".join(c for c in goods_code if c.isalnum() or c in ('-', '_'))
            return f"{safe_code}{extension}"
        else:
            url_hash = self._generate_url_hash(image_url)
            safe_title = ""
            if title:
                safe_title = "".join(c for c in title if c.isalnum() or c in ('-', '_', ' '))
                safe_title = safe_title.replace(' ', '_')[:20]
            
            if safe_title:
                return f"ticket_{safe_title}_{url_hash}{extension}"
            else:
                return f"ticket_{url_hash}_{index:03d}{extension}"
    
    def _is_duplicate_image(self, image_content: bytes) -> bool:
        """이미지 내용이 중복인지 확인"""
        image_hash = hashlib.md5(image_content).hexdigest()
        if image_hash in self.downloaded_hashes:
            return True
        self.downloaded_hashes.add(image_hash)
        return False
    
    def download_image(self, image_url: str, goods_code: str, title: str = "", index: int = 0) -> str:
        """이미지 다운로드"""
        if not image_url or not image_url.strip():
            return ""
        
        if image_url in self.downloaded_urls:
            logger.info(f"이미 처리된 URL, 스킵: {image_url}")
            return ""
        
        filename = self._generate_filename(goods_code, image_url, title, index)
        file_path = self.image_folder / filename
        
        if file_path.exists():
            logger.info(f"이미지 이미 존재, 스킵: {filename}")
            self.downloaded_urls.add(image_url)
            return str(file_path)
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(image_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            if self._is_duplicate_image(response.content):
                logger.info(f"중복 이미지 감지, 다운로드 스킵: {image_url}")
                self.downloaded_urls.add(image_url)
                return ""
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            self.downloaded_urls.add(image_url)
            logger.info(f"이미지 다운로드 완료: {filename}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"이미지 다운로드 실패 ({image_url}): {e}")
            return ""
    
    def filter_hot_tickets(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """HOT 티켓만 필터링하여 필요한 정보 추출"""
        hot_tickets = []
        
        for index, ticket in enumerate(raw_data):
            if not ticket.get('isHot', False):
                continue
            
            goods_code = ticket.get('goodsCode', '')
            image_url = ticket.get('posterImageUrl', '')
            title = ticket.get('title', '')
            
            local_image_path = ""
            if image_url:
                local_image_path = self.download_image(image_url, goods_code, title, index)
                
            ticket_info = {
                '오픈시간': ticket.get('openDateStr', ''),
                '조회수': ticket.get('viewCount', 0),
                '예매타입': ticket.get('openTypeStr', ''),
                '제목': title,
                '예매코드': goods_code,
                '멀티오픈': ticket.get('hasMultipleOpenDates', False),
                '장르': ticket.get('goodsGenreStr', ''),
                '지역': ticket.get('goodsRegionStr', ''),
                '공연장': ticket.get('venueName', ''),
                'Image': image_url,
                'LocalImage': local_image_path
            }
            hot_tickets.append(ticket_info)
        
        logger.info(f"HOT 티켓 {len(hot_tickets)}개 필터링 완료")
        return hot_tickets
    
    def run(self) -> Dict[str, Any]:
        """크롤링 및 자동 게시 시스템 실행"""
        logger.info("스마트 티켓 크롤링 및 자동 게시 시스템 시작")
        
        try:
            # 1. 티켓 데이터 수집
            raw_data = self.fetch_ticket_data()
            hot_tickets = self.filter_hot_tickets(raw_data)
            
            if not hot_tickets:
                logger.info("HOT 티켓이 없습니다.")
                return {'new_tickets': 0, 'queued_posts': 0}
            
            # 2. 기존 Hot 시트 업데이트 (기존 로직 유지)
            df = pd.DataFrame(hot_tickets)
            existing_tickets_df = self._get_existing_tickets_data()
            
            # 3. 새로운 티켓 식별
            new_tickets = self._identify_new_tickets(df, existing_tickets_df)
            
            # 4. 새로운 티켓들을 게시 대기열에 추가
            queued_count = 0
            if not new_tickets.empty and self.enable_auto_posting:
                for _, ticket in new_tickets.iterrows():
                    local_image_path = ticket.get('LocalImage', '')
                    if self.sheets_manager.add_to_posting_queue(ticket.to_dict(), local_image_path):
                        queued_count += 1
                
                logger.info(f"게시 대기열에 {queued_count}개 티켓 추가")
            
            # 5. Hot 시트 업데이트
            self._update_hot_sheet(df, existing_tickets_df)
            
            result = {
                'total_hot_tickets': len(hot_tickets),
                'new_tickets': len(new_tickets) if not new_tickets.empty else 0,
                'queued_posts': queued_count,
                'scheduler_running': self.scheduler.is_running if self.scheduler else False
            }
            
            logger.info(f"크롤링 완료: {result}")
            return result
            
        except Exception as e:
            logger.error(f"크롤링 실행 중 오류: {e}")
            raise
    
    def _get_existing_tickets_data(self) -> pd.DataFrame:
        """구글 시트에서 기존 티켓 데이터 가져오기"""
        try:
            existing_data = self.sheets_manager.hot_worksheet.get_all_records()
            if existing_data:
                df = pd.DataFrame(existing_data)
                logger.info(f"기존 티켓 {len(df)}개 확인")
                return df
            else:
                logger.info("기존 데이터가 없습니다.")
                return pd.DataFrame()
        except Exception as e:
            logger.warning(f"기존 데이터 조회 실패: {e}")
            return pd.DataFrame()
    
    def _identify_new_tickets(self, new_tickets_df: pd.DataFrame, existing_tickets_df: pd.DataFrame) -> pd.DataFrame:
        """새로운 티켓 식별"""
        if existing_tickets_df.empty:
            return new_tickets_df
        
        # 기존 티켓의 고유 키 생성
        existing_keys = existing_tickets_df.apply(
            lambda row: f"{row.get('오픈시간', '')}|{row.get('제목', '')}|{row.get('예매코드', '')}", 
            axis=1
        ).tolist()
        
        # 새 티켓의 고유 키 생성 후 중복 확인
        new_tickets_df['_temp_key'] = new_tickets_df.apply(
            lambda row: f"{row['오픈시간']}|{row['제목']}|{row['예매코드']}", 
            axis=1
        )
        
        new_tickets = new_tickets_df[~new_tickets_df['_temp_key'].isin(existing_keys)].copy()
        new_tickets = new_tickets.drop(columns=['_temp_key'])
        
        logger.info(f"새로운 티켓 {len(new_tickets)}개 발견")
        return new_tickets
    
    def _update_hot_sheet(self, df: pd.DataFrame, existing_tickets_df: pd.DataFrame):
        """Hot 시트 업데이트 (기존 로직)"""
        try:
            # LocalImage 컬럼 제거 후 시트에 추가
            sheet_df = df.drop('LocalImage', axis=1) if 'LocalImage' in df.columns else df
            
            # 시트 초기화 (필요시)
            if existing_tickets_df.empty:
                self.sheets_manager.hot_worksheet.append_row(list(sheet_df.columns))
            
            # 새로운 티켓들만 추가
            new_tickets = self._identify_new_tickets(df, existing_tickets_df)
            if not new_tickets.empty:
                new_sheet_tickets = new_tickets.drop('LocalImage', axis=1) if 'LocalImage' in new_tickets.columns else new_tickets
                for _, row in new_sheet_tickets.iterrows():
                    self.sheets_manager.hot_worksheet.append_row(row.tolist())
                
                logger.info(f"Hot 시트에 {len(new_tickets)}개 새로운 티켓 추가")
            
        except Exception as e:
            logger.error(f"Hot 시트 업데이트 실패: {e}")
    
    def get_posting_stats(self) -> Dict[str, Any]:
        """게시 통계 조회"""
        try:
            all_records = self.sheets_manager.posting_worksheet.get_all_records()
            
            stats = {
                'total_queued': len(all_records),
                'pending': len([r for r in all_records if r.get('게시상태') == '대기']),
                'completed': len([r for r in all_records if r.get('게시상태') == '완료']),
                'failed': len([r for r in all_records if r.get('게시상태') == '실패']),
                'retry': len([r for r in all_records if r.get('게시상태') == '재시도'])
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"게시 통계 조회 실패: {e}")
            return {}
    
    def stop(self):
        """시스템 종료"""
        if self.scheduler:
            self.scheduler.stop_scheduler()
        logger.info("스마트 티켓 크롤링 시스템 종료")

def main():
    """메인 실행 함수"""
    try:
        print("🎫 스마트 티켓 크롤링 및 자동 게시 시스템 시작\n")
        
        # 크롤러 인스턴스 생성
        crawler = InterparkTicketCrawler(
            image_folder='Image',
            enable_auto_posting=True  # 자동 게시 활성화
        )
        
        print("📊 시스템 상태:")
        print(f"- 자동 게시: {'활성화' if crawler.enable_auto_posting else '비활성화'}")
        print(f"- 스케줄러: {'실행 중' if crawler.scheduler and crawler.scheduler.is_running else '중지'}")
        print()
        
        # 크롤링 실행
        result = crawler.run()
        
        # 결과 출력
        print("="*60)
        print("📋 실행 결과:")
        print(f"🎫 전체 HOT 티켓: {result['total_hot_tickets']}개")
        print(f"🆕 새로운 티켓: {result['new_tickets']}개")
        print(f"📝 게시 대기열 추가: {result['queued_posts']}개")
        print(f"🤖 스케줄러 상태: {'실행 중' if result['scheduler_running'] else '중지'}")
        
        # 게시 통계 출력
        if crawler.enable_auto_posting:
            stats = crawler.get_posting_stats()
            if stats:
                print(f"\n📊 게시 통계:")
                print(f"- 전체 대기열: {stats['total_queued']}개")
                print(f"- 게시 대기: {stats['pending']}개")
                print(f"- 게시 완료: {stats['completed']}개")
                print(f"- 게시 실패: {stats['failed']}개")
                print(f"- 재시도 중: {stats['retry']}개")
        
        print("\n" + "="*60)
        
        if crawler.enable_auto_posting:
            print("🚀 자동 게시 시스템이 백그라운드에서 실행 중입니다.")
            print("📊 게시 상태는 구글 시트 'PostingQueue' 탭에서 확인하세요.")
            print()
            
            # 사용자 선택 메뉴
            while True:
                print("📋 메뉴 선택:")
                print("1. 현재 통계 확인")
                print("2. 수동 크롤링 재실행")
                print("3. 스케줄러 중지")
                print("4. 프로그램 종료")
                
                choice = input("\n선택하세요 (1-4): ").strip()
                
                if choice == '1':
                    stats = crawler.get_posting_stats()
                    print(f"\n📊 현재 게시 통계:")
                    print(f"- 게시 대기: {stats['pending']}개")
                    print(f"- 게시 완료: {stats['completed']}개")
                    print(f"- 게시 실패: {stats['failed']}개")
                    print()
                    
                elif choice == '2':
                    print("🔄 수동 크롤링 실행 중...")
                    result = crawler.run()
                    print(f"✅ 완료: 새로운 티켓 {result['new_tickets']}개 발견")
                    print()
                    
                elif choice == '3':
                    crawler.scheduler.stop_scheduler()
                    print("⏹️ 스케줄러가 중지되었습니다.")
                    print()
                    
                elif choice == '4':
                    break
                    
                else:
                    print("❌ 잘못된 선택입니다.")
                    print()
        
        # 시스템 종료
        crawler.stop()
        print("👋 프로그램을 종료합니다.")
            
    except KeyboardInterrupt:
        print("\n\n⏹️ 사용자에 의해 중단되었습니다.")
        try:
            crawler.stop()
        except:
            pass
    except Exception as e:
        logger.error(f"시스템 실행 중 오류 발생: {e}")
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()