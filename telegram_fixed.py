import os
import json
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import random
from datetime import datetime

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

class InterparkTicketCrawler:
    def __init__(self, creds='google.json', sheet_name='감사한 티켓팅 신청서'):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds, scope)
        self.sheet = gspread.authorize(creds).open(sheet_name).worksheet('Hot')

        # 캐시 파일 경로
        self.artist_cache_path = Path('artist_cache.json')
        self.hashtag_cache_path = Path('hashtag_cache.json')
        self.tweet_cache_path = Path('tweet_cache.json')

        # 캐시 로딩
        self.artist_cache = self.load_cache(self.artist_cache_path)
        self.hashtag_cache = self.load_cache(self.hashtag_cache_path)
        self.tweet_cache = self.load_cache(self.tweet_cache_path)
        
    def load_cache(self, path: Path) -> dict:
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def save_cache(self, cache: dict, path: Path):
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    def fetch_data(self):
        url = "https://tickets.interpark.com/contents/api/open-notice/notice-list"
        params = {"goodsGenre": "ALL", "goodsRegion": "ALL", "offset": 0, "pageSize": 400, "sorting": "OPEN_ASC"}
        headers = {
            "user-agent": "Mozilla/5.0",
            "referer": "https://tickets.interpark.com/contents/notice"
        }
        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.json()

    # 뮤지컬, 연극 500 , 클래식/오페라 400, 콘서트 600
    def filter_hot(self, data):
        hot = []
        for d in data:
            if d.get('goodsGenreStr') == '콘서트' and d.get('viewCount', 0) <= 600:
                continue
            if d.get('goodsGenreStr') == '뮤지컬' and d.get('viewCount', 0) <= 500:
                continue
            if d.get('goodsGenreStr') == '연극' and d.get('viewCount', 0) <= 500:
                continue
            if d.get('goodsGenreStr') == '클래식/오페라' and d.get('viewCount', 0) <= 400:
                continue
            
            hot.append({
                '오픈시간': d.get('openDateStr', ''),
                '조회수': d.get('viewCount', 0),
                '예매타입': d.get('openTypeStr', ''),
                '제목': d.get('title', ''),
                '예매코드': d.get('goodsCode', ''),
                '장르': d.get('goodsGenreStr', ''),
                'Image': d.get('posterImageUrl', '')
            })
        return hot

    def extract_artist(self, title: str) -> str:
        if title in self.artist_cache:
            return self.artist_cache[title]

        prompt = f"""
아래는 콘서트 제목이야. 여기서 가수명이나 그룹명만 간단히 추출해줘. 뮤지컬일 경우 뮤지컬 제목만 추출해줘.**영문일 경우 한글도 같이 작성해야되고, 약어가 있으면 풀네임이랑 약어도 같이 작성해야해**
예시: 악동뮤지션 (악뮤, AKMU)
제목: {title}
가수명 or 뮤지컬 제목:"""

        try:
            res = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            artist = res.choices[0].message.content.strip().strip('"')
            self.artist_cache[title] = artist
            return artist
        except Exception as e:
            print(f"❌ OpenAI 오류 (가수명): {e}")
            return "불명"

    def generate_hashtags(self, title: str, artist: str, genre: str) -> str:
        key = f"{title}"
        if key in self.hashtag_cache:
            return self.hashtag_cache[key]

        prompt = f"""
콘서트 제목: {title}
가수 또는 뮤지컬 제목: {artist}
장르: {genre}

위 콘서트를 대리티켓팅 목적으로 트위터에 해시태그 10개를 한국어로 작성해줘.
형식: #블랙핑크콘서트 #블랙핑크 #BLACKPINK #블핑댈티 #대리티켓팅
조건: '#' 포함하고 띄어쓰기 없이, 한 줄로 콤마 없이 출력해줘.
"""

        try:
            res = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
            )
            hashtags = res.choices[0].message.content.strip()
            self.hashtag_cache[key] = hashtags
            return hashtags
        except Exception as e:
            print(f"❌ OpenAI 오류 (해시태그): {e}")
            return "#대리티켓팅"

    def add_ai_columns(self, df):
        print("🤖 가수명 + 해시태그 생성 중...")
        artists = []
        hashtags = []

        for _, row in tqdm(df.iterrows(), total=len(df)):
            title = row['제목']
            genre = row['장르']

            artist = self.extract_artist(title)
            hashtag = self.generate_hashtags(title, artist, genre)

            artists.append(artist)
            hashtags.append(hashtag)

        df['가수명'] = artists
        df['해시태그'] = hashtags

        self.save_cache(self.artist_cache, self.artist_cache_path)
        self.save_cache(self.hashtag_cache, self.hashtag_cache_path)

        return df
    
    def add_twitter_columns(self, df):
        print("🤖 트위터 문구 생성 중...")
        with open('tweet_templates.json', 'r', encoding='utf-8') as f:
            templates = json.load(f)
        
        tweet_contents = []
        for _, row in tqdm(df.iterrows(), total=len(df)):
            template = {"content": "{title}\n\n🚨 {singer} 대리티켓팅(댈티)\n\n수고비 제일 저렴\n경력 매우 많음\n\n상담 링크: https://open.kakao.com/o/sAJ8m2Ah\n\n{hash_tag}"}
            
            title = row['제목']
            singer = row['가수명']
            
            # 오픈시간 처리 (빈 문자열이나 None 값 처리)
            open_time_raw = row['오픈시간']
            if isinstance(open_time_raw, str) and open_time_raw.strip():
                try:
                    # 문자열을 datetime으로 변환
                    open_time_dt = datetime.strptime(open_time_raw, '%Y-%m-%d %H:%M:%S')
                    open_time = open_time_dt.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
                except ValueError:
                    # 날짜 형식이 잘못된 경우 기본값 사용
                    open_time = "미정"
            elif hasattr(open_time_raw, 'strftime'):
                # 이미 datetime 객체인 경우
                open_time = open_time_raw.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
            else:
                # 빈 문자열이나 None인 경우 기본값 사용
                open_time = "미정"
            
            hash_tag = row['해시태그']
            content = template['content'].replace("{open_time}", open_time).replace("{title}", title).replace("{singer}", singer).replace("{hash_tag}", hash_tag)
            tweet_contents.append(content)

        df['트위터'] = tweet_contents
        self.save_cache(self.tweet_cache, self.tweet_cache_path)
        return df
        
    def bunjang_columns(self, df):
        print("🤖 번장 문구 생성 중...")
        
        bunjang_contents = []
        for _, row in tqdm(df.iterrows(), total=len(df)):
            template = {"content": "{title}\n\n🚨 {singer} 대리티켓팅(댈티)\n\n수고비 제일 저렴\n경력 매우 많음\n\n가격: 번개톡 상담\n\n{hash_tag}"}
            
            title = row['제목']
            singer = row['가수명']
            
            # 오픈시간 처리 (빈 문자열이나 None 값 처리)
            open_time_raw = row['오픈시간']
            if isinstance(open_time_raw, str) and open_time_raw.strip():
                try:
                    # 문자열을 datetime으로 변환
                    open_time_dt = datetime.strptime(open_time_raw, '%Y-%m-%d %H:%M:%S')
                    open_time = open_time_dt.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
                except ValueError:
                    # 날짜 형식이 잘못된 경우 기본값 사용
                    open_time = "미정"
            elif hasattr(open_time_raw, 'strftime'):
                # 이미 datetime 객체인 경우
                open_time = open_time_raw.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
            else:
                # 빈 문자열이나 None인 경우 기본값 사용
                open_time = "미정"
            
            hash_tag = row['해시태그']
            content = template['content'].replace("{open_time}", open_time).replace("{title}", title).replace("{singer}", singer).replace("{hash_tag}", hash_tag)
            bunjang_contents.append(content)

        df['번장'] = bunjang_contents
        
        return df

    def update_sheet(self, df):
        self.sheet.clear()
        if df.empty:
            print("📭 HOT 티켓 없음")
            return
        self.sheet.append_row(list(df.columns))
        for row in df.values.tolist():
            self.sheet.append_row(row)
        print(f"✅ {len(df)}개 티켓 업로드 완료")

    def run(self):
        raw = self.fetch_data()
        hot = self.filter_hot(raw)
        df = pd.DataFrame(hot)
        if df.empty:
            return df
        df = df.sort_values(by='오픈시간')
        df = self.add_ai_columns(df)
        df = self.add_twitter_columns(df)
        df = self.bunjang_columns(df)
        self.update_sheet(df)
        return df

if __name__ == "__main__":
    df = InterparkTicketCrawler().run()
    if not df.empty:
        print("\n📋 HOT 티켓 요약:")
        print(df[['오픈시간', '제목', '가수명', '해시태그', '번장', '트위터']].to_string(index=False)) 