

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
조건: '#' 포함하고 띄어쓰기 없이, 한 줄로 콤마 없이 출력해줘. 키워드당 9자 이하여야해
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
            
            # if row['조회수'] > 10000:
            #     template = random.choice(templates)
            # else:
            #     template = {"content": "{title}\n\n🚨 {singer} 대리티켓팅(댈티)\n\n수고비 제일 저렴\n경력 매우 많음\n\n상담 링크: https://open.kakao.com/o/sAJ8m2Ah\n\n{hash_tag} #평생한번 #놓치면후회 #앞열보장"}
            
            # 시간 치환
            
            title = row['제목']
            singer = row['가수명']
            
            # 오픈시간이 문자열인 경우 datetime으로 변환
            open_time_raw = row['오픈시간']
            if isinstance(open_time_raw, str):
                # 문자열을 datetime으로 변환
                open_time_dt = datetime.strptime(open_time_raw, '%Y-%m-%d %H:%M:%S')
                open_time = open_time_dt.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
            else:
                # 이미 datetime 객체인 경우
                open_time = open_time_raw.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
            
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
            
            # 오픈시간이 문자열인 경우 datetime으로 변환
            open_time_raw = row['오픈시간']
            if isinstance(open_time_raw, str):
                # 문자열을 datetime으로 변환
                open_time_dt = datetime.strptime(open_time_raw, '%Y-%m-%d %H:%M:%S')
                open_time = open_time_dt.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
            else:
                # 이미 datetime 객체인 경우
                open_time = open_time_raw.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
            
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
        df = df[df['오픈시간'].notna() & (df['오픈시간'] != '')]
        if df.empty:
            return df
        df = df.sort_values(by='오픈시간')
        df = self.add_ai_columns(df)
        df = self.add_twitter_columns(df)
        df = self.bunjang_columns(df)
        self.update_sheet(df)
        return df

df = InterparkTicketCrawler().run()
if not df.empty:
    print("\n📋 HOT 티켓 요약:")
    print(df)
import os
import requests
import json
from pathlib import Path
from time import sleep
import random
from tqdm import tqdm

import os
import requests
import json
from pathlib import Path
from time import sleep
import random
from tqdm import tqdm

class PostBunjang:
    def __init__(self, auth_token=None):
        self.auth_token = "53a119a23abe4baa83d75e604dbc2a2d"
        self.location = {
            "address": "서울특별시 서초구 서초4동",
            "lat": 37.5025863,
            "lon": 127.022219,
            "dongId": 648
        }
        os.makedirs("image", exist_ok=True)

    def _download_image(self, url):
        path = f"image/{url.split('/')[-1]}"
        if os.path.exists(path):
            print(f"📁 이미지 이미 존재: {path}")
            return path
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(path, "wb") as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
            print(f"✅ 이미지 다운로드 완료: {path}")
            return path
        print(f"❌ 이미지 다운로드 실패: {url}")
        return None

    def register_bunjang_product(self, image_path, name, description, keywords, price):
        # 1단계: 이미지 업로드
        upload_url = 'https://media-center.bunjang.co.kr/upload/79373298/product'
        upload_headers = {
            'referer': 'https://m.bunjang.co.kr/',
            'user-agent': 'Mozilla/5.0',
            'origin': 'https://m.bunjang.co.kr',
            'accept': 'application/json, text/plain, */*'
        }

        if not Path(image_path).exists():
            print(f"❌ 이미지 파일 없음: {image_path}")
            return None

        with open(image_path, 'rb') as img_file:
            # ✅ 파일명은 항상 ASCII (latin-1 인코딩 문제 방지)
            files = {'file': ('upload.jpg', img_file, 'image/jpeg')}
            upload_res = requests.post(upload_url, headers=upload_headers, files=files)

        if upload_res.status_code != 200:
            print("❌ 이미지 업로드 실패:", upload_res.text)
            return None

        image_id = upload_res.json().get('image_id')
        print("✅ 이미지 업로드 성공:", image_id)

        # 2단계: 상품 등록
        product_url = 'https://api.bunjang.co.kr/api/pms/v2/products'
        product_headers = {
            'content-type': 'application/json',
            'x-bun-auth-token': self.auth_token,
            'user-agent': 'Mozilla/5.0',
            'origin': 'https://m.bunjang.co.kr',
            'referer': 'https://m.bunjang.co.kr/',
            'accept': 'application/json, text/plain, */*'
        }

        # 해시태그 문자열이면 리스트로 변환
        if isinstance(keywords, str):
            keywords = [k.strip() for k in keywords.split('#') if k.strip()]

        product_data = {
            "categoryId": "900210001",
            "common": {
                "description": description,
                "keywords": keywords,
                "name": name,
                "condition": "UNDEFINED",
                "priceOfferEnabled": True
            },
            "option": [],
            "location": {"geo": self.location},
            "transaction": {
                "quantity": 1,
                "price": price,
                "trade": {
                    "freeShipping": True,
                    "isDefaultShippingFee": False,
                    "inPerson": True
                }
            },
            "media": [{"imageId": image_id}],
            "naverShoppingData": {"isEnabled": False}
        }

        res = requests.post(product_url, headers=product_headers, json=product_data)

        if res.status_code == 200:
            pid = res.json().get("data", {}).get("pid", "N/A")
            print("✅ 상품 등록 성공! 🆔", pid)
            return pid
        else:
            print(f"❌ 상품 등록 실패 ({res.status_code}): {res.text}")
            return None

    def post(self, image_url, title, text, hash_tag, price):
        path = self._download_image(image_url)
        if not path:
            return
        pid = self.register_bunjang_product(
            image_path=path,
            name=title,
            description=text,
            keywords=hash_tag,
            price=price
        )
        if pid:
            print(f"🔗 번장 링크: https://m.bunjang.co.kr/products/{pid}")


# ✅ 예시 실행 (df는 미리 정의된 pandas DataFrame이어야 함)
for _, row in tqdm(df.iterrows(), total=len(df)):
    title = row['가수명'] + " 대리티켓팅(댈티)"
    text = row['번장']
    image_url = row['Image']
    tag_str = row['해시태그']
    hash_tag = [tag.strip().lstrip('#')[:8] for tag in tag_str.split()][:5]

    price = 9999

    PostBunjang().post(image_url, title, text, hash_tag, price)
    print(f"🔄 {title} 번장 게시 완료")

    sleep_time = random.randint(60, 90)
    for remaining in range(sleep_time, 0, -1):
        print(f"\r⏰ 다음 게시까지 {remaining}초 남음...", end="", flush=True)
        sleep(1)
    print(f"\n⏳ {sleep_time}초 대기 완료")