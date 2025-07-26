import os
import json
import requests
import pandas as pd
from pathlib import Path
from openai import OpenAI
from datetime import datetime
from tqdm import tqdm
import random
from time import sleep
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import dotenv
import schedule
import time

# 환경변수 로드
dotenv.load_dotenv()

class InterparkTicketCrawler:
    def __init__(self, creds='google.json', sheet_name='감사한 티켓팅 신청서'):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds, scope)
        self.sheet = gspread.authorize(creds).open(sheet_name).worksheet('Hot')

        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        self.cache_paths = {
            "artist": Path('artist_cache.json'),
            "hashtag": Path('hashtag_cache.json'),
            "tweet": Path('tweet_cache.json')
        }
        self.cache = {k: self._load_cache(p) for k, p in self.cache_paths.items()}

    def _load_cache(self, path):
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def _save_cache(self, name):
        with open(self.cache_paths[name], 'w', encoding='utf-8') as f:
            json.dump(self.cache[name], f, ensure_ascii=False, indent=2)

    def fetch_data(self):
        url = "https://tickets.interpark.com/contents/api/open-notice/notice-list"
        params = {"goodsGenre": "ALL", "goodsRegion": "ALL", "offset": 0, "pageSize": 400, "sorting": "OPEN_ASC"}
        headers = {"user-agent": "Mozilla/5.0", "referer": "https://tickets.interpark.com/contents/notice"}
        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.json()

    def filter_hot(self, data):
        limits = {'콘서트': 600, '뮤지컬': 500, '연극': 500, '클래식/오페라': 400}
        return [{
            '오픈시간': d.get('openDateStr', ''),
            '조회수': d.get('viewCount', 0),
            '예매타입': d.get('openTypeStr', ''),
            '제목': d.get('title', ''),
            '예매코드': d.get('goodsCode', ''),
            '장르': d.get('goodsGenreStr', ''),
            'Image': d.get('posterImageUrl', '')
        } for d in data if d.get('viewCount', 0) > limits.get(d.get('goodsGenreStr', ''), 10000)]

    def extract_artist(self, title):
        if title in self.cache["artist"]:
            return self.cache["artist"][title]
        prompt = f"제목: {title}\n\n위 공연 제목에서 핵심 아티스트명이나 작품명만 간단히 추출해주세요. 15자 이내로 답변해주세요. 설명은 하지 말고 이름만 답변하세요."
        try:
            res = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
            )
            artist = res.choices[0].message.content.strip().strip('"')
            # 15자 제한
            if len(artist) > 15:
                artist = artist[:15]
            self.cache["artist"][title] = artist
            return artist
        except Exception as e:
            print(f"❌ OpenAI 오류 (가수명): {e}")
            return "불명"

    def generate_hashtags(self, title, artist, genre):
        if title in self.cache["hashtag"]:
            return self.cache["hashtag"][title]
        prompt = f"콘서트 제목: {title}\n가수 또는 뮤지컬 제목: {artist}\n장르: {genre}\n해시태그 10개를 한국어로 작성. '#' 포함, 한 줄로, 콤마 없이, 9자 이내 키워드:"
        try:
            res = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
            )
            hashtags = res.choices[0].message.content.strip()
            self.cache["hashtag"][title] = hashtags
            return hashtags
        except Exception as e:
            print(f"❌ OpenAI 오류 (해시태그): {e}")
            return "#대리티켓팅"

    def format_time(self, raw_time):
        try:
            dt = datetime.strptime(raw_time, '%Y-%m-%d %H:%M:%S') if isinstance(raw_time, str) else raw_time
            return dt.strftime('%m월 %d일 %p %I시').replace('AM', '오전').replace('PM', '오후').replace('0', '')
        except:
            return ""

    def add_columns(self, df):
        print("🤖 데이터 생성 중...")
        artists, hashtags, tweets, bunjangs = [], [], [], []
        for _, row in tqdm(df.iterrows(), total=len(df)):
            title, genre, raw_time = row['제목'], row['장르'], row['오픈시간']
            artist = self.extract_artist(title)
            hashtag = self.generate_hashtags(title, artist, genre)
            open_time = self.format_time(raw_time)

            tweet = f"{title}\n\n🚨 {artist} 대리티켓팅(댈티)\n\n수고비 제일 저렴\n경력 매우 많음\n\n상담 링크: https://open.kakao.com/o/sAJ8m2Ah\n\n{hashtag}"
            bunjang = f"{title}\n\n🚨 {artist} 대리티켓팅(댈티)\n\n수고비 제일 저렴\n경력 매우 많음\n\n가격: 번개톡 상담\n\n{hashtag}"

            artists.append(artist)
            hashtags.append(hashtag)
            tweets.append(tweet)
            bunjangs.append(bunjang)

        df['가수명'] = artists
        df['해시태그'] = hashtags
        df['트위터'] = tweets
        df['번장'] = bunjangs
        self._save_cache("artist")
        self._save_cache("hashtag")
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
        data = self.fetch_data()
        hot_list = self.filter_hot(data)
        df = pd.DataFrame(hot_list)
        df = df[df['오픈시간'].notna() & (df['오픈시간'] != '')]
        if df.empty:
            return df
        df = df.sort_values(by='오픈시간')
        df = self.add_columns(df)
        self.update_sheet(df)
        return df


class PostBunjang:
    def __init__(self, auth_token="53a119a23abe4baa83d75e604dbc2a2d"):
        self.auth_token = auth_token
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
        try:
            # SSL 인증서 검증 우회
            r = requests.get(url, stream=True, verify=False)
            if r.status_code == 200:
                with open(path, "wb") as f:
                    for chunk in r.iter_content(1024):
                        f.write(chunk)
                print(f"✅ 이미지 다운로드 완료: {path}")
                return path
            print(f"❌ 이미지 다운로드 실패: {url} (상태코드: {r.status_code})")
            return None
        except Exception as e:
            print(f"❌ 이미지 다운로드 오류: {url} - {e}")
            return None

    def register_bunjang_product(self, image_path, name, description, keywords, price):
        print(f"🚀 번장 상품 등록 시작: {name}")
        print(f"📁 이미지 경로: {image_path}")
        print(f"💰 가격: {price}")
        
        upload_url = 'https://media-center.bunjang.co.kr/upload/79373298/product'
        product_url = 'https://api.bunjang.co.kr/api/pms/v2/products'

        if not Path(image_path).exists():
            print(f"❌ 이미지 경로가 존재하지 않습니다: {image_path}")
            return None

        try:
            with open(image_path, 'rb') as img_file:
                files = {'file': ('upload.jpg', img_file, 'image/jpeg')}
                upload_res = requests.post(upload_url, headers={'referer': 'https://m.bunjang.co.kr/'}, files=files)

            if upload_res.status_code != 200:
                print(f"❌ 이미지 업로드 실패: 상태코드 {upload_res.status_code}")
                print(f"❌ 이미지 업로드 응답: {upload_res.text}")
                return None

            upload_json = upload_res.json()
            print(f"✅ 이미지 업로드 응답: {upload_json}")
            image_id = upload_json.get('image_id')
            keywords = [k.strip() for k in keywords.split('#') if k.strip()] if isinstance(keywords, str) else keywords
            # 키워드를 5개로 제한
            keywords = keywords[:5]
            print(f"🏷️ 처리된 키워드: {keywords}")
            data = {
                "categoryId": "900210001",
                "common": {
                    "description": description,
                    "keywords": keywords,
                    "name": name,
                    "condition": "UNDEFINED",
                    "priceOfferEnabled": True
                },
                "option": [], "location": {"geo": self.location},
                "transaction": {
                    "quantity": 1,
                    "price": price,
                    "trade": {"freeShipping": True, "isDefaultShippingFee": False, "inPerson": True}
                },
                "media": [{"imageId": image_id}],
                "naverShoppingData": {"isEnabled": False}
            }

            res = requests.post(product_url, headers={'x-bun-auth-token': self.auth_token}, json=data)
            if res.status_code != 200:
                print(f"❌ 제품 등록 실패: 상태코드 {res.status_code}")
                print(f"❌ 응답 내용: {res.text}")
                print(f"❌ 요청 데이터: {json.dumps(data, ensure_ascii=False, indent=2)}")
                return None

            return res.json().get("data", {}).get("pid")
        except Exception as e:
            print(f"❌ 예외 발생: {e}")
            return None

    def post(self, image_url, title, text, hash_tag, price):
        path = self._download_image(image_url)
        if not path:
            return
        pid = self.register_bunjang_product(path, title, text, hash_tag, price)
        if pid:
            print(f"🔗 번장 링크: https://m.bunjang.co.kr/products/{pid}")


def run_ticket_crawling():
    """티켓 크롤링 및 번장 게시 실행"""
    print(f"🚀 티켓 크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    df = InterparkTicketCrawler().run()
    if df is not None and not df.empty:
        for _, row in tqdm(df.iterrows(), total=len(df)):
            title = f"{row['가수명']} 대리티켓팅(댈티)"
            # 번장 상품명은 40자 제한
            if len(title) > 40:
                title = title[:37] + "..."
            PostBunjang().post(
                image_url=row['Image'],
                title=title,
                text=row['번장'],
                hash_tag=row['해시태그'],
                price=9999
            )
            wait = random.randint(60, 90)
            for sec in range(wait, 0, -1):
                print(f"\r⏰ 다음 게시까지 {sec}초 남음...", end="", flush=True)
                sleep(1)
            print(f"\n⏳ {wait}초 대기 완료")
        print(f"✅ 티켓 크롤링 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("❌ 처리할 티켓이 없습니다.")

def main():
    """메인 실행 함수 - 스케줄러 설정"""
    print("🕐 티켓 크롤링 스케줄러 시작")
    print("📅 실행 시간: 오전 9시, 12시 / 오후 6시, 9시")
    
    # 스케줄 설정
    schedule.every().day.at("09:15").do(run_ticket_crawling)
    schedule.every().day.at("12:15").do(run_ticket_crawling)
    schedule.every().day.at("18:15").do(run_ticket_crawling)
    schedule.every().day.at("21:15").do(run_ticket_crawling)
    
    print("⏰ 스케줄러가 실행 중입니다... (Ctrl+C로 종료)")
    
    # 스케줄러 실행
    while True:
        schedule.run_pending()
        time.sleep(60)  # 1분마다 체크

if __name__ == "__main__":
    # 즉시 실행 옵션 (테스트용)
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        print("🚀 즉시 실행 모드")
        run_ticket_crawling()
    else:
        main()
