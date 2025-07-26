import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time  
from supabase import create_client, Client

# ──────────────────────────────
# Supabase 설정
# ──────────────────────────────
SUPABASE_URL = "https://zqirsitkvpsmogljceoz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpxaXJzaXRrdnBzbW9nbGpjZW96Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzUwMzA1MjksImV4cCI6MjA1MDYwNjUyOX0.MXrQ-QCiUbtdNlDB-Vbqpyp9jW4ToX6DKIKZ_PCLcwY"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ──────────────────────────────
# 1. 카테고리 정보 수집
# ──────────────────────────────
start = time.time()  # ⏱ 시작 시간 기록

url_categories = "https://www.ticketbay.co.kr/ticketbayApi/content/v1/public/categories"
headers_categories = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
}

res_cat = requests.get(url_categories, headers=headers_categories)
result = res_cat.json()

# 하위 카테고리 (콘서트 종류)
concert_categories = [
    {"id": item["id"], "name": item["name"]}
    for item in result["data"][0]["children"]
]

# ──────────────────────────────
# 2. 요청 함수 정의 (에러 처리 포함)
# ──────────────────────────────
def fetch_products(category):
    url = "https://www.ticketbay.co.kr/ticketbayApi/product/v1/public/products"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {
        "category_id": str(category["id"]),
        "page": "0",
        "size": 10000,
        "offset": 0
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        result = response.json()
        items = result.get("data", {}).get("content", [])

        for item in items:
            item["category_id"] = category["id"]
            item["category_name"] = category["name"]

        print(f"✅ {category['name']} ({category['id']}) → {len(items)}개 수집됨")
        return items

    except Exception as e:
        print(f"❌ {category['name']} ({category['id']}) 오류: {e}")
        return []


# ──────────────────────────────
# 3. 병렬 요청 실행 (최대 10개 동시 요청)
# ──────────────────────────────
all_items = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_products, cat) for cat in concert_categories]

    for future in as_completed(futures):
        items = future.result()
        all_items.extend(items)

# ──────────────────────────────
# 4. 데이터 저장
# ──────────────────────────────
# ───────── 저장 + 시간 측정 ─────────
df_all = pd.DataFrame(all_items)

current_datetime = datetime.now()
df_all['collected_datetime'] = current_datetime

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename = f"ticketbay_log/{timestamp}.csv"

df_all.to_csv(filename, index=False, encoding="utf-8-sig")

end = time.time()
elapsed = end - start

# ⏱ 소요 시간 출력
print(f"\n🎉 전체 저장 완료: {filename} (총 {len(df_all)}건)")
print(f"⏱ 총 소요 시간: {elapsed:.2f}초")

# Supabase에 업로드
try:
    # DataFrame을 딕셔너리 리스트로 변환
    data_to_insert = df_all.to_dict('records')
    
    # Supabase에 배치 삽입
    result = supabase.table('ticketbay_data').insert(data_to_insert).execute()
    
    print(f"✅ Supabase 업로드 성공: {len(data_to_insert)}건")
    
except Exception as e:
    print(f"❌ Supabase 업로드 실패: {e}")

