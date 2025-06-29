import os
import tweepy
from dotenv import load_dotenv
from datetime import datetime

# 환경변수 로드
load_dotenv()

class SimpleImageTweet:
    """이미지 포함 트윗 간단 게시기"""
    
    def __init__(self):
        self.api = None
        self.client = None
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
            
            print("✅ 트위터 API 연결 성공")
            
        except Exception as e:
            print(f"❌ 트위터 API 설정 실패: {e}")
            raise
    
    def upload_image(self, image_path: str) -> str:
        """이미지 업로드"""
        try:
            if not os.path.exists(image_path):
                print(f"❌ 이미지 파일이 없습니다: {image_path}")
                return None
            
            print(f"📤 이미지 업로드 중: {image_path}")
            media = self.api.media_upload(image_path)
            print(f"✅ 이미지 업로드 성공! Media ID: {media.media_id}")
            return media.media_id
            
        except Exception as e:
            print(f"❌ 이미지 업로드 실패: {e}")
            return None
    
    def post_tweet_with_image(self, text: str, image_path: str = None) -> bool:
        """이미지와 함께 트윗 게시"""
        try:
            media_ids = []
            
            # 이미지가 있으면 업로드
            if image_path:
                media_id = self.upload_image(image_path)
                if media_id:
                    media_ids.append(media_id)
                else:
                    print("⚠️ 이미지 없이 텍스트만 게시합니다.")
            
            # 트윗 게시
            print(f"📝 트윗 게시 중...")
            if media_ids:
                response = self.client.create_tweet(text=text, media_ids=media_ids)
                print(f"✅ 이미지 트윗 게시 성공!")
            else:
                response = self.client.create_tweet(text=text)
                print(f"✅ 텍스트 트윗 게시 성공!")
            
            tweet_url = f"https://twitter.com/gamsahanticket/status/{response.data['id']}"
            print(f"🔗 트윗 URL: {tweet_url}")
            
            return True
            
        except Exception as e:
            print(f"❌ 트윗 게시 실패: {e}")
            return False
    
    def create_ticket_tweet(self, title: str, open_time: str, image_path: str = None) -> str:
        """티켓팅 트윗 텍스트 생성"""
        timestamp = datetime.now().strftime("%m월 %d일 %H:%M")
        
        tweet_text = f"""{title}

대리 티켓팅 진행
최근 세븐틴 / BTS / 블랙핑크 댈티 성공경력

선착순 할인 이벤트:
VIP 잡아도 수고비 5만원 선입금, 실패시 수고비 전액환불

🕐 오픈시간: {open_time}

친절한 상담: https://open.kakao.com/o/sAJ8m2Ah

#티켓팅 #대리티켓팅 #콘서트 #선착순할인"""

        return tweet_text[:280]  # 280자 제한

def simple_test():
    """간단한 테스트 함수"""
    print("🐦 이미지 포함 트윗 게시 테스트\n")
    
    # API 키 확인
    required_vars = [
        'TWITTER_API_KEY',
        'TWITTER_API_SECRET', 
        'TWITTER_ACCESS_TOKEN',
        'TWITTER_ACCESS_TOKEN_SECRET'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ 환경변수가 설정되지 않았습니다: {', '.join(missing_vars)}")
        return
    
    try:
        # 트윗 게시기 초기화
        tweeter = SimpleImageTweet()
        
        # 사용자 입력 받기
        print("📝 트윗 정보 입력:")
        title = input("공연 제목: ").strip() or "테스트 콘서트"
        open_time = input("오픈시간: ").strip() or "2025.02.15 (토) 20:00"
        image_path = input("이미지 파일 경로 (없으면 Enter): ").strip()
        
        # 이미지 파일 존재 확인
        if image_path and not os.path.exists(image_path):
            print(f"⚠️ 이미지 파일이 없습니다: {image_path}")
            use_image = input("이미지 없이 진행하시겠습니까? (y/n): ").strip().lower()
            if use_image != 'y':
                print("❌ 종료합니다.")
                return
            image_path = None
        
        # 트윗 텍스트 생성
        tweet_text = tweeter.create_ticket_tweet(title, open_time, image_path)
        
        # 미리보기
        print(f"\n📋 생성된 트윗 미리보기 ({len(tweet_text)}자):")
        print("="*50)
        print(tweet_text)
        print("="*50)
        
        if image_path:
            print(f"🖼️ 첨부 이미지: {image_path}")
        
        # 확인 후 게시
        confirm = input("\n🚀 이 트윗을 게시하시겠습니까? (y/n): ").strip().lower()
        if confirm == 'y':
            success = tweeter.post_tweet_with_image(tweet_text, image_path)
            if success:
                print("\n🎉 트윗 게시 완료!")
            else:
                print("\n❌ 트윗 게시 실패")
        else:
            print("❌ 게시를 취소했습니다.")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def quick_post(title: str, open_time: str, image_path: str = None):
    """빠른 게시 함수"""
    try:
        tweeter = SimpleImageTweet()
        tweet_text = tweeter.create_ticket_tweet(title, open_time, image_path)
        
        print(f"📝 트윗: {tweet_text[:50]}...")
        if image_path:
            print(f"🖼️ 이미지: {image_path}")
        
        return tweeter.post_tweet_with_image(tweet_text, image_path)
        
    except Exception as e:
        print(f"❌ 빠른 게시 실패: {e}")
        return False

# 사용 예시들
def usage_examples():
    """사용 예시"""
    print("📚 사용 예시:")
    print()
    
    print("1️⃣ 대화형 모드:")
    print("python simple_tweet.py")
    print()
    
    print("2️⃣ 코드에서 직접 호출:")
    print('quick_post("세븐틴 콘서트", "2025.02.15 (토) 20:00", "poster.jpg")')
    print()
    
    print("3️⃣ 텍스트만 게시:")
    print('quick_post("뉴진스 팬미팅", "2025.01.25 (토) 14:00")')

def main():
    """메인 함수"""
    print("🎯 이미지 포함 트윗 게시기")
    print()
    
    choice = input("📋 모드 선택:\n1. 대화형 테스트\n2. 사용 예시 보기\n3. 종료\n\n선택 (1-3): ").strip()
    
    if choice == '1':
        simple_test()
    elif choice == '2':
        usage_examples()
    elif choice == '3':
        print("👋 종료합니다.")
    else:
        print("❌ 잘못된 선택입니다.")

if __name__ == "__main__":
    main()