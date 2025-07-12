import requests
import pandas as pd
from datetime import datetime
import logging
from typing import List, Dict, Any

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleInterparkCrawler:
    """심플 인터파크 HOT 티켓 크롤러"""
    
    def __init__(self):
        self.base_url = "https://tickets.interpark.com/contents/api/open-notice/notice-list"
        logger.info("인터파크 크롤러 초기화 완료")
    
    def _get_request_headers(self) -> Dict[str, str]:
        """API 요청 헤더"""
        return {
            "host": "tickets.interpark.com",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "accept": "application/json, text/plain, */*",
            "referer": "https://tickets.interpark.com/contents/notice",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
        }
    
    def _get_request_params(self) -> Dict[str, Any]:
        """API 요청 파라미터"""
        return {
            "goodsGenre": "ALL", 
            "goodsRegion": "ALL",
            "offset": 0,
            "pageSize": 1000,
            "sorting": "OPEN_ASC"
        }
    
    def fetch_ticket_data(self) -> List[Dict[str, Any]]:
        """인터파크 API에서 티켓 데이터 가져오기"""
        try:
            response = requests.get(
                self.base_url, 
                params=self._get_request_params(), 
                headers=self._get_request_headers(),
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"API 호출 성공: {len(data)}개 티켓 조회")
            return data
            
        except requests.RequestException as e:
            logger.error(f"API 호출 실패: {e}")
            raise
    
    def filter_hot_tickets(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """HOT 티켓만 필터링하고 정보 정리"""
        hot_tickets = []
        
        for ticket in raw_data:
            # HOT 티켓만 선별
            if not ticket.get('isHot', False):
                continue
            
            # 티켓 정보 정리
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
                '포스터URL': ticket.get('posterImageUrl', ''),
                '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            hot_tickets.append(ticket_info)
        
        logger.info(f"HOT 티켓 {len(hot_tickets)}개 필터링 완료")
        return hot_tickets
    
    def save_to_csv(self, tickets: List[Dict[str, Any]], filename: str = None) -> str:
        """티켓 데이터를 CSV 파일로 저장"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"interpark_hot_tickets_{timestamp}.csv"
        
        try:
            df = pd.DataFrame(tickets)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            logger.info(f"CSV 파일 저장 완료: {filename}")
            return filename
        except Exception as e:
            logger.error(f"CSV 저장 실패: {e}")
            return ""
    
    def crawl(self) -> Dict[str, Any]:
        """크롤링 실행"""
        logger.info("인터파크 HOT 티켓 크롤링 시작")
        
        try:
            # 1. 티켓 데이터 가져오기
            raw_data = self.fetch_ticket_data()
            
            # 2. HOT 티켓 필터링
            hot_tickets = self.filter_hot_tickets(raw_data)
            
            # 3. CSV 저장
            csv_filename = ""
            if hot_tickets:
                csv_filename = self.save_to_csv(hot_tickets)
            
            # 4. 결과 반환
            result = {
                '전체티켓수': len(raw_data),
                'HOT티켓수': len(hot_tickets),
                'CSV파일': csv_filename,
                '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '티켓데이터': hot_tickets
            }
            
            logger.info(f"크롤링 완료: 전체 {len(raw_data)}개 중 HOT {len(hot_tickets)}개")
            return result
            
        except Exception as e:
            logger.error(f"크롤링 실행 오류: {e}")
            raise
    
    def print_summary(self, result: Dict[str, Any]):
        """크롤링 결과 요약 출력"""
        print("=" * 60)
        print("🎫 인터파크 HOT 티켓 크롤링 결과")
        print("=" * 60)
        print(f"📊 전체 티켓: {result['전체티켓수']}개")
        print(f"🔥 HOT 티켓: {result['HOT티켓수']}개")
        print(f"📁 CSV 파일: {result['CSV파일']}")
        print(f"⏰ 크롤링 시간: {result['크롤링시간']}")
        print("=" * 60)
        
        # HOT 티켓 목록 출력
        if result['티켓데이터']:
            print("\n🔥 HOT 티켓 목록:")
            for i, ticket in enumerate(result['티켓데이터'], 1):
                print(f"{i:2d}. {ticket['제목']}")
                print(f"    ⏰ {ticket['오픈시간']} | 👀 {ticket['조회수']:,}회 | 🎭 {ticket['장르']}")
                print(f"    🎪 {ticket['공연장']} | 🎫 {ticket['예매코드']}")
                print()

def main():
    """메인 실행 함수"""
    try:
        print("🎫 인터파크 HOT 티켓 크롤러 시작\n")
        
        # 크롤러 생성
        crawler = SimpleInterparkCrawler()
        
        # 크롤링 실행
        result = crawler.crawl()
        
        # 결과 출력
        crawler.print_summary(result)
        
        # 추가 옵션
        while True:
            print("\n📋 추가 옵션:")
            print("1. 크롤링 재실행")
            print("2. 티켓 상세정보 보기")
            print("3. 종료")
            
            choice = input("\n선택하세요 (1-3): ").strip()
            
            if choice == '1':
                print("\n🔄 크롤링 재실행 중...")
                result = crawler.crawl()
                crawler.print_summary(result)
                
            elif choice == '2':
                if result['티켓데이터']:
                    ticket_num = input(f"티켓 번호 입력 (1-{len(result['티켓데이터'])}): ").strip()
                    try:
                        idx = int(ticket_num) - 1
                        if 0 <= idx < len(result['티켓데이터']):
                            ticket = result['티켓데이터'][idx]
                            print(f"\n📋 {ticket['제목']} 상세정보:")
                            for key, value in ticket.items():
                                print(f"  {key}: {value}")
                        else:
                            print("❌ 잘못된 번호입니다.")
                    except ValueError:
                        print("❌ 숫자를 입력해주세요.")
                else:
                    print("❌ 크롤링된 티켓이 없습니다.")
                    
            elif choice == '3':
                break
                
            else:
                print("❌ 잘못된 선택입니다.")
        
        print("\n👋 프로그램을 종료합니다.")
            
    except KeyboardInterrupt:
        print("\n\n⏹️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        logger.error(f"메인 실행 오류: {e}")

if __name__ == "__main__":
    main()