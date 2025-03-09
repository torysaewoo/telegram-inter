# 인터파크 티켓 알림 텔레그램 봇

이 프로젝트는 인터파크 티켓 정보를 수집하고 텔레그램 봇을 통해 알림을 보내는 자동화 도구입니다.

## 주요 기능

- 인터파크 티켓 정보 수집
- 오늘 오픈하는 티켓 필터링
- 텔레그램 봇을 통한 알림 전송
- 구독자 관리 기능

## 설치 방법

1. 저장소 클론
```bash
git clone https://github.com/[사용자명]/[저장소명].git
cd [저장소명]
```

2. 가상 환경 설정
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. 필요한 패키지 설치
```bash
pip install requests beautifulsoup4 python-telegram-bot
```

4. 환경 변수 설정
`.env` 파일을 생성하고 다음 내용을 추가합니다:
```
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
```

## 사용 방법

```bash
python telegram_interpark.py
```

## 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 