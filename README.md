# SQL Query Assistant

자연어로 SQL 쿼리를 생성하고 실행하는 도구입니다. Amazon Bedrock의 Claude와 PostgreSQL을 활용합니다.

## 기능

- 자연어를 SQL 쿼리로 변환
- 테이블 구조 자동 분석
- 실시간 쿼리 실행 및 결과 표시
- 실행 취소 기능

## 필요 조건

- Python 3.8+
- PostgreSQL
- Amazon Bedrock 접근 권한
- Node.js와 npm (MCP 서버 실행용)

## 설치 방법

1. 레포지토리 클론
```bash
git clone https://github.com/etaek/sql-query-assistant.git
cd sql-query-assistant
```

2. 의존성 설치
```bash
pip install -r requirements.txt
npm install -g @modelcontextprotocol/server-postgres
```

3. 환경 설정
- PostgreSQL 데이터베이스 준비
- Amazon Bedrock 접근 권한 설정

## 실행 방법

```bash
streamlit run client_for_multi_server/main.py
```

## 사용 방법

1. 웹 브라우저에서 Streamlit 앱 접속
2. 자연어로 쿼리 작성 (예: "부서별 모니터 신청 현황을 조회해주세요")
3. "쿼리 실행" 버튼 클릭
4. 결과 확인

## 주요 기술 스택

- Streamlit
- Amazon Bedrock (Claude 3 Sonnet)
- PostgreSQL
- Model Context Protocol (MCP)
- Python asyncio