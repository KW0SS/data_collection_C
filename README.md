# 데이터 수집 프로젝트 초기 셋업

⚠️ IMPORTANT:
- `.env` 파일은 절대 Git에 커밋하지 말 것
- 노출된 토큰은 즉시 폐기(rotate)할 것
- AWS 키는 팀 외부에 공유 금지

이 저장소는 **환경변수(.env) 초기화 + raw_data JSON S3 업로드 구조**를 빠르게 시작하기 위한 기본 템플릿입니다.

## 프로젝트 구조

```text
.
├── .env                  # 로컬에서만 생성 (커밋 금지)
├── .env.example          # 키 이름/기본값 템플릿
├── .gitignore
├── README.md
├── requirements.txt
├── companies.csv         # 샘플(커밋 가능)
├── docs/
│   ├── DOPPLER_GUIDE.md
│   └── NAMING_CONVENTION.md
├── scripts/
│   ├── download_env.sh
│   └── upload_raw_data.py
├── raw_data/             # 로컬 원본 JSON 저장 경로 (커밋 금지)
└── output/               # 가공 CSV 결과 경로 (커밋 금지 기본 정책)
```

## 1) 환경 세팅

Python 3.10 이상 기준입니다.

```bash
python -m venv venv
source venv/bin/activate   # mac/linux
venv\Scripts\activate      # windows
pip install -r requirements.txt
```

## 2) Doppler로 .env 다운로드

토큰은 공유/캡처 금지입니다.

### Mac/Linux Terminal

```bash
export DOPPLER_TOKEN="여기에_새토큰"
bash scripts/download_env.sh
```

### Windows PowerShell

```powershell
$env:DOPPLER_TOKEN="여기에_새토큰"
bash scripts/download_env.sh
```

> Windows에서 `bash`가 없다면 Git Bash 또는 WSL에서 실행하세요.

## 3) companies.csv 준비

`companies.csv` 컬럼 스펙:

- `stock_code,corp_name,label,gics_sector`
- `gics_sector`는 아래 문자열과 **완전히 동일**해야 합니다.

```text
Energy
Materials
Industrials
Consumer Discretionary
Consumer Staples
Health Care
Financials
Information Technology
Communication Services
Utilities
Real Estate
```

## 4) raw_data 파일 네이밍 규칙

로컬 원본 JSON 형식:

- `{종목코드}_{연도}_{분기}_{재무제표구분}.json`
- 분기: `Q1`, `H1`, `Q3`, `ANNUAL`
- 재무제표구분: `CFS`, `OFS`

예시:

- `019440_2023_Q1_CFS.json`
- `019440_2023_H1_CFS.json`
- `019440_2023_Q3_OFS.json`
- `019440_2023_ANNUAL_CFS.json`

## 5) Dry-run 테스트

실제 업로드 없이 경로만 확인합니다.

```bash
python scripts/upload_raw_data.py --dry-run
```

## 6) 특정 섹터만 업로드

```bash
python scripts/upload_raw_data.py --sector Financials --sector Energy
```

## 7) OFS 포함 업로드

기본은 CFS 우선이며, `--use-ofs`를 주면 CFS가 없는 경우 OFS 업로드를 허용합니다.

```bash
python scripts/upload_raw_data.py --use-ofs
```

## 업로드 경로 규칙 (프로젝트 prefix 포함)

S3 key 규칙:

- `data_collection/{GICS섹터}/{종목코드}_{연도}_{분기}.json`

최종 URI 예시:

- `s3://kw0ss-raw-data-s3/data_collection/Financials/105560_2023_Q1.json`

## 스크립트 동작 요약

`scripts/upload_raw_data.py`:

- `argparse` 옵션 제공: `--sector`, `--dry-run`, `--use-ofs`
- 기본 섹터: `Financials`, `Consumer Staples`, `Energy`, `Utilities`, `Real Estate`
- `companies.csv`를 읽어 섹터 필터링
- `raw_data`에서 규칙 일치 파일만 선별
- S3 업로드 시 `Content-Type: application/json` 지정
- 성공 시 `Uploaded: s3://bucket/key` 출력
- 실패 시 구조화 에러 로그 출력

## 참고 문서

- Doppler 상세 가이드: `docs/DOPPLER_GUIDE.md`
- 네이밍 규칙 원문: `docs/NAMING_CONVENTION.md`
