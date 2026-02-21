# 데이터 파일 네이밍 컨벤션

이 문서는 로컬 저장 파일명과 S3 업로드 경로 규칙을 정의합니다.  
아래 규칙을 반드시 그대로 사용하세요.

## 1) 출력 CSV 파일(가공 결과)

- 형식: `{종목코드}_{연도}.csv`
- 예시: `019440_2023.csv`

## 2) 원본 재무제표 JSON(로컬 저장용)

- 형식: `{종목코드}_{연도}_{분기}_{재무제표구분}.json`
- 분기 값(대문자 고정): `Q1`, `H1`, `Q3`, `ANNUAL`
- 재무제표구분: `CFS`(연결), `OFS`(별도)

예시:

- `019440_2023_Q1_CFS.json`
- `019440_2023_H1_CFS.json`
- `019440_2023_Q3_OFS.json`
- `019440_2023_ANNUAL_CFS.json`

## 3) S3 업로드 경로(프로젝트 prefix 포함)

- 형식: `s3://{S3_BUCKET_NAME}/data_collection/{GICS섹터}/{종목코드}_{연도}_{분기}.json`
- 예시: `s3://kw0ss-raw-data-s3/data_collection/Materials/019440_2023_Q1.json`

중요:

- 로컬 파일명과 달리 **S3 파일명에는 `CFS/OFS`를 포함하지 않습니다.**
- 업로드 정책은 기본적으로 **CFS 우선**입니다.
- `--use-ofs` 옵션 사용 시에만 CFS가 없는 경우 OFS 업로드를 허용합니다.

## 4) GICS 섹터 공식 영문 명칭 (문자열 완전 일치)

- `Energy`
- `Materials`
- `Industrials`
- `Consumer Discretionary`
- `Consumer Staples`
- `Health Care`
- `Financials`
- `Information Technology`
- `Communication Services`
- `Utilities`
- `Real Estate`
