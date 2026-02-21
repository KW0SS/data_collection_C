# Doppler로 .env 받기 가이드

## 보안 원칙

- 토큰/키/시크릿은 코드, 문서, 채팅, 스크린샷에 실제값을 남기지 않습니다.
- `.env` 파일은 로컬 전용이며 Git 커밋 금지입니다.
- 이미 노출된 토큰은 즉시 폐기(rotate/revoke)하고 새 토큰을 발급받으세요.
- 토큰은 공유/캡처 금지입니다.

## 1) 사전 준비

- `curl` 사용 가능 환경
- 프로젝트 루트에서 실행
- Doppler에서 발급한 개인 토큰 준비

## 2) 토큰 환경변수 설정

### Mac/Linux Terminal

```bash
export DOPPLER_TOKEN="여기에_새토큰"
```

### Windows PowerShell

```powershell
$env:DOPPLER_TOKEN="여기에_새토큰"
```

## 3) .env 다운로드

### 스크립트 사용(권장)

```bash
bash scripts/download_env.sh
```

### 직접 curl 실행(동일 동작)

```bash
curl -sf \
  --url "https://api.doppler.com/v3/configs/config/secrets/download?format=env" \
  --header "authorization: Bearer $DOPPLER_TOKEN" \
  > .env
```

## 4) 실패 시 체크리스트

- `DOPPLER_TOKEN`이 비어 있지 않은지 확인
- 토큰 만료/권한 문제 여부 확인
- 네트워크 및 사내 방화벽 정책 확인
- 실패 후 생성된 불완전 `.env` 파일이 있다면 삭제 후 재시도

## 5) 검증

- 프로젝트 루트에 `.env` 생성 여부 확인
- `.env.example` 대비 필수 키 누락 여부 확인
