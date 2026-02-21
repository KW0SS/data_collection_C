#!/usr/bin/env bash
set -euo pipefail

# 보안 경고: 토큰 값은 출력/공유/캡처하지 않는다.
if [[ -z "${DOPPLER_TOKEN:-}" ]]; then
  echo "[ERROR] DOPPLER_TOKEN 환경변수가 설정되어 있지 않습니다."
  echo "[HINT] export DOPPLER_TOKEN=\"새로_발급받은_토큰\""
  exit 1
fi

echo "[INFO] Doppler에서 .env 다운로드를 시작합니다."

if curl -sf \
  --url "https://api.doppler.com/v3/configs/config/secrets/download?format=env" \
  --header "authorization: Bearer $DOPPLER_TOKEN" \
  > .env; then
  echo "[SUCCESS] .env 파일 생성이 완료되었습니다."
  echo "[SECURITY] .env 파일은 절대 Git에 커밋하지 마세요."
else
  echo "[ERROR] .env 다운로드에 실패했습니다. 토큰 유효성/권한/네트워크를 확인하세요."
  rm -f .env
  exit 1
fi
