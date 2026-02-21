"""S3 업로드 모듈 – 원본 재무제표 JSON을 GICS 섹터별로 S3에 저장.

S3 디렉터리 구조
─────────────────
s3://{bucket}/
  └── {gics_sector}/
      ├── 019440_2023_Q1.json
      ├── 019440_2023_H1.json
      ├── 019440_2023_Q3.json
      └── 019440_2023_ANNUAL.json

필요한 환경변수 (.env)
─────────────────────
  S3_ACCESS_KEY    – AWS Access Key ID
  S3_PRIVATE_KEY   – AWS Secret Access Key
  S3_BUCKET_NAME   – S3 버킷 이름
  S3_REGION        – (선택) AWS 리전 (기본: ap-northeast-2)
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


def _load_env() -> dict[str, str]:
    """프로젝트 루트의 .env 파일에서 환경변수 읽기."""
    env: dict[str, str] = {}
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return env
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def _get_s3_config(
    bucket: str | None = None,
    region: str | None = None,
) -> dict[str, str]:
    """S3 접속 정보를 환경변수 + .env에서 가져옴."""
    env = _load_env()

    access_key = os.getenv("S3_ACCESS_KEY") or env.get("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_PRIVATE_KEY") or env.get("S3_PRIVATE_KEY")
    bucket_name = bucket or os.getenv("S3_BUCKET_NAME") or env.get("S3_BUCKET_NAME")
    region_name = region or os.getenv("S3_REGION") or env.get("S3_REGION", "ap-northeast-2")

    if not access_key or not secret_key:
        raise RuntimeError(
            "S3 인증 키가 없습니다. .env에 S3_ACCESS_KEY, S3_PRIVATE_KEY를 설정하세요."
        )
    if not bucket_name:
        raise RuntimeError(
            "S3 버킷 이름이 없습니다. --s3-bucket 옵션이나 .env에 S3_BUCKET_NAME을 설정하세요."
        )

    return {
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": bucket_name,
        "region": region_name,
    }


def _get_s3_client(config: dict[str, str]):
    """boto3 S3 클라이언트 생성."""
    try:
        import boto3
    except ImportError:
        raise RuntimeError(
            "boto3가 설치되어 있지 않습니다. pip install boto3 를 실행하세요."
        )

    return boto3.client(
        "s3",
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name=config["region"],
    )


def _try_create_bucket(client, bucket: str, region: str) -> None:
    """버킷이 없을 때 생성을 시도합니다.

    IAM 사용자에 CreateBucket 권한이 없으면 경고만 출력하고 넘어갑니다.
    (PutObject 권한만 있어도 기존 버킷에 업로드는 가능)
    """
    try:
        print(f"  🪣 S3 버킷 '{bucket}' 생성 시도 중...", file=sys.stderr)
        if region == "us-east-1":
            client.create_bucket(Bucket=bucket)
        else:
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"  ✅ S3 버킷 '{bucket}' 생성 완료", file=sys.stderr)
    except client.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"  ✅ S3 버킷 '{bucket}' 이미 존재", file=sys.stderr)
        elif error_code == "AccessDenied":
            print(
                f"  ⚠️  버킷 생성 권한 없음 (기존 버킷에 직접 업로드 시도)",
                file=sys.stderr,
            )
        else:
            raise


def upload_raw_to_s3(
    raw_items: list[dict[str, Any]],
    stock_code: str,
    year: str,
    quarter: str,
    gics_sector: str,
    bucket: str | None = None,
    region: str | None = None,
) -> str:
    """
    원본 재무제표 JSON 1건을 S3에 업로드.

    S3 Key: {gics_sector}/{stock_code}_{year}_{quarter}.json

    Args:
        raw_items: DART에서 받은 원시 재무제표 데이터
        stock_code: 종목코드
        year: 연도
        quarter: 분기 (Q1, H1, Q3, ANNUAL)
        gics_sector: GICS 섹터명 (예: "Energy", "Industrials")
        bucket: S3 버킷 이름 (없으면 .env에서 읽기)
        region: AWS 리전 (없으면 .env에서 읽기)

    Returns:
        업로드된 S3 key
    """
    config = _get_s3_config(bucket, region)
    client = _get_s3_client(config)

    # S3 key 생성: {gics_sector}/{stock_code}_{year}_{quarter}.json
    s3_key = f"{gics_sector}/{stock_code}_{year}_{quarter}.json"
    body = json.dumps(raw_items, ensure_ascii=False, indent=2).encode("utf-8")

    # 업로드 시도 → NoSuchBucket이면 버킷 생성 후 재시도
    try:
        client.put_object(
            Bucket=config["bucket"], Key=s3_key, Body=body,
            ContentType="application/json; charset=utf-8",
        )
    except client.exceptions.NoSuchBucket:
        _try_create_bucket(client, config["bucket"], config["region"])
        client.put_object(
            Bucket=config["bucket"], Key=s3_key, Body=body,
            ContentType="application/json; charset=utf-8",
        )

    return f"s3://{config['bucket']}/{s3_key}"


def upload_batch_to_s3(
    raw_data_list: list[dict[str, Any]],
    bucket: str | None = None,
    region: str | None = None,
) -> list[str]:
    """
    여러 건의 원본 재무제표를 S3에 배치 업로드.

    Args:
        raw_data_list: [
            {
                "raw_items": [...],
                "stock_code": "019440",
                "year": "2023",
                "quarter": "Q1",
                "gics_sector": "Materials",
            },
            ...
        ]

    Returns:
        업로드된 S3 key 리스트
    """
    if not raw_data_list:
        return []

    config = _get_s3_config(bucket, region)
    client = _get_s3_client(config)
    bucket_name = config["bucket"]
    bucket_checked = False  # NoSuchBucket 발생 시 한 번만 생성 시도

    uploaded: list[str] = []

    for entry in raw_data_list:
        s3_key = (
            f"{entry['gics_sector']}/"
            f"{entry['stock_code']}_{entry['year']}_{entry['quarter']}.json"
        )
        body = json.dumps(
            entry["raw_items"], ensure_ascii=False, indent=2
        ).encode("utf-8")

        try:
            client.put_object(
                Bucket=bucket_name, Key=s3_key, Body=body,
                ContentType="application/json; charset=utf-8",
            )
        except client.exceptions.NoSuchBucket:
            if not bucket_checked:
                _try_create_bucket(client, bucket_name, config["region"])
                bucket_checked = True
                # 재시도
                client.put_object(
                    Bucket=bucket_name, Key=s3_key, Body=body,
                    ContentType="application/json; charset=utf-8",
                )
            else:
                raise

        s3_uri = f"s3://{bucket_name}/{s3_key}"
        uploaded.append(s3_uri)
        print(f"  ☁️  {s3_uri}", file=sys.stderr)

    print(
        f"\n✅ S3 업로드 완료: {len(uploaded)}개 파일 → s3://{config['bucket']}/",
        file=sys.stderr,
    )
    return uploaded