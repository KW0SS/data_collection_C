import os
from dotenv import load_dotenv
import boto3

PROJECT_PREFIX = "data_collection"

def main():
    load_dotenv()

    bucket = os.getenv("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION", "ap-northeast-2")
    access_key = os.getenv("S3_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("S3_PRIVATE_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")

    if not bucket:
        raise RuntimeError("S3_BUCKET_NAME 이(가) .env에 없습니다.")
    if not access_key or not secret_key:
        raise RuntimeError("S3_ACCESS_KEY/S3_PRIVATE_KEY 또는 AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY 가 .env에 없습니다.")

    s3 = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    local_path = r"raw_data\test.json"
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"테스트 파일이 없습니다: {local_path}")

    # 네이밍 컨벤션/폴더 구조에 맞춘 테스트 경로
    sector = "Financials"  # 아무거나 하나 (네 담당 섹터)
    s3_key = f"{PROJECT_PREFIX}/{sector}/TEST_2026_Q1.json"

    s3.upload_file(
        local_path,
        bucket,
        s3_key,
        ExtraArgs={"ContentType": "application/json"},
    )

    print("✅ 업로드 성공:", f"s3://{bucket}/{s3_key}")
    print("※ 목록 조회 권한이 없으면 S3 콘솔에서 직접 보이진 않을 수 있어도 업로드는 성공한 것입니다.")

if __name__ == "__main__":
    main()