import os
from dotenv import load_dotenv
import boto3

def main():
    load_dotenv()

    bucket = os.getenv("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION", "ap-northeast-2")

    # 팀 .env가 S3_ACCESS_KEY/S3_PRIVATE_KEY를 쓰는 경우 + AWS_*를 쓰는 경우 둘 다 대응
    access_key = os.getenv("S3_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("S3_PRIVATE_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")  # 있을 수도 있음

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

    # 권한/연결 확인: 버킷의 일부 객체 목록 조회 (비어있어도 OK)
    resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=5)
    keys = [x["Key"] for x in resp.get("Contents", [])]

    print("✅ S3 연결 성공")
    print("Bucket:", bucket)
    print("Region:", region)
    print("Sample keys:", keys)

if __name__ == "__main__":
    main()