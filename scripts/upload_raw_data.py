from __future__ import annotations

import argparse
import json
import traceback
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pandas_library
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import os

# 조장이 요구한 S3 프로젝트 prefix. 하드코딩 문자열 반복을 방지하기 위해 상수로 관리한다.
PROJECT_PREFIX = "data_collection"

# 담당 기본 섹터(소수 섹터 + 안정군)
DEFAULT_TARGET_SECTORS = [
    "Financials",
    "Consumer Staples",
    "Energy",
    "Utilities",
    "Real Estate",
]

# GICS 공식 영문 섹터명(문자열 일치 강제)
VALID_GICS_SECTORS = [
    "Energy",
    "Materials",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Health Care",
    "Financials",
    "Information Technology",
    "Communication Services",
    "Utilities",
    "Real Estate",
]

# raw_data 파일명 규칙: {stock_code}_{year}_{quarter}_{statement}.json
RAW_JSON_FILENAME_PATTERN = re.compile(
    r"^(?P<stock_code>\d{6})_(?P<year>\d{4})_(?P<quarter>Q1|H1|Q3|ANNUAL)_(?P<statement>CFS|OFS)\.json$"
)


def log_event(
    level: str,
    stage_name: str,
    status_summary: str,
    major_parameters: Optional[Dict[str, object]] = None,
    error_code: Optional[str] = None,
    error_stack: Optional[str] = None,
) -> None:
    """구조화 로그 출력: timestamp, 단계명, 주요 파라미터, 상태 요약, 에러 코드/스택."""
    log_payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "stage_name": stage_name,
        "major_parameters": major_parameters or {},
        "status_summary": status_summary,
        "error_code": error_code,
        "error_stack": error_stack,
    }
    print(json.dumps(log_payload, ensure_ascii=False))


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="raw_data JSON 파일을 표준 규칙으로 S3에 업로드합니다."
    )
    parser.add_argument(
        "--companies-csv",
        default="companies.csv",
        help="기업 메타데이터 CSV 경로 (기본값: companies.csv)",
    )
    parser.add_argument(
        "--raw-data-dir",
        default="raw_data",
        help="원본 JSON 디렉토리 경로 (기본값: raw_data)",
    )
    parser.add_argument(
        "--sector",
        action="append",
        choices=VALID_GICS_SECTORS,
        help="업로드 대상 섹터 지정 (여러 개 사용 가능). 지정하지 않으면 기본 5개 섹터 사용",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="실제 업로드 없이 업로드 예정 경로만 출력",
    )
    parser.add_argument(
        "--use-ofs",
        action="store_true",
        help="CFS가 없는 경우 OFS 업로드를 허용",
    )
    return parser.parse_args()


def validate_environment_variables() -> Dict[str, str]:
    load_dotenv()
    required_environment_keys = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "S3_BUCKET_NAME",
    ]
    missing_environment_keys = [
        environment_key
        for environment_key in required_environment_keys
        if not os.getenv(environment_key)
    ]
    if missing_environment_keys:
        raise ValueError(
            "필수 환경변수가 누락되었습니다: "
            + ", ".join(missing_environment_keys)
            + " (.env를 확인하세요)"
        )

    return {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_REGION": os.getenv("AWS_REGION", ""),
        "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME", ""),
        "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN", ""),
    }


def load_companies_sector_mapping(
    companies_csv_path: Path, target_sectors: List[str]
) -> Dict[str, str]:
    if not companies_csv_path.exists():
        raise FileNotFoundError(f"companies.csv 파일을 찾을 수 없습니다: {companies_csv_path}")

    companies_dataframe = pandas_library.read_csv(
        companies_csv_path, dtype={"stock_code": str}
    )

    required_columns = {"stock_code", "corp_name", "label", "gics_sector"}
    missing_columns = required_columns - set(companies_dataframe.columns)
    if missing_columns:
        raise ValueError(
            "companies.csv 필수 컬럼 누락: " + ", ".join(sorted(missing_columns))
        )

    companies_dataframe["stock_code"] = companies_dataframe["stock_code"].str.zfill(6)

    invalid_sector_values = sorted(
        set(companies_dataframe["gics_sector"].dropna()) - set(VALID_GICS_SECTORS)
    )
    if invalid_sector_values:
        raise ValueError(
            "companies.csv의 gics_sector 값이 공식 섹터명과 일치하지 않습니다: "
            + ", ".join(invalid_sector_values)
        )

    filtered_companies_dataframe = companies_dataframe[
        companies_dataframe["gics_sector"].isin(target_sectors)
    ]

    return dict(
        zip(
            filtered_companies_dataframe["stock_code"],
            filtered_companies_dataframe["gics_sector"],
        )
    )


def select_upload_candidates(
    raw_data_directory_path: Path,
    stock_code_to_sector_map: Dict[str, str],
    allow_ofs_upload: bool,
) -> List[Tuple[Path, str, str]]:
    if not raw_data_directory_path.exists():
        raise FileNotFoundError(f"raw_data 디렉토리를 찾을 수 없습니다: {raw_data_directory_path}")

    parsed_file_map: Dict[Tuple[str, str, str], Dict[str, Path]] = {}

    for raw_json_file_path in raw_data_directory_path.glob("*.json"):
        filename_match = RAW_JSON_FILENAME_PATTERN.match(raw_json_file_path.name)
        if not filename_match:
            log_event(
                level="WARN",
                stage_name="파일명검증",
                status_summary="네이밍 규칙 불일치 파일을 건너뜁니다.",
                major_parameters={"filename": raw_json_file_path.name},
                error_code="INVALID_FILENAME_PATTERN",
            )
            continue

        stock_code = filename_match.group("stock_code")
        year_value = filename_match.group("year")
        quarter_value = filename_match.group("quarter")
        statement_type = filename_match.group("statement")

        if stock_code not in stock_code_to_sector_map:
            continue

        logical_file_group_key = (stock_code, year_value, quarter_value)
        if logical_file_group_key not in parsed_file_map:
            parsed_file_map[logical_file_group_key] = {}
        parsed_file_map[logical_file_group_key][statement_type] = raw_json_file_path

    upload_candidates: List[Tuple[Path, str, str]] = []
    for logical_file_group_key, statement_file_map in parsed_file_map.items():
        stock_code, year_value, quarter_value = logical_file_group_key
        sector_name = stock_code_to_sector_map[stock_code]

        selected_file_path: Optional[Path] = None
        if "CFS" in statement_file_map:
            selected_file_path = statement_file_map["CFS"]
        elif allow_ofs_upload and "OFS" in statement_file_map:
            selected_file_path = statement_file_map["OFS"]

        if selected_file_path is None:
            log_event(
                level="INFO",
                stage_name="대상선정",
                status_summary="업로드 가능한 파일(CFS 우선, OFS 옵션)이 없어 건너뜁니다.",
                major_parameters={
                    "stock_code": stock_code,
                    "year": year_value,
                    "quarter": quarter_value,
                },
            )
            continue

        s3_object_key = (
            f"{PROJECT_PREFIX}/{sector_name}/{stock_code}_{year_value}_{quarter_value}.json"
        )
        upload_candidates.append((selected_file_path, sector_name, s3_object_key))

    return upload_candidates


def create_s3_client(environment_variables: Dict[str, str]):
    boto3_client_parameters = {
        "service_name": "s3",
        "region_name": environment_variables["AWS_REGION"],
        "aws_access_key_id": environment_variables["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": environment_variables["AWS_SECRET_ACCESS_KEY"],
    }
    if environment_variables["AWS_SESSION_TOKEN"]:
        boto3_client_parameters["aws_session_token"] = environment_variables[
            "AWS_SESSION_TOKEN"
        ]
    return boto3.client(**boto3_client_parameters)


def upload_files_to_s3(
    s3_client,
    bucket_name: str,
    upload_candidates: List[Tuple[Path, str, str]],
    dry_run_enabled: bool,
) -> None:
    if not upload_candidates:
        log_event(
            level="WARN",
            stage_name="업로드",
            status_summary="업로드 대상 파일이 없습니다.",
        )
        return

    for source_file_path, sector_name, s3_object_key in upload_candidates:
        target_s3_uri = f"s3://{bucket_name}/{s3_object_key}"
        if dry_run_enabled:
            print(f"[DRY-RUN] {source_file_path} -> {target_s3_uri}")
            continue

        try:
            s3_client.upload_file(
                Filename=str(source_file_path),
                Bucket=bucket_name,
                Key=s3_object_key,
                ExtraArgs={"ContentType": "application/json"},
            )
            print(f"Uploaded: {target_s3_uri}")
            log_event(
                level="INFO",
                stage_name="업로드",
                status_summary="업로드 성공",
                major_parameters={
                    "file": source_file_path.name,
                    "sector": sector_name,
                    "target": target_s3_uri,
                },
            )
        except (ClientError, BotoCoreError, OSError) as upload_error:
            log_event(
                level="ERROR",
                stage_name="업로드",
                status_summary="업로드 실패",
                major_parameters={
                    "file": source_file_path.name,
                    "sector": sector_name,
                    "target": target_s3_uri,
                },
                error_code=upload_error.__class__.__name__,
                error_stack=traceback.format_exc(),
            )


def main() -> None:
    argument_namespace = parse_arguments()
    target_sectors = argument_namespace.sector or DEFAULT_TARGET_SECTORS

    try:
        log_event(
            level="INFO",
            stage_name="시작",
            status_summary="업로드 스크립트 실행 시작",
            major_parameters={
                "target_sectors": target_sectors,
                "dry_run": argument_namespace.dry_run,
                "use_ofs": argument_namespace.use_ofs,
            },
        )

        environment_variables = validate_environment_variables()
        stock_code_to_sector_map = load_companies_sector_mapping(
            companies_csv_path=Path(argument_namespace.companies_csv),
            target_sectors=target_sectors,
        )
        log_event(
            level="INFO",
            stage_name="기업필터링",
            status_summary="섹터 기준 기업 필터링 완료",
            major_parameters={"selected_companies_count": len(stock_code_to_sector_map)},
        )

        upload_candidates = select_upload_candidates(
            raw_data_directory_path=Path(argument_namespace.raw_data_dir),
            stock_code_to_sector_map=stock_code_to_sector_map,
            allow_ofs_upload=argument_namespace.use_ofs,
        )
        log_event(
            level="INFO",
            stage_name="대상선정",
            status_summary="업로드 대상 파일 선별 완료",
            major_parameters={"candidate_count": len(upload_candidates)},
        )

        s3_client = create_s3_client(environment_variables)
        upload_files_to_s3(
            s3_client=s3_client,
            bucket_name=environment_variables["S3_BUCKET_NAME"],
            upload_candidates=upload_candidates,
            dry_run_enabled=argument_namespace.dry_run,
        )
        log_event(
            level="INFO",
            stage_name="종료",
            status_summary="업로드 스크립트 실행 종료",
        )
    except Exception as unexpected_error:
        log_event(
            level="ERROR",
            stage_name="치명오류",
            status_summary="스크립트 실행 중 예외가 발생했습니다.",
            error_code=unexpected_error.__class__.__name__,
            error_stack=traceback.format_exc(),
        )
        raise


if __name__ == "__main__":
    main()
