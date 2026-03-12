# 상폐 기업 C섹터 필터링 + 정상 기업과 합쳐서 C_companies_final.csv 생성
# 사전 준비: f_fetch_induty_codes.py 실행 → data/etc/delisted_induty_codes.csv 생성

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd

# ── 경로 설정 ─────────────────────────────────────────────
INDUTY_FILE   = Path("data/input/delisted_induty_codes.csv")
DELISTED_FILE = Path("data/input/상장폐지현황.xlsx")

NORMAL_FILE   = Path("data/input/C_companies.csv")
OUTPUT_FILE   = Path("data/input/C_companies_final.csv")

START_YEAR = 2015
LABEL = 1


# ── induty_code → GICS 매핑 (C섹터 기준) ───────────────────

INDUTY_GICS_3 = {

    # Financials
    "641": "Financials",
    "642": "Financials",
    "643": "Financials",
    "649": "Financials",

    # Consumer Staples
    "101": "Consumer Staples",
    "102": "Consumer Staples",
    "103": "Consumer Staples",
    "104": "Consumer Staples",
    "105": "Consumer Staples",
    "106": "Consumer Staples",
    "107": "Consumer Staples",
    "108": "Consumer Staples",
    "111": "Consumer Staples",
    "112": "Consumer Staples",
    "463": "Consumer Staples",
    "472": "Consumer Staples",

    # Energy
    "192": "Energy",
    "199": "Energy",

    # Utilities
    "351": "Utilities",
    "352": "Utilities",
    "353": "Utilities",
    "360": "Utilities",

    # Real Estate
    "681": "Real Estate",
    "682": "Real Estate",
}


INDUTY_GICS_2 = {

    # Consumer Staples
    "10": "Consumer Staples",
    "11": "Consumer Staples",
    "46": "Consumer Staples",

    # Energy
    "19": "Energy",

    # Utilities
    "35": "Utilities",
    "36": "Utilities",

    # Real Estate
    "68": "Real Estate",

    # Financials
    "64": "Financials",
    "65": "Financials",
    "66": "Financials",
}


# 제외 업종 (C섹터 아닌 것)
EXCLUDE_3 = {
    "262", "263", "264", "265"
}

EXCLUDE_2 = {
    "14","26","27","28","29","30","31",
    "41","42",
    "55","56",
    "58","59","60","61","62","63",
    "70","71","72","73","74","75",
    "86","87","90","91"
}


def map_gics_by_code(code: str):

    if not code or not isinstance(code, str):
        return None

    code = code.strip()

    prefix3 = code[:3]
    prefix2 = code[:2]

    if prefix3 in EXCLUDE_3 or prefix2 in EXCLUDE_2:
        return None

    if prefix3 in INDUTY_GICS_3:
        return INDUTY_GICS_3[prefix3]

    if prefix2 in INDUTY_GICS_2:
        return INDUTY_GICS_2[prefix2]

    return None


def main():

    # 업종코드 로드
    df_induty = pd.read_csv(
        INDUTY_FILE,
        dtype={"종목코드": str, "induty_code": str}
    )

    print(f"업종코드 보유 기업: {len(df_induty)}개")


    # 상폐 기업 데이터 로드
    df_del = pd.read_excel(
        DELISTED_FILE,
        dtype={"종목코드": str}
    )

    df_del = df_del[["종목코드", "폐지일자", "폐지사유"]]


    # 병합
    df = df_induty.merge(df_del, on="종목코드", how="left")


    # SPAC 제거
    SPAC_KEYWORDS = ["스펙", "기업인수목적", "SPAC"]

    df = df[~df["회사명"].str.contains("|".join(SPAC_KEYWORDS), na=False)]

    print(f"SPAC 제거 후: {len(df)}개")


    # 재무적 리스크 사유 필터
    FINANCIAL_RISK_KEYWORDS = [
        "감사의견거절","감사의견 거절",
        "감사범위제한","감사범위 제한",
        "감사의견 부적정","감사의견부적정",
        "자본전액잠식","자본잠식률",
        "최종부도","부도",
        "영업손실",
        "법인세비용차감전계속사업손실",
        "법인세차감전계속사업손실",
        "매출액 미달","매출액미달",
        "시가총액",
        "계속기업",
        "회생절차",
        "파산",
        "기업의 계속성",
    ]

    EXCLUDE_REASON_KEYWORDS = [
        "피흡수합병","합병",
        "유가증권시장 상장",
        "증권거래소 상장",
        "자진등록취소",
        "상장폐지신청",
        "주식분산기준",
        "주된영업의 양도",
        "증권투자회사법",
        "불성실공시",
        "거래실적부진",
    ]


    def is_financial_risk(reason):

        if pd.isna(reason):
            return False

        if any(k in reason for k in EXCLUDE_REASON_KEYWORDS):
            return False

        return any(k in reason for k in FINANCIAL_RISK_KEYWORDS)


    before = len(df)

    df = df[df["폐지사유"].apply(is_financial_risk)]

    print(f"재무적 리스크 필터링 후: {len(df)}개")


    # GICS 매핑
    df["gics_sector"] = df["induty_code"].apply(map_gics_by_code)


    print("\n=== GICS 매핑 결과 ===")
    print(df["gics_sector"].value_counts(dropna=False))


    df = df[df["gics_sector"].notna()]

    print(f"\nC섹터 필터링 후: {len(df)}개")


    df["end_year"] = pd.to_datetime(
        df["폐지일자"], errors="coerce"
    ).dt.year.fillna(2025).astype(int)


    df["stock_code"] = df["종목코드"]
    df["corp_name"] = df["회사명"]
    df["label"] = LABEL
    df["start_year"] = START_YEAR


    df_del_final = df[
        ["stock_code","corp_name","label","gics_sector","start_year","end_year"]
    ]


    print("\n=== 상폐 기업 최종 ===")
    print(df_del_final["gics_sector"].value_counts())


    # 정상 기업 로드
    df_normal = pd.read_csv(
        NORMAL_FILE,
        dtype={"stock_code": str}
    )

    print(f"\n정상 기업: {len(df_normal)}개")


    df_final = pd.concat(
        [df_normal, df_del_final],
        ignore_index=True
    )


    df_final = df_final.drop_duplicates(
        subset=["stock_code"],
        keep="first"
    )


    print("\n=== 최종 결과 ===")
    print(f"전체 기업 수: {len(df_final)}")
    print(df_final["label"].value_counts())
    print(df_final["gics_sector"].value_counts())


    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    df_final.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"\n저장 완료: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()