from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from typing import Any, Iterable, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode
from urllib.request import Request, urlopen

import pandas as pd
from snowflake.snowpark import Session

MOLIT_API_ENDPOINTS = {
    "trade": "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
    "rent": "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
}

DEFAULT_SEOUL_LAWD_CODES = [
    "11110",
    "11140",
    "11170",
    "11200",
    "11215",
    "11230",
    "11260",
    "11290",
    "11305",
    "11320",
    "11350",
    "11380",
    "11410",
    "11440",
    "11470",
    "11500",
    "11530",
    "11545",
    "11560",
    "11590",
    "11620",
    "11650",
    "11680",
    "11710",
    "11740",
]

RAW_TRADE_TABLE = "RAW_MOLIT_APT_TRADE"
RAW_RENT_TABLE = "RAW_MOLIT_APT_RENT"

TRADE_COLUMNS = [
    "LOAD_BATCH_ID",
    "LOADED_AT",
    "SOURCE_MONTH",
    "LAWD_CD",
    "DEAL_YEAR",
    "DEAL_MONTH",
    "DEAL_DAY",
    "DEAL_DATE",
    "SGG_CD",
    "UMD_NM",
    "APT_NM",
    "APT_DONG",
    "JIBUN",
    "EXCL_AREA",
    "FLOOR",
    "BUILD_YEAR",
    "DEAL_AMOUNT",
    "DEAL_AMOUNT_KRW",
    "REGISTER_DATE",
    "CANCEL_YN",
    "CANCEL_DATE",
    "BUYER_GBN",
    "SELLER_GBN",
    "ESTATE_AGENT_SGG_NM",
    "LAND_LEASEHOLD_GBN",
    "RAW_ITEM_JSON",
    "UNIQUE_KEY",
]

RENT_COLUMNS = [
    "LOAD_BATCH_ID",
    "LOADED_AT",
    "SOURCE_MONTH",
    "LAWD_CD",
    "DEAL_YEAR",
    "DEAL_MONTH",
    "DEAL_DAY",
    "DEAL_DATE",
    "SGG_CD",
    "UMD_NM",
    "APT_NM",
    "APT_DONG",
    "JIBUN",
    "EXCL_AREA",
    "FLOOR",
    "BUILD_YEAR",
    "DEPOSIT_AMOUNT",
    "DEPOSIT_AMOUNT_KRW",
    "MONTHLY_RENT_AMOUNT",
    "MONTHLY_RENT_AMOUNT_KRW",
    "CONTRACT_TYPE",
    "CONTRACT_TERM",
    "USE_RR_RIGHT",
    "PREV_DEPOSIT_AMOUNT",
    "PREV_MONTHLY_RENT_AMOUNT",
    "ESTATE_AGENT_SGG_NM",
    "RAW_ITEM_JSON",
    "UNIQUE_KEY",
]

TRADE_ALIASES = {
    "deal_amount": ("거래금액", "dealAmount"),
    "build_year": ("건축년도", "buildYear"),
    "deal_year": ("년", "dealYear"),
    "deal_month": ("월", "dealMonth"),
    "deal_day": ("일", "dealDay"),
    "sgg_cd": ("지역코드", "sggCd"),
    "umd_nm": ("법정동", "umdNm"),
    "apt_nm": ("아파트", "aptNm"),
    "apt_dong": ("동", "aptDong"),
    "jibun": ("지번", "jibun"),
    "excl_area": ("전용면적", "excluUseAr"),
    "floor": ("층", "floor"),
    "register_date": ("등기일자", "rgstDate"),
    "cancel_yn": ("해제여부", "cdealType"),
    "cancel_date": ("해제사유발생일", "cdealDay"),
    "buyer_gbn": ("매수자", "buyerGbn"),
    "seller_gbn": ("매도자", "slerGbn"),
    "estate_agent_sgg_nm": ("중개사소재지", "estateAgentSggNm"),
    "land_leasehold_gbn": ("토지임대부아파트여부", "landLeaseholdGbn"),
}

RENT_ALIASES = {
    "deposit_amount": ("보증금액", "deposit"),
    "monthly_rent_amount": ("월세금액", "monthlyRent"),
    "build_year": ("건축년도", "buildYear"),
    "deal_year": ("년", "dealYear"),
    "deal_month": ("월", "dealMonth"),
    "deal_day": ("일", "dealDay"),
    "sgg_cd": ("지역코드", "sggCd"),
    "umd_nm": ("법정동", "umdNm"),
    "apt_nm": ("아파트", "aptNm"),
    "apt_dong": ("동", "aptDong"),
    "jibun": ("지번", "jibun"),
    "excl_area": ("전용면적", "excluUseAr"),
    "floor": ("층", "floor"),
    "contract_type": ("계약구분", "contractType"),
    "contract_term": ("계약기간", "contractTerm"),
    "use_rr_right": ("갱신요구권사용", "useRRRight"),
    "prev_deposit_amount": ("종전계약보증금", "preDeposit"),
    "prev_monthly_rent_amount": ("종전계약월세", "preMonthlyRent"),
    "estate_agent_sgg_nm": ("중개사소재지", "estateAgentSggNm"),
}


def iter_year_months(start_ym: str, end_ym: str) -> Iterator[str]:
    start = datetime.strptime(start_ym, "%Y%m")
    end = datetime.strptime(end_ym, "%Y%m")
    if start > end:
        raise ValueError("start_ym must be earlier than or equal to end_ym.")

    current = start
    while current <= end:
        yield current.strftime("%Y%m")
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def get_default_month_window(month_count: int = 24) -> tuple[str, str]:
    today = date.today()
    end = today.replace(day=1)
    year = end.year
    month = end.month

    for _ in range(month_count - 1):
        month -= 1
        if month == 0:
            year -= 1
            month = 12

    start = date(year, month, 1)
    return start.strftime("%Y%m"), end.strftime("%Y%m")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _pick(item: dict[str, Any], aliases: Iterable[str]) -> str | None:
    for alias in aliases:
        value = _clean_text(item.get(alias))
        if value is not None:
            return value
    return None


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    cleaned = _clean_text(value)
    if cleaned is None:
        return None
    normalized = cleaned.replace(",", "").replace("-", "").replace(" ", "")
    if not normalized:
        return None
    if normalized.isdigit():
        sign = -1 if "-" in cleaned else 1
        return sign * int(normalized)

    try:
        return int(float(cleaned.replace(",", "")))
    except ValueError:
        return None


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    cleaned = _clean_text(value)
    if cleaned is None:
        return None
    normalized = cleaned.replace(",", "")
    try:
        parsed = float(normalized)
    except ValueError:
        return None
    return None if math.isnan(parsed) else parsed


def _parse_date(year_value: Any, month_value: Any, day_value: Any) -> date | None:
    year = _parse_int(year_value)
    month = _parse_int(month_value)
    day = _parse_int(day_value)
    if not year or not month or not day:
        return None

    try:
        return date(year, month, day)
    except ValueError:
        return None


def _parse_freeform_date(value: Any) -> date | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None

    normalized = cleaned.replace(".", "").replace("-", "").replace("/", "")
    if len(normalized) != 8 or not normalized.isdigit():
        return None

    try:
        return datetime.strptime(normalized, "%Y%m%d").date()
    except ValueError:
        return None


def _compose_unique_key(*parts: Any) -> str:
    payload = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _item_to_dict(item_element: ET.Element) -> dict[str, str]:
    payload: dict[str, str] = {}
    for child in list(item_element):
        payload[child.tag] = _clean_text(child.text) or ""
    return payload


def _is_success_result(result_code: str | None, result_message: str | None) -> bool:
    normalized_code = (result_code or "").strip()
    normalized_message = (result_message or "").strip().upper()

    if not normalized_code:
        return True

    if normalized_code and set(normalized_code) == {"0"}:
        return True

    if normalized_message in {"OK", "NORMAL SERVICE.", "NORMAL SERVICE"}:
        return True

    return False


def _parse_xml_response(xml_payload: str) -> tuple[list[dict[str, str]], int]:
    root = ET.fromstring(xml_payload)
    result_code = _clean_text(root.findtext(".//resultCode"))
    result_message = _clean_text(root.findtext(".//resultMsg")) or "Unknown error"

    if not _is_success_result(result_code, result_message):
        raise RuntimeError(f"MOLIT API error {result_code}: {result_message}")

    total_count = _parse_int(root.findtext(".//totalCount")) or 0
    items = [_item_to_dict(item) for item in root.findall(".//item")]
    if total_count == 0 and items:
        total_count = len(items)

    return items, total_count


def normalize_molit_endpoint(endpoint: str, endpoint_type: str) -> str:
    normalized = endpoint.strip().rstrip("/")
    if not normalized:
        raise ValueError("endpoint must not be empty.")

    if normalized.endswith("getRTMSDataSvcAptTrade") or normalized.endswith("getRTMSDataSvcAptTradeDev"):
        return normalized
    if normalized.endswith("getRTMSDataSvcAptRent"):
        return normalized

    if normalized.endswith("RTMSDataSvcAptTradeDev"):
        return f"{normalized}/getRTMSDataSvcAptTradeDev"
    if normalized.endswith("RTMSDataSvcAptTrade"):
        return f"{normalized}/getRTMSDataSvcAptTrade"
    if normalized.endswith("RTMSDataSvcAptRent"):
        return f"{normalized}/getRTMSDataSvcAptRent"

    fallback_methods = {
        "trade": "getRTMSDataSvcAptTrade",
        "rent": "getRTMSDataSvcAptRent",
    }
    return f"{normalized}/{fallback_methods[endpoint_type]}"


def _redact_service_key(url: str, service_key: str) -> str:
    if not service_key:
        return url
    return url.replace(service_key, "***REDACTED***")


def fetch_molit_items(
    endpoint: str,
    service_key: str,
    lawd_cd: str,
    deal_ymd: str,
    num_of_rows: int = 1000,
    timeout_seconds: int = 30,
    max_retries: int = 5,
    initial_backoff_seconds: float = 1.5,
    request_delay_seconds: float = 0.2,
) -> Iterator[tuple[int, list[dict[str, str]], int]]:
    page_no = 1
    decoded_service_key = unquote(service_key)
    normalized_endpoint = endpoint.strip()

    while True:
        query_string = urlencode(
            {
                "serviceKey": decoded_service_key,
                "LAWD_CD": lawd_cd,
                "DEAL_YMD": deal_ymd,
                "pageNo": page_no,
                "numOfRows": num_of_rows,
            }
        )
        request_url = f"{normalized_endpoint}?{query_string}"
        safe_url = _redact_service_key(request_url, decoded_service_key)

        for attempt in range(1, max_retries + 1):
            if request_delay_seconds > 0:
                time.sleep(request_delay_seconds)

            try:
                request = Request(
                    request_url,
                    headers={
                        "User-Agent": "snowflake-loader/1.0 (+https://data.go.kr)",
                        "Accept": "application/xml,text/xml,*/*",
                    },
                )
                with urlopen(request, timeout=timeout_seconds) as response:
                    xml_payload = response.read().decode("utf-8", errors="replace")
                break
            except HTTPError as exc:
                error_body = exc.read().decode("utf-8", errors="replace")
                preview = error_body[:400].replace("\n", " ").replace("\r", " ")
                is_retryable = exc.code in {429, 500, 502, 503, 504}
                if is_retryable and attempt < max_retries:
                    sleep_seconds = min(initial_backoff_seconds * (2 ** (attempt - 1)), 20.0)
                    print(
                        f"[retry] HTTP {exc.code} for {safe_url} "
                        f"(attempt {attempt}/{max_retries}) -> sleeping {sleep_seconds:.1f}s"
                    )
                    time.sleep(sleep_seconds)
                    continue
                raise RuntimeError(
                    f"MOLIT API HTTP {exc.code} for {safe_url}. "
                    f"Response preview: {preview or exc.reason}"
                ) from exc
            except (URLError, TimeoutError) as exc:
                if attempt < max_retries:
                    sleep_seconds = min(initial_backoff_seconds * (2 ** (attempt - 1)), 20.0)
                    print(
                        f"[retry] Network error for {safe_url} "
                        f"(attempt {attempt}/{max_retries}) -> sleeping {sleep_seconds:.1f}s"
                    )
                    time.sleep(sleep_seconds)
                    continue
                raise RuntimeError(f"MOLIT API network error for {safe_url}: {exc}") from exc

        items, total_count = _parse_xml_response(xml_payload)
        yield page_no, items, total_count

        if not items or page_no * num_of_rows >= total_count:
            break
        page_no += 1


def normalize_trade_item(
    raw_item: dict[str, str],
    source_month: str,
    lawd_cd: str,
    load_batch_id: str,
) -> dict[str, Any]:
    deal_date = _parse_date(
        _pick(raw_item, TRADE_ALIASES["deal_year"]),
        _pick(raw_item, TRADE_ALIASES["deal_month"]),
        _pick(raw_item, TRADE_ALIASES["deal_day"]),
    )
    register_date = _parse_freeform_date(_pick(raw_item, TRADE_ALIASES["register_date"]))
    cancel_date = _parse_freeform_date(_pick(raw_item, TRADE_ALIASES["cancel_date"]))
    deal_amount = _parse_int(_pick(raw_item, TRADE_ALIASES["deal_amount"]))

    unique_key = _compose_unique_key(
        "trade",
        lawd_cd,
        deal_date,
        _pick(raw_item, TRADE_ALIASES["apt_nm"]),
        _pick(raw_item, TRADE_ALIASES["jibun"]),
        _pick(raw_item, TRADE_ALIASES["excl_area"]),
        _pick(raw_item, TRADE_ALIASES["floor"]),
        deal_amount,
    )

    return {
        "LOAD_BATCH_ID": load_batch_id,
        "LOADED_AT": datetime.now(timezone.utc).replace(tzinfo=None),
        "SOURCE_MONTH": source_month,
        "LAWD_CD": lawd_cd,
        "DEAL_YEAR": _parse_int(_pick(raw_item, TRADE_ALIASES["deal_year"])),
        "DEAL_MONTH": _parse_int(_pick(raw_item, TRADE_ALIASES["deal_month"])),
        "DEAL_DAY": _parse_int(_pick(raw_item, TRADE_ALIASES["deal_day"])),
        "DEAL_DATE": deal_date,
        "SGG_CD": _pick(raw_item, TRADE_ALIASES["sgg_cd"]) or lawd_cd,
        "UMD_NM": _pick(raw_item, TRADE_ALIASES["umd_nm"]),
        "APT_NM": _pick(raw_item, TRADE_ALIASES["apt_nm"]),
        "APT_DONG": _pick(raw_item, TRADE_ALIASES["apt_dong"]),
        "JIBUN": _pick(raw_item, TRADE_ALIASES["jibun"]),
        "EXCL_AREA": _parse_float(_pick(raw_item, TRADE_ALIASES["excl_area"])),
        "FLOOR": _pick(raw_item, TRADE_ALIASES["floor"]),
        "BUILD_YEAR": _parse_int(_pick(raw_item, TRADE_ALIASES["build_year"])),
        "DEAL_AMOUNT": deal_amount,
        "DEAL_AMOUNT_KRW": deal_amount * 10_000 if deal_amount is not None else None,
        "REGISTER_DATE": register_date,
        "CANCEL_YN": _pick(raw_item, TRADE_ALIASES["cancel_yn"]),
        "CANCEL_DATE": cancel_date,
        "BUYER_GBN": _pick(raw_item, TRADE_ALIASES["buyer_gbn"]),
        "SELLER_GBN": _pick(raw_item, TRADE_ALIASES["seller_gbn"]),
        "ESTATE_AGENT_SGG_NM": _pick(raw_item, TRADE_ALIASES["estate_agent_sgg_nm"]),
        "LAND_LEASEHOLD_GBN": _pick(raw_item, TRADE_ALIASES["land_leasehold_gbn"]),
        "RAW_ITEM_JSON": json.dumps(raw_item, ensure_ascii=False, sort_keys=True),
        "UNIQUE_KEY": unique_key,
    }


def normalize_rent_item(
    raw_item: dict[str, str],
    source_month: str,
    lawd_cd: str,
    load_batch_id: str,
) -> dict[str, Any]:
    deal_date = _parse_date(
        _pick(raw_item, RENT_ALIASES["deal_year"]),
        _pick(raw_item, RENT_ALIASES["deal_month"]),
        _pick(raw_item, RENT_ALIASES["deal_day"]),
    )
    deposit_amount = _parse_int(_pick(raw_item, RENT_ALIASES["deposit_amount"]))
    monthly_rent_amount = _parse_int(_pick(raw_item, RENT_ALIASES["monthly_rent_amount"]))

    unique_key = _compose_unique_key(
        "rent",
        lawd_cd,
        deal_date,
        _pick(raw_item, RENT_ALIASES["apt_nm"]),
        _pick(raw_item, RENT_ALIASES["jibun"]),
        _pick(raw_item, RENT_ALIASES["excl_area"]),
        _pick(raw_item, RENT_ALIASES["floor"]),
        deposit_amount,
        monthly_rent_amount,
        _pick(raw_item, RENT_ALIASES["contract_type"]),
    )

    return {
        "LOAD_BATCH_ID": load_batch_id,
        "LOADED_AT": datetime.now(timezone.utc).replace(tzinfo=None),
        "SOURCE_MONTH": source_month,
        "LAWD_CD": lawd_cd,
        "DEAL_YEAR": _parse_int(_pick(raw_item, RENT_ALIASES["deal_year"])),
        "DEAL_MONTH": _parse_int(_pick(raw_item, RENT_ALIASES["deal_month"])),
        "DEAL_DAY": _parse_int(_pick(raw_item, RENT_ALIASES["deal_day"])),
        "DEAL_DATE": deal_date,
        "SGG_CD": _pick(raw_item, RENT_ALIASES["sgg_cd"]) or lawd_cd,
        "UMD_NM": _pick(raw_item, RENT_ALIASES["umd_nm"]),
        "APT_NM": _pick(raw_item, RENT_ALIASES["apt_nm"]),
        "APT_DONG": _pick(raw_item, RENT_ALIASES["apt_dong"]),
        "JIBUN": _pick(raw_item, RENT_ALIASES["jibun"]),
        "EXCL_AREA": _parse_float(_pick(raw_item, RENT_ALIASES["excl_area"])),
        "FLOOR": _pick(raw_item, RENT_ALIASES["floor"]),
        "BUILD_YEAR": _parse_int(_pick(raw_item, RENT_ALIASES["build_year"])),
        "DEPOSIT_AMOUNT": deposit_amount,
        "DEPOSIT_AMOUNT_KRW": deposit_amount * 10_000 if deposit_amount is not None else None,
        "MONTHLY_RENT_AMOUNT": monthly_rent_amount,
        "MONTHLY_RENT_AMOUNT_KRW": monthly_rent_amount * 10_000 if monthly_rent_amount is not None else None,
        "CONTRACT_TYPE": _pick(raw_item, RENT_ALIASES["contract_type"]),
        "CONTRACT_TERM": _pick(raw_item, RENT_ALIASES["contract_term"]),
        "USE_RR_RIGHT": _pick(raw_item, RENT_ALIASES["use_rr_right"]),
        "PREV_DEPOSIT_AMOUNT": _parse_int(_pick(raw_item, RENT_ALIASES["prev_deposit_amount"])),
        "PREV_MONTHLY_RENT_AMOUNT": _parse_int(_pick(raw_item, RENT_ALIASES["prev_monthly_rent_amount"])),
        "ESTATE_AGENT_SGG_NM": _pick(raw_item, RENT_ALIASES["estate_agent_sgg_nm"]),
        "RAW_ITEM_JSON": json.dumps(raw_item, ensure_ascii=False, sort_keys=True),
        "UNIQUE_KEY": unique_key,
    }


def ensure_public_api_tables(session: Session, database: str, schema: str) -> None:
    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{RAW_TRADE_TABLE} (
            LOAD_BATCH_ID VARCHAR,
            LOADED_AT TIMESTAMP_NTZ,
            SOURCE_MONTH VARCHAR(6),
            LAWD_CD VARCHAR(5),
            DEAL_YEAR NUMBER(4, 0),
            DEAL_MONTH NUMBER(2, 0),
            DEAL_DAY NUMBER(2, 0),
            DEAL_DATE DATE,
            SGG_CD VARCHAR(5),
            UMD_NM VARCHAR,
            APT_NM VARCHAR,
            APT_DONG VARCHAR,
            JIBUN VARCHAR,
            EXCL_AREA FLOAT,
            FLOOR VARCHAR,
            BUILD_YEAR NUMBER(4, 0),
            DEAL_AMOUNT NUMBER(18, 0),
            DEAL_AMOUNT_KRW NUMBER(18, 0),
            REGISTER_DATE DATE,
            CANCEL_YN VARCHAR,
            CANCEL_DATE DATE,
            BUYER_GBN VARCHAR,
            SELLER_GBN VARCHAR,
            ESTATE_AGENT_SGG_NM VARCHAR,
            LAND_LEASEHOLD_GBN VARCHAR,
            RAW_ITEM_JSON VARCHAR,
            UNIQUE_KEY VARCHAR
        )
        """
    ).collect()

    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{RAW_RENT_TABLE} (
            LOAD_BATCH_ID VARCHAR,
            LOADED_AT TIMESTAMP_NTZ,
            SOURCE_MONTH VARCHAR(6),
            LAWD_CD VARCHAR(5),
            DEAL_YEAR NUMBER(4, 0),
            DEAL_MONTH NUMBER(2, 0),
            DEAL_DAY NUMBER(2, 0),
            DEAL_DATE DATE,
            SGG_CD VARCHAR(5),
            UMD_NM VARCHAR,
            APT_NM VARCHAR,
            APT_DONG VARCHAR,
            JIBUN VARCHAR,
            EXCL_AREA FLOAT,
            FLOOR VARCHAR,
            BUILD_YEAR NUMBER(4, 0),
            DEPOSIT_AMOUNT NUMBER(18, 0),
            DEPOSIT_AMOUNT_KRW NUMBER(18, 0),
            MONTHLY_RENT_AMOUNT NUMBER(18, 0),
            MONTHLY_RENT_AMOUNT_KRW NUMBER(18, 0),
            CONTRACT_TYPE VARCHAR,
            CONTRACT_TERM VARCHAR,
            USE_RR_RIGHT VARCHAR,
            PREV_DEPOSIT_AMOUNT NUMBER(18, 0),
            PREV_MONTHLY_RENT_AMOUNT NUMBER(18, 0),
            ESTATE_AGENT_SGG_NM VARCHAR,
            RAW_ITEM_JSON VARCHAR,
            UNIQUE_KEY VARCHAR
        )
        """
    ).collect()


def _records_to_dataframe(records: list[dict[str, Any]], column_order: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    if frame.empty:
        return pd.DataFrame(columns=column_order)

    frame = frame.reindex(columns=column_order)
    for column in ("LOADED_AT",):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce").dt.tz_localize(None)
    for column in ("DEAL_DATE", "REGISTER_DATE", "CANCEL_DATE"):
        if column in frame.columns:
            parsed_dates = pd.to_datetime(frame[column], errors="coerce")
            frame[column] = parsed_dates.apply(lambda value: value.date() if pd.notna(value) else None)

    if "UNIQUE_KEY" in frame.columns:
        sort_columns = [column for column in ("UNIQUE_KEY", "LOADED_AT", "LOAD_BATCH_ID") if column in frame.columns]
        if sort_columns:
            frame = frame.sort_values(sort_columns, kind="stable")
        frame = frame.drop_duplicates(subset=["UNIQUE_KEY"], keep="last").reset_index(drop=True)

    return frame


def _merge_dataframe(
    session: Session,
    dataframe: pd.DataFrame,
    database: str,
    schema: str,
    table_name: str,
    column_order: list[str],
) -> int:
    if dataframe.empty:
        return 0

    stage_name = f"{table_name}_STAGE_{uuid.uuid4().hex[:8].upper()}"
    target_name = f"{database}.{schema}.{table_name}"
    source_name = f"{database}.{schema}.{stage_name}"

    session.sql(
        f"CREATE OR REPLACE TEMP TABLE {source_name} LIKE {target_name}"
    ).collect()

    session.write_pandas(
        dataframe,
        stage_name,
        database=database,
        schema=schema,
        auto_create_table=False,
        overwrite=False,
        table_type="temp",
        use_logical_type=True,
    )

    deduped_source_name = f"{source_name}_DEDUP"
    session.sql(
        f"""
        CREATE OR REPLACE TEMP TABLE {deduped_source_name} AS
        SELECT *
        FROM {source_name}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UNIQUE_KEY
            ORDER BY LOADED_AT DESC NULLS LAST, LOAD_BATCH_ID DESC, RAW_ITEM_JSON DESC
        ) = 1
        """
    ).collect()

    update_columns = [column for column in column_order if column != "UNIQUE_KEY"]
    assignments = ", ".join(f"{column} = src.{column}" for column in update_columns)
    insert_columns = ", ".join(column_order)
    insert_values = ", ".join(f"src.{column}" for column in column_order)

    session.sql(
        f"""
        MERGE INTO {target_name} AS tgt
        USING {deduped_source_name} AS src
          ON tgt.UNIQUE_KEY = src.UNIQUE_KEY
        WHEN MATCHED THEN
          UPDATE SET {assignments}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
    ).collect()

    session.sql(f"DROP TABLE IF EXISTS {source_name}").collect()
    session.sql(f"DROP TABLE IF EXISTS {deduped_source_name}").collect()
    return len(dataframe.index)


def flush_trade_records(
    session: Session,
    records: list[dict[str, Any]],
    database: str,
    schema: str,
) -> int:
    dataframe = _records_to_dataframe(records, TRADE_COLUMNS)
    return _merge_dataframe(session, dataframe, database, schema, RAW_TRADE_TABLE, TRADE_COLUMNS)


def flush_rent_records(
    session: Session,
    records: list[dict[str, Any]],
    database: str,
    schema: str,
) -> int:
    dataframe = _records_to_dataframe(records, RENT_COLUMNS)
    return _merge_dataframe(session, dataframe, database, schema, RAW_RENT_TABLE, RENT_COLUMNS)


def ingest_molit_endpoint(
    session: Session,
    endpoint_type: str,
    service_key: str,
    lawd_codes: list[str],
    months: list[str],
    database: str,
    schema: str,
    endpoint_url: str | None = None,
    page_size: int = 1000,
    flush_every: int = 5000,
    max_retries: int = 5,
    request_delay_seconds: float = 0.2,
) -> dict[str, int]:
    if endpoint_type not in MOLIT_API_ENDPOINTS:
        raise ValueError(f"Unsupported endpoint_type: {endpoint_type}")

    ensure_public_api_tables(session, database, schema)

    endpoint = normalize_molit_endpoint(
        endpoint_url or MOLIT_API_ENDPOINTS[endpoint_type],
        endpoint_type=endpoint_type,
    )
    load_batch_id = uuid.uuid4().hex
    buffered_records: list[dict[str, Any]] = []
    total_requests = 0
    total_rows = 0
    total_written = 0

    normalizer = normalize_trade_item if endpoint_type == "trade" else normalize_rent_item
    flusher = flush_trade_records if endpoint_type == "trade" else flush_rent_records

    for source_month in months:
        for lawd_cd in lawd_codes:
            for page_no, items, _ in fetch_molit_items(
                endpoint=endpoint,
                service_key=service_key,
                lawd_cd=lawd_cd,
                deal_ymd=source_month,
                num_of_rows=page_size,
                max_retries=max_retries,
                request_delay_seconds=request_delay_seconds,
            ):
                total_requests += 1
                if not items:
                    continue

                normalized = [
                    normalizer(
                        raw_item=item,
                        source_month=source_month,
                        lawd_cd=lawd_cd,
                        load_batch_id=load_batch_id,
                    )
                    for item in items
                ]
                total_rows += len(normalized)
                buffered_records.extend(normalized)

                print(
                    f"[{endpoint_type}] {source_month} {lawd_cd} page {page_no}: "
                    f"fetched {len(items)} rows"
                )

                if len(buffered_records) >= flush_every:
                    written = flusher(session, buffered_records, database, schema)
                    total_written += written
                    buffered_records.clear()

    if buffered_records:
        written = flusher(session, buffered_records, database, schema)
        total_written += written

    return {
        "requests": total_requests,
        "rows_fetched": total_rows,
        "rows_written": total_written,
    }
