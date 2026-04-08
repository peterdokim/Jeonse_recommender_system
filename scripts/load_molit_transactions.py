from __future__ import annotations

import argparse
import sys
from pathlib import Path

from snowflake.snowpark import Session

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.molit_loader import (  # noqa: E402
    DEFAULT_SEOUL_LAWD_CODES,
    RAW_RENT_TABLE,
    RAW_TRADE_TABLE,
    get_default_month_window,
    ingest_molit_endpoint,
    iter_year_months,
)
from common.settings import (  # noqa: E402
    get_local_snowflake_connection_config,
    get_public_data_lawd_codes,
    get_public_data_service_key,
    get_public_data_target_config,
)


def build_session() -> Session:
    config = get_local_snowflake_connection_config()
    session = Session.builder.configs(config).create()

    if config.get("role"):
        session.sql(f'USE ROLE "{config["role"]}"').collect()
    session.sql(f'USE WAREHOUSE "{config["warehouse"]}"').collect()
    session.sql(f'USE DATABASE "{config["database"]}"').collect()
    session.sql(f'USE SCHEMA "{config["schema"]}"').collect()
    return session


def parse_args() -> argparse.Namespace:
    default_start_month, default_end_month = get_default_month_window()

    parser = argparse.ArgumentParser(
        description="Load MOLIT apartment sale and rent public API data into Snowflake raw tables."
    )
    parser.add_argument(
        "--target",
        choices=["trade", "rent", "all"],
        default="all",
        help="Which public API dataset to ingest.",
    )
    parser.add_argument(
        "--start-month",
        default=default_start_month,
        help="Start contract year-month in YYYYMM format. Default is the last 24 months.",
    )
    parser.add_argument(
        "--end-month",
        default=default_end_month,
        help="End contract year-month in YYYYMM format.",
    )
    parser.add_argument(
        "--lawd-codes",
        default="",
        help="Comma-separated 5-digit legal district codes. Defaults to secrets or all Seoul districts.",
    )
    parser.add_argument(
        "--service-key",
        default="",
        help="Public Data Portal service key. Defaults to public_data_api.service_key.",
    )
    parser.add_argument(
        "--database",
        default="HACKATHON_APP",
        help="Snowflake database name.",
    )
    parser.add_argument(
        "--schema",
        default="RESILIENCE",
        help="Snowflake schema name.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Rows requested per API page. Lower values are slower but usually more stable for data.go.kr.",
    )
    parser.add_argument(
        "--flush-every",
        type=int,
        default=5000,
        help="Flush buffered rows to Snowflake after this many records.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retry count for transient HTTP/network failures from the public API.",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.2,
        help="Delay in seconds between API requests to reduce gateway errors.",
    )
    return parser.parse_args()


def resolve_lawd_codes(raw_value: str) -> list[str]:
    if raw_value.strip():
        return [code.strip() for code in raw_value.split(",") if code.strip()]

    configured = get_public_data_lawd_codes()
    if configured:
        return configured

    return DEFAULT_SEOUL_LAWD_CODES


def main() -> int:
    args = parse_args()
    lawd_codes = resolve_lawd_codes(args.lawd_codes)
    months = list(iter_year_months(args.start_month, args.end_month))
    targets = ["trade", "rent"] if args.target == "all" else [args.target]

    print(
        "Starting MOLIT ingestion with "
        f"targets={targets}, months={months[0]}..{months[-1]}, "
        f"lawd_codes={len(lawd_codes)}"
    )

    session = build_session()
    try:
        for target in targets:
            target_config = get_public_data_target_config(target)
            service_key = args.service_key.strip() or target_config.get("service_key") or get_public_data_service_key()
            endpoint_url = target_config.get("endpoint")

            summary = ingest_molit_endpoint(
                session=session,
                endpoint_type=target,
                service_key=service_key,
                lawd_codes=lawd_codes,
                months=months,
                database=args.database,
                schema=args.schema,
                endpoint_url=endpoint_url,
                page_size=args.page_size,
                flush_every=args.flush_every,
                max_retries=args.max_retries,
                request_delay_seconds=args.request_delay,
            )
            target_table = RAW_TRADE_TABLE if target == "trade" else RAW_RENT_TABLE
            print(
                f"[{target}] finished -> endpoint={endpoint_url}, "
                f"table={args.database}.{args.schema}.{target_table}, "
                f"requests={summary['requests']}, rows_fetched={summary['rows_fetched']}, "
                f"rows_written={summary['rows_written']}"
            )
    finally:
        session.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
