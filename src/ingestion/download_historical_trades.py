import json
import time
from datetime import datetime, timedelta, timezone
from getpass import getpass
from pathlib import Path

import requests


SYMBOL = "AAPL"
TARGET_RECORDS = 1_000_000
PAGE_SIZE = 10_000


def main():
    api_key = getpass("Alpaca API key: ").strip()
    secret_key = getpass("Alpaca secret key: ").strip()

    if not api_key or not secret_key:
        raise SystemExit("Both API credentials are required.")

    now = datetime.now(timezone.utc)

    # Use older data, safely outside the latest 15-minute window.
    end = now - timedelta(days=1)
    start = end - timedelta(days=30)

    project_root = Path(__file__).resolve().parents[2]

    output_dir = (
        project_root
        / "data"
        / "raw"
        / "historical"
        / "sip"
        / "trades"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    # A separate file for each run.
    run_id = now.strftime("%Y%m%dT%H%M%S%fZ")
    output_file = output_dir / f"{SYMBOL}_{run_id}.jsonl"

    url = (
        f"https://data.alpaca.markets"
        f"/v2/stocks/{SYMBOL}/trades"
    )

    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "feed": "sip",
        "sort": "asc",
        "limit": PAGE_SIZE,
    }

    total_records = 0

    print(f"\nStock: {SYMBOL}")
    print("Feed: historical SIP")
    print(f"Start: {start.isoformat()}")
    print(f"End: {end.isoformat()}")
    print(f"Target: {TARGET_RECORDS:,} records")
    print(f"Output: {output_file}\n")

    with requests.Session() as session:
        session.headers.update({
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
        })

        with output_file.open("x", encoding="utf-8") as file:
            while total_records < TARGET_RECORDS:
                params["limit"] = min(
                    PAGE_SIZE,
                    TARGET_RECORDS - total_records,
                )

                # Retry temporary server errors and rate limits.
                for attempt in range(5):
                    response = session.get(
                        url,
                        params=params,
                        timeout=60,
                    )

                    if response.status_code not in (
                        429, 500, 502, 503, 504
                    ):
                        break

                    delay = min(2 ** (attempt + 1), 30)
                    print(
                        f"HTTP {response.status_code}; "
                        f"retrying after {delay} seconds..."
                    )
                    time.sleep(delay)

                if response.status_code >= 400:
                    raise SystemExit(
                        f"HTTP {response.status_code}: "
                        f"{response.text}"
                    )

                result = response.json()
                trades = result.get("trades") or []

                received_at = datetime.now(
                    timezone.utc
                ).isoformat()

                for trade in trades:
                    event = {
                        "source": "alpaca",
                        "feed": "sip",
                        "ingestion_mode": "historical",
                        "symbol": SYMBOL,
                        "received_at_utc": received_at,
                        "payload": trade,
                    }

                    file.write(
                        json.dumps(
                            event,
                            separators=(",", ":"),
                        ) + "\n"
                    )

                total_records += len(trades)
                file.flush()

                print(f"Saved {total_records:,} records")

                next_page = result.get("next_page_token")

                if not next_page:
                    break

                params["page_token"] = next_page

                # Pace this downloader below the Basic rate limit.
                time.sleep(0.6)

    print(f"\nFinished: {total_records:,} records saved")
    print(f"File: {output_file}")

    if total_records < TARGET_RECORDS:
        print("The requested date range has been exhausted.")


if __name__ == "__main__":
    main()