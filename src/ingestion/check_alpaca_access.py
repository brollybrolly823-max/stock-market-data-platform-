from datetime import datetime, timedelta, timezone
from getpass import getpass

import requests


api_key = getpass("Alpaca API key: ").strip()
secret_key = getpass("Alpaca secret key: ").strip()

if not api_key or not secret_key:
    raise SystemExit("Both credentials are required.")

# An older date range, outside the recent SIP restriction.
end = datetime.now(timezone.utc) - timedelta(days=1)
start = end - timedelta(days=7)

history_params = {
    "start": start.isoformat(),
    "end": end.isoformat(),
    "limit": 1,
    "sort": "asc",
}

base_url = "https://data.alpaca.markets/v2/stocks/AAPL"

checks = [
    (
        "1. Latest IEX trade",
        f"{base_url}/trades/latest",
        {"feed": "iex"},
    ),
    (
        "2. Historical IEX trade",
        f"{base_url}/trades",
        {**history_params, "feed": "iex"},
    ),
    (
        "3. Historical SIP trade",
        f"{base_url}/trades",
        {**history_params, "feed": "sip"},
    ),
]

with requests.Session() as session:
    session.headers.update({
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
        "Accept": "application/json",
    })

    for name, url, params in checks:
        try:
            response = session.get(
                url,
                params=params,
                timeout=30,
                allow_redirects=False,
            )
        except requests.RequestException as error:
            print(f"\n{name}: {type(error).__name__}")
            continue

        print(f"\n{name}: HTTP {response.status_code}")

        if response.status_code == 200:
            print("Request accepted.")
        else:
            # Redact credentials if an error happens to include them.
            message = response.text.replace(
                api_key, "[REDACTED]"
            ).replace(
                secret_key, "[REDACTED]"
            )

            print(message[:500])