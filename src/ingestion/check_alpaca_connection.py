from getpass import getpass

from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


# Credentials are entered privately when the program runs.
api_key = getpass("Enter your Alpaca API key: ").strip()
secret_key = getpass("Enter your Alpaca secret key: ").strip()

if not api_key or not secret_key:
    raise SystemExit("Please provide both credentials.")

client = StockHistoricalDataClient(
    api_key=api_key,
    secret_key=secret_key,
)

request = StockLatestTradeRequest(
    symbol_or_symbols=["AAPL", "MSFT"],
    feed=DataFeed.IEX,
)

trades = client.get_stock_latest_trade(request)

if not trades:
    raise SystemExit("The request returned no trade records.")

print("\nConnection successful!")
print("Latest available IEX trades:\n")

for symbol, trade in trades.items():
    print(f"Stock: {symbol}")
    print(f"Price: {trade.price}")
    print(f"Shares: {trade.size:g}")
    print(f"Trade time: {trade.timestamp.isoformat()}")
    print()