# BinanceElixir

An **unofficial** Elixir connector for **Binance Spot REST and public market WebSocket streams**. It provides a small, reusable HTTP core and common market, account, and order endpoints. Responses preserve Binance rate-limit headers and API error codes. A custom transport can be injected for tests or an application's own HTTP stack.

This is an early Spot connector release. Unit tests, public and signed Spot testnet requests, a live public market WebSocket event, and a testnet limit-order create/query/cancel cycle have passed. The connector does not yet automate unknown-order reconciliation or cover signed account WebSocket events. Do not treat this release as a production trading risk control.

## Install and run

Requires Elixir 1.15 or newer. In this project:

```sh
mix deps.get
mix test
```

For a Phoenix application, add `{:binance_elixir, git: "https://github.com/ugurbay/binance-elixir-connector.git", tag: "v0.1.0"}` to its dependencies once the release tag is available. For local development, use `{:binance_elixir, path: "../binance_elixir"}`.

```elixir
alias BinanceElixir.Spot

public = BinanceElixir.new()
{:ok, ticker} = Spot.ticker_price(public, symbol: "BTCUSDT")
IO.inspect(ticker.data)

private = BinanceElixir.new(
  api_key: System.fetch_env!("BINANCE_API_KEY"),
  api_secret: System.fetch_env!("BINANCE_API_SECRET"),
  base_url: "https://testnet.binance.vision"
)

{:ok, account} = Spot.account(private)
{:ok, test} = Spot.test_order(private, %{
  symbol: "BTCUSDT",
  side: "BUY",
  type: "LIMIT",
  timeInForce: "GTC",
  quantity: "0.00100000",
  price: "10000.00"
})
```

Use strings for prices and quantities to preserve decimal precision; floats are rejected. Read the symbol filters in `Spot.exchange_info/2` before sending orders. `Spot.place_order/2` sends a real order when pointed at production.

## API

`BinanceElixir.Spot` includes `ping`, `server_time`, `exchange_info`, `order_book`, `trades`, `klines`, `ticker_price`, `account`, `open_orders`, `order`, `place_order`, `test_order`, and `cancel_order`. Pass endpoint-specific parameters as a map or keyword list using Binance's original camelCase names.

For other Spot REST routes, use the low-level function:

```elixir
BinanceElixir.request(private, :get, "/api/v3/myTrades", %{symbol: "BTCUSDT"}, security: :signed)
```

Return values are `{:ok, %BinanceElixir.Response{data: ..., status: ..., headers: ..., rate_limits: ...}}` or `{:error, %BinanceElixir.Error{...}}`. `Spot.place_order/2` requires a `newClientOrderId` that the trading application has **persisted before** the call and includes it as `client_order_id` on errors. An error's `unknown_execution?` flag means a write may have reached Binance; reconcile by querying the order with `origClientOrderId` before taking further action. Do not submit another order while the first result remains unknown.

## Market WebSocket streams

`BinanceElixir.Spot.MarketStream` connects to Binance's combined market streams. It defaults to Spot testnet; set `environment: :production` explicitly for production market data. The connection verifies TLS, answers ping with the same pong payload, reconnects after disconnection, and restores the configured streams. It renews before the 24-hour limit and reconnects on Binance's `serverShutdown` event.

```elixir
alias BinanceElixir.Spot.MarketStream

{:ok, stream} = MarketStream.start_link(
  streams: ["btcusdt@trade", "btcusdt@bookTicker"],
  capacity: 1_024
)

case MarketStream.pop(stream) do
  {:ok, event} -> IO.inspect(event)
  :empty -> :no_event_yet
end

IO.inspect(MarketStream.stats(stream))
```

Run this process under your Phoenix application's supervisor. Check `stats(stream).dropped` and `stats(stream).reconnects`; either increase means derived market state may be incomplete and must be rebuilt from REST snapshots. A depth stream alone is not a synchronized local order book, and `dropped: 0` does not prove that no events were lost on the network. The current stream set is fixed for each connection; stop and start a new process to change it. User Data Stream and WebSocket API are not included yet.

The client retries **GET only**, with exponential backoff and Binance's `Retry-After` header. It never retries order writes. Configure `retries`, `backoff_ms`, `timeout`, `recv_window`, and `base_url` with `BinanceElixir.new/1`. The default REST URL is **production**; set `base_url: "https://testnet.binance.vision"` explicitly for Spot testnet. The HTTP transport verifies TLS certificates and does not follow redirects. API keys are hidden when inspecting the client struct. Store credentials in environment variables or a secret manager, never in source code.

Retries and backoff are per request. Applications with concurrent workers need a shared rate-limit budget and a durable order state machine before live trading. Clock synchronization, WebSocket account events, and exchange filter validation are also the application's responsibility in this version.

This first release covers Spot REST with HMAC API keys and public Spot market WebSocket streams. Signed user events, WebSocket API, Futures, and RSA/Ed25519 signing are outside its current API.

## Live Spot testnet check

Run `mix run scripts/smoke_testnet.exs` to check testnet REST connectivity, server time, BTCUSDT exchange information, order book, ticker price, and a live trade WebSocket event. For signed checks, create a local `.env.testnet` or `.env` file with `BINANCE_TESTNET_API_KEY=...` and `BINANCE_TESTNET_API_SECRET=...`, or set those environment variables. Both local files are ignored by Git. The script then checks a signed account read and `/api/v3/order/test`. The test-order endpoint validates a sample order without executing it. The script reports signed checks as skipped when credentials are unavailable; it never prints credentials or account balances.

Run `mix run scripts/smoke_testnet_order.exs` with testnet credentials to place a BTCUSDT limit order, query it by its client order ID, cancel it, and verify the final status. The script uses the fixed Spot testnet URL, computes price and quantity from current symbol filters, caps the order at 100 virtual USDT, and records its client order ID in the Git-ignored `.testnet-order-journal` directory before submission. It removes the journal entry only after confirming cancellation. If the check fails, inspect and reconcile the recorded testnet order before rerunning it.

## Sources

- [Binance Spot REST API](https://developers.binance.com/en/docs/products/spot/rest-api)
- [Binance Python connector](https://github.com/binance/binance-connector-python)
- [Binance JavaScript connector](https://github.com/binance/binance-connector-js)
- [Elixir naming conventions](https://hexdocs.pm/elixir/main/naming-conventions.html)
