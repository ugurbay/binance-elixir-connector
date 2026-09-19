# BinanceElixir

Unofficial Binance **Spot** connector for Elixir and Phoenix: public and signed REST, public market WebSocket streams, and signed User Data Stream events over the Binance WebSocket API. This is a connector, not a trading strategy or a complete order management system. Futures and Margin are outside its scope.

## Install

Requires Elixir 1.15 or newer. Add to your application's `mix.exs`:

```elixir
{:binance_elixir, "~> 0.2.1"}
```

For local development, use `{:binance_elixir, path: "../binance-elixir-connector"}`. Run `mix deps.get` and `mix test` here.

## REST and order safety

The default REST host is **Spot testnet**. Production requires an explicit `base_url`; write requests also require `enable_live_trading?: true`. HTTP writes are never automatically retried. Use decimal strings or `Decimal` values for prices and quantities, never floats.

```elixir
alias BinanceElixir.Spot

public = BinanceElixir.new()
{:ok, ticker} = Spot.ticker_price(public, symbol: "BTCUSDT")

private = BinanceElixir.new(
  api_key: System.fetch_env!("BINANCE_TESTNET_API_KEY"),
  api_secret: System.fetch_env!("BINANCE_TESTNET_API_SECRET")
)

{:ok, account} = Spot.account(private)
{:ok, info} = Spot.exchange_info(private, symbol: "BTCUSDT")
symbol_info = Enum.find(info.data["symbols"], &(&1["symbol"] == "BTCUSDT"))

{:ok, validated} = Spot.validate_order(symbol_info, %{
  symbol: "BTCUSDT", side: "BUY", type: "LIMIT", timeInForce: "GTC",
  quantity: "0.001", price: "10000", newClientOrderId: "my-unique-id"
})
```

`Spot.validate_order/3` checks supported order types against `exchangeInfo` filters using exact decimal arithmetic. It never rounds an invalid order into validity. For an executable order, use `Spot.submit_order/4` with a **durable** `:persist` callback that reserves each `newClientOrderId` before submission, including across application restarts. A Phoenix application can reserve the ID in a database transaction with a unique constraint. The connector submits once, then queries by client order ID if the outcome is unknown. Keep an `:unresolved` ID reserved and reconcile it from REST or execution reports before making another trading decision.

The lower level `Spot.place_order/2` requires a caller supplied client order ID and returns `unknown_execution?: true` for uncertain writes. It does not perform filter preflight or durable reservation. `Spot.test_order/2` checks an order without executing it.

The Spot module includes `ping`, `server_time`, `exchange_info`, `order_book`, `trades`, `klines`, `ticker_price`, `ticker_24h`, `book_ticker`, `account`, `my_trades`, `open_orders`, `order`, `place_order`, `test_order`, and `cancel_order`. Endpoint parameters use Binance's camelCase names. `BinanceElixir.request/5` supports additional Spot REST paths. Responses are `{:ok, %BinanceElixir.Response{}}` or `{:error, %BinanceElixir.Error{}}` and retain API error codes and rate limit headers.

For Ed25519 keys, set `signing_algorithm: :ed25519` and `private_key:` to an unencrypted Ed25519 PKCS#8 PEM string. HMAC uses `api_secret:`. A live Ed25519 testnet key has not yet been supplied; Ed25519 signing is covered by a published test vector and unit tests.

## WebSocket streams

Run both streams under your application's supervisor. Both use verified TLS, bounded queues, reconnection, and renewal before Binance's 24 hour connection limit. Watch `dropped`, `decode_errors`, and `reconnects` in `stats/1`. After a disconnect or dropped event, rebuild derived account or book state from REST snapshots. A depth stream alone is not a synchronized local order book.

```elixir
alias BinanceElixir.Spot.{MarketStream, UserStream}

{:ok, market} = MarketStream.start_link(streams: [MarketStream.trades("BTCUSDT")])
:ok = MarketStream.subscribe(market, MarketStream.book_ticker("BTCUSDT"))
case MarketStream.pop_normalized(market) do
  {:ok, event} -> IO.inspect(event)
  :empty -> :no_event_yet
end
:ok = MarketStream.unsubscribe(market, MarketStream.trades("BTCUSDT"))

{:ok, user} = UserStream.start_link(private)
# The signed subscription becomes active asynchronously; check UserStream.stats(user).
case UserStream.pop_normalized(user) do
  {:ok, event} -> IO.inspect(event)
  :empty -> :no_event_yet
end
```

Market streams default to Spot testnet. Set `environment: :production` explicitly for production market data. Dynamic market subscription changes are batched into WebSocket control frames; the desired set is restored after reconnect. Control acknowledgments remain in the raw event queue. The user stream signs each subscription with a fresh timestamp. `UserStream.signed_request/2` contains credentials and must never be logged. `pop/1` returns raw Binance messages; `pop_normalized/1` returns normalized maps while retaining the raw payload. The WebSocket API is currently used for signed User Data Stream subscriptions, not WebSocket order placement.

## Time and rate limits

`BinanceElixir.Clock` synchronizes against `/api/v3/time` using a request midpoint. Pass `clock: fn -> BinanceElixir.Clock.now(clock_pid) end` to the client and resynchronize periodically. `BinanceElixir.RateLimit` records shared request attempts and observed Binance rate limit headers across clients. Pass `rate_tracker: tracker_pid` when creating clients. It is an observer, not a quota enforcement scheduler; the trading application must budget concurrent requests and obey `429`/`418` responses. GET requests use bounded backoff; writes are never retried.

## Testnet verification

Set `BINANCE_TESTNET_API_KEY` and `BINANCE_TESTNET_API_SECRET` as environment variables or in a local `.env` file. Local credential files are ignored by Git. Never put real keys in `.env.example` or commits.

- `mix run scripts/smoke_testnet.exs`: public REST, market WebSocket, clock, rate tracking, signed account reads, and a nonexecuting test order.
- `mix run scripts/smoke_testnet_order.exs`: bounded Spot testnet order create, query, cancel, and confirmation. It caps exposure at 100 virtual USDT and keeps an ignored journal until cancellation is confirmed.
- `mix run scripts/smoke_user_stream.exs`: signed WebSocket subscription, order execution reports, renewal, and resubscription.

The live scripts require network access and are separate from the deterministic unit suite. Check `.testnet-order-journal` for unresolved test orders before rerunning an order script.

## References

- [Binance Spot REST API](https://developers.binance.com/en/docs/binance-spot-api-docs/rest-api)
- [Binance Spot WebSocket streams](https://developers.binance.com/en/docs/binance-spot-api-docs/web-socket-streams)
- [Binance Spot WebSocket API](https://developers.binance.com/en/docs/binance-spot-api-docs/websocket-api/general-api-information)
- [Elixir naming conventions](https://hexdocs.pm/elixir/naming-conventions.html)
