alias BinanceElixir.Spot
alias BinanceElixir.Spot.MarketStream

credential_file = Enum.find([".env.testnet", ".env"], &File.regular?/1)

credentials =
  case credential_file && File.read(credential_file) do
    {:ok, contents} ->
      contents
      |> String.split(~r/\r?\n/)
      |> Enum.reduce(%{}, fn line, acc ->
        case String.split(String.trim(line), "=", parts: 2) do
          [name, value]
          when name in ["BINANCE_TESTNET_API_KEY", "BINANCE_TESTNET_API_SECRET"] ->
            Map.put(acc, name, String.trim(value))

          _ ->
            acc
        end
      end)

    nil ->
      %{}

    {:error, reason} ->
      raise "cannot read local credential file: #{inspect(reason)}"
  end

{:ok, tracker} = BinanceElixir.RateLimit.start_link()

client =
  BinanceElixir.new(
    base_url: "https://testnet.binance.vision",
    retries: 0,
    rate_tracker: tracker
  )

checks = [
  {:ping, fn -> Spot.ping(client) end, fn data -> data == %{} end},
  {:server_time, fn -> Spot.server_time(client) end,
   fn data -> is_integer(data["serverTime"]) end},
  {:exchange_info, fn -> Spot.exchange_info(client, symbol: "BTCUSDT") end,
   fn data -> Enum.any?(data["symbols"] || [], &(&1["symbol"] == "BTCUSDT")) end},
  {:order_book, fn -> Spot.order_book(client, "BTCUSDT", limit: 5) end,
   fn data -> is_list(data["bids"]) and is_list(data["asks"]) end},
  {:ticker_price, fn -> Spot.ticker_price(client, symbol: "BTCUSDT") end,
   fn data -> data["symbol"] == "BTCUSDT" and is_binary(data["price"]) end},
  {:ticker_24h, fn -> Spot.ticker_24h(client, symbol: "BTCUSDT") end,
   fn data -> data["symbol"] == "BTCUSDT" end},
  {:book_ticker, fn -> Spot.book_ticker(client, symbol: "BTCUSDT") end,
   fn data -> data["symbol"] == "BTCUSDT" and is_binary(data["bidPrice"]) end}
]

Enum.each(checks, fn {name, request, valid?} ->
  case request.() do
    {:ok, %{status: 200, data: data}} ->
      if valid?.(data), do: IO.puts("PASS REST #{name}"), else: raise("invalid #{name} response")

    {:error, error} ->
      raise "REST #{name} failed: type=#{error.type} status=#{error.status} code=#{error.code}"

    other ->
      raise "REST #{name} returned unexpected result: #{inspect(other)}"
  end
end)

rate_snapshot = BinanceElixir.RateLimit.snapshot(tracker)

if rate_snapshot.raw_request_count < 5 or map_size(rate_snapshot.request_weight) == 0 do
  raise "testnet rate-limit tracker did not capture request attempts and Binance headers"
end

IO.puts("PASS shared rate-limit observations")

{:ok, stream} = MarketStream.start_link(streams: ["btcusdt@trade"], environment: :testnet)

event =
  Enum.reduce_while(1..20, nil, fn _, _ ->
    Process.sleep(1_000)

    case MarketStream.pop(stream) do
      {:ok, %{"stream" => "btcusdt@trade", "data" => %{"e" => "trade"}} = event} ->
        {:halt, event}

      _ ->
        {:cont, nil}
    end
  end)

stats = MarketStream.stats(stream)
GenServer.stop(stream)

if is_nil(event), do: raise("testnet WebSocket did not receive a BTCUSDT trade in 20 seconds")
if stats.decode_errors > 0, do: raise("testnet WebSocket reported decode errors")
IO.puts("PASS WebSocket btcusdt@trade")

{:ok, renewal_stream} =
  MarketStream.start_link(streams: ["btcusdt@trade"], environment: :testnet)

Process.sleep(1_000)
:ok = MarketStream.subscribe(renewal_stream, "btcusdt@bookTicker")

dynamic_event? =
  Enum.reduce_while(1..15, false, fn _, _ ->
    Process.sleep(1_000)

    found? =
      Enum.reduce_while(1..1_024, false, fn _, _ ->
        case MarketStream.pop(renewal_stream) do
          {:ok, %{"stream" => "btcusdt@bookTicker"}} -> {:halt, true}
          {:ok, _} -> {:cont, false}
          :empty -> {:halt, false}
        end
      end)

    if found?, do: {:halt, true}, else: {:cont, false}
  end)

unless dynamic_event?, do: raise("testnet market WebSocket dynamic subscription received no event")
:ok = MarketStream.unsubscribe(renewal_stream, "btcusdt@trade")
unless MarketStream.subscriptions(renewal_stream) == ["btcusdt@bookTicker"],
  do: raise("testnet market WebSocket subscription set is incorrect")

IO.puts("PASS WebSocket dynamic subscribe and unsubscribe")
:ok = MarketStream.renew(renewal_stream)

renewed? =
  Enum.reduce_while(1..15, false, fn _, _ ->
    Process.sleep(1_000)
    renewed = MarketStream.stats(renewal_stream)
    if renewed.connected? and renewed.reconnects > 0, do: {:halt, true}, else: {:cont, false}
  end)

GenServer.stop(renewal_stream)
unless renewed?, do: raise("testnet market WebSocket did not reconnect after renewal")
IO.puts("PASS WebSocket renewal and stream restore")

key = System.get_env("BINANCE_TESTNET_API_KEY") || credentials["BINANCE_TESTNET_API_KEY"]
secret = System.get_env("BINANCE_TESTNET_API_SECRET") || credentials["BINANCE_TESTNET_API_SECRET"]

if is_binary(key) and key != "" and is_binary(secret) and secret != "" do
  {:ok, clock} = BinanceElixir.Clock.start_link()
  {:ok, offset} = BinanceElixir.Clock.synchronize(client, clock)
  if abs(offset) > 5_000, do: raise("testnet clock offset exceeds five seconds")
  IO.puts("PASS server-time synchronization")

  signed_client =
    BinanceElixir.new(
      base_url: "https://testnet.binance.vision",
      api_key: key,
      api_secret: secret,
      clock: fn -> BinanceElixir.Clock.now(clock) end,
      retries: 0
    )

  case Spot.account(signed_client) do
    {:ok, %{status: 200, data: %{"balances" => balances}}} when is_list(balances) ->
      IO.puts("PASS signed account")

    {:error, error} ->
      raise "signed account failed: type=#{error.type} status=#{error.status} code=#{error.code}"

    other ->
      raise "signed account returned unexpected result: #{inspect(other)}"
  end

  case Spot.my_trades(signed_client, "BTCUSDT", limit: 5) do
    {:ok, %{status: 200, data: trades}} when is_list(trades) ->
      IO.puts("PASS signed my_trades")

    {:error, error} ->
      raise "signed my_trades failed: type=#{error.type} status=#{error.status} code=#{error.code}"

    other ->
      raise "signed my_trades returned unexpected result: #{inspect(other)}"
  end

  case Spot.test_order(signed_client, %{
         symbol: "BTCUSDT",
         side: "BUY",
         type: "MARKET",
         quoteOrderQty: "10.00"
       }) do
    {:ok, %{status: 200, data: %{}}} -> IO.puts("PASS signed test order (no execution)")
    {:error, error} ->
      raise "signed test order failed: type=#{error.type} status=#{error.status} code=#{error.code}"
    other -> raise "signed test order returned unexpected result: #{inspect(other)}"
  end

  GenServer.stop(clock)
else
  IO.puts("SKIP signed account and test order: testnet credentials are unavailable")
end
