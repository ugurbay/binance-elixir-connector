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

client = BinanceElixir.new(base_url: "https://testnet.binance.vision", retries: 0)

checks = [
  {:ping, fn -> Spot.ping(client) end, fn data -> data == %{} end},
  {:server_time, fn -> Spot.server_time(client) end,
   fn data -> is_integer(data["serverTime"]) end},
  {:exchange_info, fn -> Spot.exchange_info(client, symbol: "BTCUSDT") end,
   fn data -> Enum.any?(data["symbols"] || [], &(&1["symbol"] == "BTCUSDT")) end},
  {:order_book, fn -> Spot.order_book(client, "BTCUSDT", limit: 5) end,
   fn data -> is_list(data["bids"]) and is_list(data["asks"]) end},
  {:ticker_price, fn -> Spot.ticker_price(client, symbol: "BTCUSDT") end,
   fn data -> data["symbol"] == "BTCUSDT" and is_binary(data["price"]) end}
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

key = System.get_env("BINANCE_TESTNET_API_KEY") || credentials["BINANCE_TESTNET_API_KEY"]
secret = System.get_env("BINANCE_TESTNET_API_SECRET") || credentials["BINANCE_TESTNET_API_SECRET"]

if is_binary(key) and key != "" and is_binary(secret) and secret != "" do
  signed_client =
    BinanceElixir.new(
      base_url: "https://testnet.binance.vision",
      api_key: key,
      api_secret: secret,
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
else
  IO.puts("SKIP signed account and test order: testnet credentials are unavailable")
end
