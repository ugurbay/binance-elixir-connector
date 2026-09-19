defmodule TestnetOrderSmoke do
  alias BinanceElixir.Spot

  @scale 100_000_000

  def run do
    credentials = read_credentials()
    key = System.get_env("BINANCE_TESTNET_API_KEY") || credentials["BINANCE_TESTNET_API_KEY"]
    secret = System.get_env("BINANCE_TESTNET_API_SECRET") || credentials["BINANCE_TESTNET_API_SECRET"]

    if not (is_binary(key) and key != "" and is_binary(secret) and secret != "") do
      raise "testnet credentials are required for the order lifecycle check"
    end

    client =
      BinanceElixir.new(
        base_url: "https://testnet.binance.vision",
        api_key: key,
        api_secret: secret,
        retries: 0
      )

    %{"symbols" => [symbol]} = data!(Spot.exchange_info(client, symbol: "BTCUSDT"))
    %{"price" => market_price} = data!(Spot.ticker_price(client, symbol: "BTCUSDT"))
    %{"balances" => balances} = data!(Spot.account(client))
    filters = Map.new(symbol["filters"], &{&1["filterType"], &1})
    price_filter = Map.fetch!(filters, "PRICE_FILTER")
    lot_filter = Map.fetch!(filters, "LOT_SIZE")
    tick = decimal!(price_filter["tickSize"])
    step = decimal!(lot_filter["stepSize"])
    min_price = decimal!(price_filter["minPrice"])
    min_qty = decimal!(lot_filter["minQty"])
    min_notional =
      decimal!(get_in(filters, ["NOTIONAL", "minNotional"]) ||
                 get_in(filters, ["MIN_NOTIONAL", "minNotional"]) || "0")

    if tick <= 0 or step <= 0, do: raise("invalid BTCUSDT exchange filters")

    price = max(min_price, div(div(decimal!(market_price) * 9, 10), tick) * tick)
    target_notional = max(25 * @scale, min_notional * 2)

    if target_notional > 100 * @scale, do: raise("test notional exceeds 100 testnet USDT")

    qty_steps = ceil_div(target_notional * @scale, price * step)
    quantity = max(min_qty, qty_steps * step)
    notional = div(price * quantity, @scale)
    usdt = Enum.find(balances, &(&1["asset"] == "USDT"))

    if is_nil(usdt) or decimal!(usdt["free"]) < notional do
      raise "insufficient testnet USDT for the bounded order check"
    end

    id = "codexT" <> (:crypto.strong_rand_bytes(10) |> Base.encode16(case: :lower))
    journal_dir = ".testnet-order-journal"
    File.mkdir_p!(journal_dir)
    journal = Path.join(journal_dir, id <> ".json")

    File.write!(
      journal,
      Jason.encode!(%{environment: "spot_testnet", symbol: "BTCUSDT", client_order_id: id})
    )

    params = %{
      symbol: "BTCUSDT",
      side: "BUY",
      type: "LIMIT",
      timeInForce: "GTC",
      price: format_decimal(price),
      quantity: format_decimal(quantity),
      newClientOrderId: id
    }

    placed = Spot.place_order(client, params)
    queried = Spot.order(client, %{symbol: "BTCUSDT", origClientOrderId: id})
    canceled = Spot.cancel_order(client, %{symbol: "BTCUSDT", origClientOrderId: id})
    final = Spot.order(client, %{symbol: "BTCUSDT", origClientOrderId: id})

    case {placed, queried, canceled, final} do
      {{:ok, %{data: %{"clientOrderId" => ^id}}},
       {:ok, %{data: %{"clientOrderId" => ^id}}},
       {:ok, %{data: %{"status" => "CANCELED"}}},
       {:ok, %{data: %{"status" => "CANCELED"}}}} ->
        File.rm!(journal)
        IO.puts("PASS testnet order create, query, cancel, and verify")

      results ->
        summary =
          Enum.map_join(Tuple.to_list(results), ", ", fn
            {:ok, %{data: data}} -> "ok:#{data["status"]}"
            {:error, error} -> "error:#{error.status}/#{error.code}/#{error.message}"
          end)

        raise "testnet order lifecycle failed for #{id}; journal retained; #{summary}"
    end
  end

  defp read_credentials do
    file = Enum.find([".env.testnet", ".env"], &File.regular?/1)

    if file do
      file
      |> File.read!()
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
    else
      %{}
    end
  end

  defp data!({:ok, %{status: 200, data: data}}), do: data

  defp data!({:error, error}),
    do: raise("testnet read failed: #{error.type}/#{error.status}/#{error.code}")

  defp decimal!(value) when is_binary(value) do
    case String.split(value, ".", parts: 2) do
      [whole, fraction] when byte_size(fraction) <= 8 ->
        String.to_integer(whole) * @scale +
          String.to_integer(String.pad_trailing(fraction, 8, "0"))

      [whole] ->
        String.to_integer(whole) * @scale
    end
  end

  defp format_decimal(value) do
    whole = div(value, @scale)
    fraction = rem(value, @scale) |> Integer.to_string() |> String.pad_leading(8, "0")
    "#{whole}.#{String.trim_trailing(fraction, "0") |> then(&if(&1 == "", do: "0", else: &1))}"
  end

  defp ceil_div(numerator, denominator), do: div(numerator + denominator - 1, denominator)
end

TestnetOrderSmoke.run()
