alias BinanceElixir.Spot.UserStream

file = Enum.find([".env.testnet", ".env"], &File.regular?/1)

credentials =
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

key = System.get_env("BINANCE_TESTNET_API_KEY") || credentials["BINANCE_TESTNET_API_KEY"]
secret = System.get_env("BINANCE_TESTNET_API_SECRET") || credentials["BINANCE_TESTNET_API_SECRET"]

unless is_binary(key) and key != "" and is_binary(secret) and secret != "" do
  raise "testnet credentials are required"
end

client =
  BinanceElixir.new(
    base_url: "https://testnet.binance.vision",
    api_key: key,
    api_secret: secret,
    retries: 0
  )

{:ok, stream} = UserStream.start_link(client)

active? =
  Enum.reduce_while(1..20, false, fn _, _ ->
    Process.sleep(1_000)

    case UserStream.stats(stream).subscription do
      :active -> {:halt, true}
      :failed -> raise "testnet user stream subscription failed"
      _ -> {:cont, false}
    end
  end)

unless active?, do: raise("testnet user stream did not become active")
IO.puts("PASS signed testnet user stream subscription")

{order_id, _binding} = Code.eval_file("scripts/smoke_testnet_order.exs")

reports =
  Enum.reduce_while(1..40, MapSet.new(), fn _, seen ->
    Process.sleep(500)

    next =
      case UserStream.pop(stream) do
        {:ok, %{"event" => %{"e" => "executionReport", "X" => status} = event}} ->
          if event["c"] == order_id or event["C"] == order_id,
            do: MapSet.put(seen, status),
            else: seen

        _ ->
          seen
      end

    if MapSet.member?(next, "NEW") and MapSet.member?(next, "CANCELED") do
      {:halt, next}
    else
      {:cont, next}
    end
  end)

unless MapSet.member?(reports, "NEW") and MapSet.member?(reports, "CANCELED") do
  raise "testnet user stream did not emit both NEW and CANCELED execution reports; observed #{inspect(MapSet.to_list(reports))}"
end

IO.puts("PASS signed testnet user stream NEW and CANCELED execution reports")

:ok = UserStream.renew(stream)

renewed? =
  Enum.reduce_while(1..15, false, fn _, _ ->
    Process.sleep(1_000)
    status = UserStream.stats(stream)

    if status.reconnects > 0 and status.connected? and status.subscription == :active,
      do: {:halt, true},
      else: {:cont, false}
  end)

GenServer.stop(stream)
unless renewed?, do: raise("signed testnet user stream did not resubscribe after renewal")
IO.puts("PASS signed testnet user stream renewal and resubscription")
