alias BinanceElixir.Spot.MarketStream

{:ok, stream} =
  MarketStream.start_link(streams: ["btcusdt@trade"], environment: :production, capacity: 100)

Process.sleep(5_000)

case {MarketStream.stats(stream), MarketStream.pop(stream)} do
  {%{connected?: true} = stats, {:ok, %{"stream" => "btcusdt@trade", "data" => %{"e" => "trade"}}}} ->
    IO.inspect(stats, label: "market stream connected")

  {stats, event} ->
    IO.inspect({stats, event}, label: "market stream smoke failure")
    System.halt(1)
end

GenServer.stop(stream)
