defmodule BinanceElixir.RateLimit do
  @moduledoc "Shared, supervised observation of Binance REST rate-limit headers."

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @spec record_attempt(GenServer.server(), non_neg_integer()) :: :ok
  def record_attempt(server, weight \\ 0), do: GenServer.call(server, {:attempt, weight})

  @spec record_response(GenServer.server(), map() | [{String.t(), String.t()}]) :: :ok
  def record_response(server, headers), do: GenServer.call(server, {:response, headers})

  @spec snapshot(GenServer.server()) :: map()
  def snapshot(server), do: GenServer.call(server, :snapshot)

  @impl true
  def init(opts) do
    clock = Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end)

    {:ok,
     %{
       clock: clock,
       raw_request_count: 0,
       estimated_request_weight: 0,
       request_weight: %{},
       orders: %{},
       retry_after_seconds: nil,
       updated_at_ms: nil
     }}
  end

  @impl true
  def handle_call({:attempt, weight}, _from, state)
      when is_integer(weight) and weight >= 0 do
    {:reply, :ok,
     %{
       state
       | raw_request_count: state.raw_request_count + 1,
         estimated_request_weight: state.estimated_request_weight + weight,
         updated_at_ms: state.clock.()
     }}
  end

  def handle_call({:response, headers}, _from, state) do
    headers =
      Map.new(headers, fn {key, value} -> {String.downcase(to_string(key)), to_string(value)} end)

    weight = parse_observations(headers, ~r/\Ax-mbx-used-weight-(\d+)([smhd])\z/)
    orders = parse_observations(headers, ~r/\Ax-mbx-order-count-(\d+)([smhd])\z/)

    retry_after =
      case Integer.parse(Map.get(headers, "retry-after", "")) do
        {seconds, ""} when seconds >= 0 -> seconds
        _ -> nil
      end

    {:reply, :ok,
     %{
       state
       | request_weight: Map.merge(state.request_weight, weight),
         orders: Map.merge(state.orders, orders),
         retry_after_seconds: retry_after,
         updated_at_ms: state.clock.()
     }}
  end

  def handle_call(:snapshot, _from, state), do: {:reply, Map.delete(state, :clock), state}

  defp parse_observations(headers, pattern) do
    Enum.reduce(headers, %{}, fn {key, value}, acc ->
      with [_, count, unit] <- Regex.run(pattern, key),
           {number, ""} when number >= 0 <- Integer.parse(value) do
        Map.put(acc, {String.to_integer(count), unit}, number)
      else
        _ -> acc
      end
    end)
  end
end
