defmodule BinanceElixir.Clock do
  @moduledoc """
  A supervised clock with a Binance server-time offset.

  Supply `clock: fn -> BinanceElixir.Clock.now(clock) end` when constructing a
  client. Synchronize before signed requests and periodically thereafter.
  """

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @spec now(GenServer.server()) :: non_neg_integer()
  def now(clock), do: GenServer.call(clock, :now)

  @spec offset_ms(GenServer.server()) :: integer()
  def offset_ms(clock), do: GenServer.call(clock, :offset)

  @doc "Reads `/api/v3/time` and applies the midpoint offset estimate."
  @spec synchronize(BinanceElixir.t(), GenServer.server()) ::
          {:ok, integer()} | {:error, BinanceElixir.Error.t()}
  def synchronize(client, clock) do
    started = GenServer.call(clock, :base_now)

    result = BinanceElixir.Spot.server_time(client)
    finished = GenServer.call(clock, :base_now)

    case result do
      {:ok, %{data: %{"serverTime" => server_time}}}
      when is_integer(server_time) and server_time >= 0 ->
        GenServer.call(clock, {:synchronize, server_time, started, finished})

      {:ok, _} ->
        {:error, %BinanceElixir.Error{type: :decode, message: "serverTime is missing or invalid"}}

      error ->
        error
    end
  end

  @impl true
  def init(opts) do
    base_clock = Keyword.get(opts, :base_clock, fn -> System.system_time(:millisecond) end)

    if is_function(base_clock, 0) do
      {:ok, %{base_clock: base_clock, offset: 0}}
    else
      {:stop, :invalid_base_clock}
    end
  end

  @impl true
  def handle_call(:base_now, _from, state), do: {:reply, state.base_clock.(), state}
  def handle_call(:now, _from, state), do: {:reply, state.base_clock.() + state.offset, state}
  def handle_call(:offset, _from, state), do: {:reply, state.offset, state}

  def handle_call({:synchronize, server, started, finished}, _from, state)
      when is_integer(started) and is_integer(finished) and finished >= started do
    offset = server - div(started + finished, 2)
    {:reply, {:ok, offset}, %{state | offset: offset}}
  end
end
