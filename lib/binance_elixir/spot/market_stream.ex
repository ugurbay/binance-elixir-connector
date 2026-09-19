defmodule BinanceElixir.Spot.MarketStream do
  @moduledoc """
  Supervised Binance Spot market WebSocket stream with a bounded event queue.

  Subscriptions can change while connected and are restored after reconnect.
  `dropped` in `stats/1`
  means events were lost; consumers must resynchronize any derived state.
  """

  use GenServer

  alias BinanceElixir.Spot.Events

  @production_url "wss://stream.binance.com:9443"
  @testnet_url "wss://stream.testnet.binance.vision"
  @default_capacity 1_024

  @doc "Builds a symbol trade stream name."
  def trades(symbol), do: symbol_stream(symbol, "trade")

  @doc "Builds a symbol aggregate-trade stream name."
  def aggregate_trades(symbol), do: symbol_stream(symbol, "aggTrade")

  @doc "Builds a symbol best-bid/ask stream name."
  def book_ticker(symbol), do: symbol_stream(symbol, "bookTicker")

  @doc "Builds a symbol 24-hour ticker stream name."
  def symbol_ticker(symbol), do: symbol_stream(symbol, "ticker")

  @doc "Builds the all-symbol mini-ticker array stream name."
  def all_mini_tickers, do: "!miniTicker@arr"

  @doc "Builds a partial-depth stream name."
  def partial_depth(symbol, levels, speed_ms \\ 1_000)
      when levels in [5, 10, 20] and speed_ms in [100, 1_000] do
    suffix = if speed_ms == 100, do: "@100ms", else: ""
    symbol_stream(symbol, "depth#{levels}" <> suffix)
  end

  @type event :: map() | list()

  @doc "Starts a supervised market stream. Example: `streams: [\"btcusdt@trade\"]`."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @doc "Pops the oldest decoded event, or returns `:empty`."
  @spec pop(GenServer.server()) :: {:ok, event()} | :empty
  def pop(server), do: GenServer.call(server, :pop)

  @doc "Pops and normalizes the oldest market event."
  @spec pop_normalized(GenServer.server()) :: {:ok, map()} | :empty
  def pop_normalized(server) do
    case pop(server) do
      {:ok, message} -> {:ok, Events.market(message)}
      :empty -> :empty
    end
  end

  @doc "Returns connection and queue counters."
  @spec stats(GenServer.server()) :: map()
  def stats(server), do: GenServer.call(server, :stats)

  @doc "Closes and reopens the connection with its existing streams."
  @spec renew(GenServer.server()) :: :ok | {:error, :disconnected}
  def renew(server), do: GenServer.call(server, :renew)

  @doc "Adds a market stream subscription. The server confirms receipt asynchronously."
  @spec subscribe(GenServer.server(), String.t()) :: :ok | {:error, String.t()}
  def subscribe(server, stream), do: GenServer.call(server, {:subscription, :add, stream})

  @doc "Removes a market stream subscription; at least one stream must remain."
  @spec unsubscribe(GenServer.server(), String.t()) :: :ok | {:error, String.t()}
  def unsubscribe(server, stream), do: GenServer.call(server, {:subscription, :remove, stream})

  @doc "Returns the desired market stream subscriptions."
  @spec subscriptions(GenServer.server()) :: [String.t()]
  def subscriptions(server), do: GenServer.call(server, :subscriptions)

  @doc "Builds and validates a Spot combined-stream URL."
  @spec url([String.t()], keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def url(streams, opts \\ []) do
    base =
      case Keyword.get(opts, :environment, :testnet) do
        :testnet -> @testnet_url
        :production -> @production_url
        _ -> nil
      end

    cond do
      is_nil(base) ->
        {:error, "environment must be :testnet or :production"}

      not is_list(streams) or streams == [] or length(streams) > 1_024 ->
        {:error, "streams must contain 1 to 1024 names"}

      not Enum.all?(streams, &valid_stream?/1) ->
        {:error, "stream names contain invalid characters"}

      true ->
        {:ok, base <> "/stream?streams=" <> Enum.join(streams, "/")}
    end
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    capacity = Keyword.get(opts, :capacity, @default_capacity)
    streams = Keyword.get(opts, :streams, [])
    socket_start = Keyword.get(opts, :socket_start, &start_socket/1)

    with true <- is_integer(capacity) and capacity > 0,
         {:ok, stream_url} <- url(streams, opts),
         {:ok, socket} <- socket_start.(stream_url) do
      {:ok,
       %{
         socket: socket,
         url: stream_url,
         environment: Keyword.get(opts, :environment, :testnet),
         streams: MapSet.new(streams),
         socket_streams: MapSet.new(streams),
         server_streams: MapSet.new(streams),
         control_timer: nil,
         socket_start: socket_start,
         queue: :queue.new(),
         count: 0,
         capacity: capacity,
         dropped: 0,
         decode_errors: 0,
         reconnects: 0,
         failure_streak: 0,
         connected?: false,
         reconnect_timer: nil
       }}
    else
      false -> {:stop, {:invalid_option, "capacity must be a positive integer"}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:pop, _from, state) do
    case :queue.out(state.queue) do
      {{:value, event}, queue} ->
        {:reply, {:ok, event}, %{state | queue: queue, count: state.count - 1}}

      {:empty, _} ->
        {:reply, :empty, state}
    end
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     Map.take(state, [:count, :capacity, :dropped, :decode_errors, :reconnects, :connected?]),
     state}
  end

  def handle_call(:subscriptions, _from, state) do
    {:reply, state.streams |> MapSet.to_list() |> Enum.sort(), state}
  end

  def handle_call({:subscription, action, stream}, _from, state) do
    cond do
      not valid_stream?(stream) ->
        {:reply, {:error, "invalid stream name"}, state}

      action == :add and MapSet.member?(state.streams, stream) ->
        {:reply, :ok, state}

      action == :remove and not MapSet.member?(state.streams, stream) ->
        {:reply, :ok, state}

      action == :remove and MapSet.size(state.streams) == 1 ->
        {:reply, {:error, "at least one stream is required"}, state}

      action == :add and MapSet.size(state.streams) == 1_024 ->
        {:reply, {:error, "at most 1024 streams are allowed"}, state}

      true ->
        streams =
          case action do
            :add -> MapSet.put(state.streams, stream)
            :remove -> MapSet.delete(state.streams, stream)
          end

        {:ok, stream_url} =
          url(streams |> MapSet.to_list() |> Enum.sort(), environment: state.environment)

        state = %{state | streams: streams, url: stream_url}

        {:reply, :ok, schedule_controls(state)}
    end
  end

  def handle_call(:renew, _from, state) do
    if is_pid(state.socket) do
      WebSockex.cast(state.socket, :renew)
      {:reply, :ok, state}
    else
      {:reply, {:error, :disconnected}, state}
    end
  end

  @impl true
  def handle_info({:market_connected, socket}, state) when socket == state.socket do
    state = %{state | connected?: true, failure_streak: 0, server_streams: state.socket_streams}
    {:noreply, schedule_controls(state)}
  end

  def handle_info(:flush_controls, state) do
    state = %{state | control_timer: nil}

    if state.connected? do
      added = MapSet.difference(state.streams, state.server_streams) |> MapSet.to_list()
      removed = MapSet.difference(state.server_streams, state.streams) |> MapSet.to_list()
      if added != [], do: send_control(state, "SUBSCRIBE", Enum.sort(added))
      if removed != [], do: send_control(state, "UNSUBSCRIBE", Enum.sort(removed))
      {:noreply, %{state | server_streams: state.streams}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:market_disconnected, socket, _reason}, state) when socket == state.socket do
    {:noreply, %{state | connected?: false}}
  end

  def handle_info({:market_event, socket, event}, state) when socket == state.socket do
    state =
      if state.count == state.capacity do
        {{:value, _oldest}, queue} = :queue.out(state.queue)
        %{state | queue: queue, count: state.count - 1, dropped: state.dropped + 1}
      else
        state
      end

    state = %{state | queue: :queue.in(event, state.queue), count: state.count + 1}

    if shutdown_event?(event), do: WebSockex.cast(socket, :renew)
    {:noreply, state}
  end

  def handle_info({:market_decode_error, socket}, state) when socket == state.socket do
    {:noreply, %{state | decode_errors: state.decode_errors + 1}}
  end

  def handle_info({:EXIT, socket, _reason}, state) when socket == state.socket do
    timer = Process.send_after(self(), :reconnect, reconnect_delay(state.failure_streak))

    {:noreply,
     %{
       state
       | socket: nil,
         connected?: false,
         reconnect_timer: timer,
         reconnects: state.reconnects + 1,
         failure_streak: state.failure_streak + 1
     }}
  end

  def handle_info(:reconnect, state) do
    case state.socket_start.(state.url) do
      {:ok, socket} ->
        {:noreply, %{state | socket: socket, socket_streams: state.streams, reconnect_timer: nil}}

      {:error, _reason} ->
        timer = Process.send_after(self(), :reconnect, reconnect_delay(state.failure_streak))

        {:noreply,
         %{
           state
           | reconnect_timer: timer,
             reconnects: state.reconnects + 1,
             failure_streak: state.failure_streak + 1
         }}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.reconnect_timer, do: Process.cancel_timer(state.reconnect_timer)
    if state.control_timer, do: Process.cancel_timer(state.control_timer)

    if is_pid(state.socket) and Process.alive?(state.socket),
      do: Process.exit(state.socket, :shutdown)

    :ok
  end

  defp start_socket(stream_url) do
    ssl_options = :httpc.ssl_verify_host_options(true)

    WebSockex.start_link(stream_url, BinanceElixir.Spot.MarketStream.Socket, %{sink: self()},
      insecure: false,
      cacerts: :public_key.cacerts_get(),
      ssl_options: ssl_options,
      handle_initial_conn_failure: true,
      socket_connect_timeout: 5_000,
      socket_recv_timeout: 5_000
    )
  end

  defp valid_stream?(name) when is_binary(name) and byte_size(name) > 0,
    do: Regex.match?(~r/\A[A-Za-z0-9!@_.:+-]+\z/, name)

  defp valid_stream?(_), do: false

  defp symbol_stream(symbol, channel) when is_binary(symbol) and byte_size(symbol) > 0,
    do: String.downcase(symbol) <> "@" <> channel

  defp shutdown_event?(%{"data" => %{"e" => "serverShutdown"}}), do: true
  defp shutdown_event?(%{"e" => "serverShutdown"}), do: true
  defp shutdown_event?(_), do: false

  defp send_control(state, method, streams) do
    WebSockex.cast(state.socket, {:control, method, streams, System.unique_integer([:positive])})
  end

  defp schedule_controls(%{connected?: true, control_timer: nil} = state) do
    %{state | control_timer: Process.send_after(self(), :flush_controls, 500)}
  end

  defp schedule_controls(state), do: state

  defp reconnect_delay(attempt), do: min(1_000 * Integer.pow(2, min(attempt, 5)), 30_000)
end
