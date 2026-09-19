defmodule BinanceElixir.Spot.MarketStream do
  @moduledoc """
  Supervised Binance Spot market WebSocket stream with a bounded event queue.

  Streams are static for this connection. The same URL is used after every
  reconnect, restoring all requested subscriptions. `dropped` in `stats/1`
  means events were lost; consumers must resynchronize any derived state.
  """

  use GenServer

  @production_url "wss://stream.binance.com:9443"
  @testnet_url "wss://stream.testnet.binance.vision"
  @default_capacity 1_024

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

  @doc "Returns connection and queue counters."
  @spec stats(GenServer.server()) :: map()
  def stats(server), do: GenServer.call(server, :stats)

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

  @impl true
  def handle_info({:market_connected, socket}, state) when socket == state.socket do
    {:noreply, %{state | connected?: true, failure_streak: 0}}
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
        {:noreply, %{state | socket: socket, reconnect_timer: nil}}

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

  defp shutdown_event?(%{"data" => %{"e" => "serverShutdown"}}), do: true
  defp shutdown_event?(%{"e" => "serverShutdown"}), do: true
  defp shutdown_event?(_), do: false

  defp reconnect_delay(attempt), do: min(1_000 * Integer.pow(2, min(attempt, 5)), 30_000)
end
