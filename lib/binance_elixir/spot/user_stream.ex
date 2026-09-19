defmodule BinanceElixir.Spot.UserStream do
  @moduledoc """
  Supervised signed Spot User Data Stream over Binance WebSocket API.

  Reconnects use a fresh timestamp and signature. Events are held in a bounded
  queue; consumers must resynchronize account and order state after drops or
  reconnects.
  """

  use GenServer

  alias BinanceElixir.Spot.UserStream.Socket
  alias BinanceElixir.Signature
  alias BinanceElixir.Spot.Events

  @testnet_url "wss://ws-api.testnet.binance.vision/ws-api/v3"
  @production_url "wss://ws-api.binance.com:443/ws-api/v3"

  @spec start_link(BinanceElixir.t(), keyword()) :: GenServer.on_start()
  def start_link(client, opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, {client, opts}, if(name, do: [name: name], else: []))
  end

  @spec pop(GenServer.server()) :: {:ok, map()} | :empty
  def pop(server), do: GenServer.call(server, :pop)

  @doc "Pops and normalizes the oldest account event or WebSocket API response."
  @spec pop_normalized(GenServer.server()) :: {:ok, map()} | :empty
  def pop_normalized(server) do
    case pop(server) do
      {:ok, message} -> {:ok, Events.user(message)}
      :empty -> :empty
    end
  end

  @spec stats(GenServer.server()) :: map()
  def stats(server), do: GenServer.call(server, :stats)

  @spec subscribe(GenServer.server()) :: :ok
  def subscribe(server), do: GenServer.call(server, :subscribe)

  @spec unsubscribe(GenServer.server()) :: :ok
  def unsubscribe(server), do: GenServer.call(server, :unsubscribe)

  @spec subscriptions(GenServer.server()) :: :ok
  def subscriptions(server), do: GenServer.call(server, :subscriptions)

  @doc "Renews the connection and signs a fresh subscription request."
  @spec renew(GenServer.server()) :: :ok | {:error, :disconnected}
  def renew(server), do: GenServer.call(server, :renew)

  @doc "Builds a signed WebSocket API subscription request. Keep its return value out of logs."
  @spec signed_request(BinanceElixir.t(), String.t()) :: map()
  def signed_request(client, id) do
    params = %{
      "apiKey" => client.api_key,
      "recvWindow" => client.recv_window,
      "timestamp" => client.clock.()
    }

    signature = Signature.sign(client, Signature.ws_payload(params))

    %{
      id: id,
      method: "userDataStream.subscribe.signature",
      params: Map.put(params, "signature", signature)
    }
  end

  @impl true
  def init({client, opts}) do
    Process.flag(:trap_exit, true)
    capacity = Keyword.get(opts, :capacity, 1_024)
    socket_start = Keyword.get(opts, :socket_start, &start_socket/2)

    cond do
      not match?(%BinanceElixir{}, client) ->
        {:stop, :invalid_client}

      not (is_binary(client.api_key) and client.api_key != "" and
               ((client.signing_algorithm == :hmac and is_binary(client.api_secret) and
                   client.api_secret != "") or
                  (client.signing_algorithm == :ed25519 and is_binary(client.private_key)))) ->
        {:stop, :missing_credentials}

      not (is_integer(capacity) and capacity > 0) ->
        {:stop, :invalid_capacity}

      true ->
        url = if testnet?(client.base_url), do: @testnet_url, else: @production_url

        case socket_start.(url, %{
               client: client,
               sink: self(),
               desired?: true,
               renew_timer: nil,
               subscription_id: nil
             }) do
          {:ok, socket} ->
            {:ok,
             %{
               client: client,
               url: url,
               socket_start: socket_start,
               socket: socket,
               queue: :queue.new(),
               count: 0,
               capacity: capacity,
               dropped: 0,
               decode_errors: 0,
               reconnects: 0,
               failure_streak: 0,
               connected?: false,
               subscription: :pending,
               subscription_id: nil,
               pending_id: nil,
               desired?: true,
               reconnect_timer: nil
             }}

          {:error, reason} ->
            {:stop, reason}
        end
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
     Map.take(state, [
       :count,
       :capacity,
       :dropped,
       :decode_errors,
       :reconnects,
       :connected?,
       :subscription,
       :subscription_id
     ]), state}
  end

  def handle_call(action, _from, state)
      when action in [:subscribe, :unsubscribe, :subscriptions] do
    if is_pid(state.socket) do
      WebSockex.cast(state.socket, action)

      desired? =
        if action == :subscribe,
          do: true,
          else: if(action == :unsubscribe, do: false, else: state.desired?)

      subscription = if action == :unsubscribe, do: :inactive, else: state.subscription
      {:reply, :ok, %{state | desired?: desired?, subscription: subscription}}
    else
      {:reply, {:error, :disconnected}, state}
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
  def handle_info({:user_connected, socket}, state) when socket == state.socket,
    do: {:noreply, %{state | connected?: true, failure_streak: 0}}

  def handle_info({:user_disconnected, socket, _reason}, state) when socket == state.socket,
    do:
      {:noreply,
       %{
         state
         | connected?: false,
           subscription: if(state.desired?, do: :pending, else: :inactive),
           subscription_id: nil,
           reconnects: state.reconnects + 1
       }}

  def handle_info({:user_subscribe_sent, socket, id}, state) when socket == state.socket,
    do: {:noreply, %{state | pending_id: id, subscription: :pending}}

  def handle_info({:user_message, socket, message}, state) when socket == state.socket do
    state =
      if is_map(message) and message["id"] == state.pending_id and not is_nil(state.pending_id) do
        if message["status"] == 200 and is_integer(get_in(message, ["result", "subscriptionId"])) do
          id = message["result"]["subscriptionId"]
          WebSockex.cast(socket, {:subscription_id, id})
          %{state | subscription: :active, subscription_id: id, pending_id: nil}
        else
          %{state | subscription: :failed, pending_id: nil}
        end
      else
        state
      end

    state = enqueue(state, message)
    if shutdown_event?(message), do: WebSockex.cast(socket, :renew)
    {:noreply, state}
  end

  def handle_info({:user_decode_error, socket}, state) when socket == state.socket,
    do: {:noreply, %{state | decode_errors: state.decode_errors + 1}}

  def handle_info({:EXIT, socket, _reason}, state) when socket == state.socket do
    timer = Process.send_after(self(), :reconnect, reconnect_delay(state.failure_streak))

    {:noreply,
     %{
       state
       | socket: nil,
         connected?: false,
         reconnect_timer: timer,
         failure_streak: state.failure_streak + 1
     }}
  end

  def handle_info(:reconnect, state) do
    socket_state = %{
      client: state.client,
      sink: self(),
      desired?: state.desired?,
      renew_timer: nil,
      subscription_id: nil
    }

    case state.socket_start.(state.url, socket_state) do
      {:ok, socket} ->
        {:noreply, %{state | socket: socket, reconnect_timer: nil}}

      {:error, _} ->
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

  defp enqueue(state, message) do
    state =
      if state.count == state.capacity do
        {{:value, _}, queue} = :queue.out(state.queue)
        %{state | queue: queue, count: state.count - 1, dropped: state.dropped + 1}
      else
        state
      end

    %{state | queue: :queue.in(message, state.queue), count: state.count + 1}
  end

  defp start_socket(url, state) do
    WebSockex.start_link(url, Socket, state,
      insecure: false,
      cacerts: :public_key.cacerts_get(),
      ssl_options: :httpc.ssl_verify_host_options(true),
      handle_initial_conn_failure: true,
      socket_connect_timeout: 5_000,
      socket_recv_timeout: 5_000
    )
  end

  defp testnet?(url),
    do: URI.parse(url).host in ["testnet.binance.vision", "api1.testnet.binance.vision"]

  defp shutdown_event?(%{"event" => %{"e" => "serverShutdown"}}), do: true
  defp shutdown_event?(%{"e" => "serverShutdown"}), do: true
  defp shutdown_event?(_), do: false

  defp reconnect_delay(attempt), do: min(1_000 * Integer.pow(2, min(attempt, 5)), 30_000)
end
