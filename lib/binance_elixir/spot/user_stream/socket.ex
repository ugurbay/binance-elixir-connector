defmodule BinanceElixir.Spot.UserStream.Socket do
  @moduledoc false
  use WebSockex

  alias BinanceElixir.Spot.UserStream

  @renew_after_ms 23 * 60 * 60 * 1_000

  @impl true
  def handle_connect(_conn, state) do
    send(state.sink, {:user_connected, self()})
    timer = Process.send_after(self(), :renew, @renew_after_ms)
    state = %{state | renew_timer: timer}

    if state.desired? do
      WebSockex.cast(self(), :subscribe)
    end

    {:ok, state}
  end

  @impl true
  def handle_frame({:text, payload}, state) do
    case Jason.decode(payload) do
      {:ok, message} -> send(state.sink, {:user_message, self(), message})
      {:error, _} -> send(state.sink, {:user_decode_error, self()})
    end

    {:ok, state}
  end

  def handle_frame(_frame, state), do: {:ok, state}

  @impl true
  def handle_ping(:ping, state), do: {:reply, :pong, state}
  def handle_ping({:ping, payload}, state), do: {:reply, {:pong, payload}, state}

  @impl true
  def handle_cast(:subscribe, state), do: subscribe(%{state | desired?: true})

  def handle_cast(:unsubscribe, state) do
    request = %{
      id: request_id(),
      method: "userDataStream.unsubscribe",
      params:
        if(is_nil(state.subscription_id), do: %{}, else: %{subscriptionId: state.subscription_id})
    }

    {:reply, {:text, Jason.encode!(request)}, %{state | desired?: false}}
  end

  def handle_cast(:subscriptions, state) do
    {:reply,
     {:text, Jason.encode!(%{id: request_id(), method: "session.subscriptions", params: %{}})},
     state}
  end

  def handle_cast({:subscription_id, id}, state), do: {:ok, %{state | subscription_id: id}}
  def handle_cast(:renew, state), do: {:close, state}

  @impl true
  def handle_info(:renew, state), do: {:close, state}
  def handle_info(_message, state), do: {:ok, state}

  @impl true
  def handle_disconnect(reason, state) do
    if state.renew_timer, do: Process.cancel_timer(state.renew_timer)
    send(state.sink, {:user_disconnected, self(), reason})
    {:ok, %{state | renew_timer: nil, subscription_id: nil}}
  end

  defp subscribe(state) do
    id = request_id()
    send(state.sink, {:user_subscribe_sent, self(), id})
    request = UserStream.signed_request(state.client, id)
    {:reply, {:text, Jason.encode!(request)}, state}
  end

  defp request_id, do: :crypto.strong_rand_bytes(12) |> Base.encode16(case: :lower)
end
