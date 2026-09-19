defmodule BinanceElixir.Spot.MarketStream.Socket do
  @moduledoc false
  use WebSockex

  @renew_after_ms 23 * 60 * 60 * 1_000

  @impl true
  def handle_connect(_conn, state) do
    send(state.sink, {:market_connected, self()})
    timer = Process.send_after(self(), :renew, @renew_after_ms)
    {:ok, Map.put(state, :renew_timer, timer)}
  end

  @impl true
  def handle_frame({:text, payload}, state) do
    case Jason.decode(payload) do
      {:ok, event} -> send(state.sink, {:market_event, self(), event})
      {:error, _} -> send(state.sink, {:market_decode_error, self()})
    end

    {:ok, state}
  end

  def handle_frame(_frame, state), do: {:ok, state}

  @impl true
  def handle_ping(:ping, state), do: {:reply, :pong, state}
  def handle_ping({:ping, payload}, state), do: {:reply, {:pong, payload}, state}

  @impl true
  def handle_cast(:renew, state), do: {:close, state}

  @impl true
  def handle_info(:renew, state), do: {:close, state}
  def handle_info(_message, state), do: {:ok, state}

  @impl true
  def handle_disconnect(reason, state) do
    if state[:renew_timer], do: Process.cancel_timer(state.renew_timer)
    send(state.sink, {:market_disconnected, self(), reason})
    {:ok, state}
  end
end
