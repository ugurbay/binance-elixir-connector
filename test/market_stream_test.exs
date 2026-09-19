defmodule BinanceElixir.MarketStreamTest do
  use ExUnit.Case, async: true

  alias BinanceElixir.Spot.MarketStream
  alias BinanceElixir.Spot.MarketStream.Socket

  test "builds production and testnet combined-stream URLs" do
    assert {:ok,
            "wss://stream.testnet.binance.vision/stream?streams=btcusdt@trade/ethusdt@bookTicker"} =
             MarketStream.url(["btcusdt@trade", "ethusdt@bookTicker"])

    assert {:ok, "wss://stream.binance.com:9443/stream?streams=btcusdt@trade"} =
             MarketStream.url(["btcusdt@trade"], environment: :production)

    assert {:error, _} = MarketStream.url(["../bad"])
    assert {:error, _} = MarketStream.url([])
  end

  test "buffers events with a fixed capacity and reports loss" do
    socket =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {:ok, pid} =
      MarketStream.start_link(
        streams: ["btcusdt@trade"],
        capacity: 2,
        socket_start: fn _ -> {:ok, socket} end
      )

    send(pid, {:market_connected, socket})
    send(pid, {:market_event, socket, %{"t" => 1}})
    send(pid, {:market_event, socket, %{"t" => 2}})
    send(pid, {:market_event, socket, %{"t" => 3}})

    assert %{count: 2, dropped: 1, connected?: true} = MarketStream.stats(pid)
    assert {:ok, %{"t" => 2}} = MarketStream.pop(pid)
    assert {:ok, %{"t" => 3}} = MarketStream.pop(pid)
    assert :empty = MarketStream.pop(pid)

    GenServer.stop(pid)
  end

  test "answers Binance ping with identical pong payload" do
    state = %{sink: self()}
    assert {:reply, {:pong, "abc"}, ^state} = Socket.handle_ping({:ping, "abc"}, state)
    assert {:reply, :pong, ^state} = Socket.handle_ping(:ping, state)
  end

  test "changes subscriptions and restores the current set after reconnect" do
    test_pid = self()

    socket_start = fn url ->
      socket =
        spawn_link(fn ->
          receive do
            :stop -> :ok
          end
        end)

      send(test_pid, {:socket_started, socket, url})
      {:ok, socket}
    end

    {:ok, stream} =
      MarketStream.start_link(streams: ["btcusdt@trade"], socket_start: socket_start)

    assert_receive {:socket_started, first_socket, _}
    assert :ok = MarketStream.subscribe(stream, "ethusdt@trade")
    assert ["btcusdt@trade", "ethusdt@trade"] = MarketStream.subscriptions(stream)
    assert :ok = MarketStream.unsubscribe(stream, "btcusdt@trade")
    assert {:error, _} = MarketStream.unsubscribe(stream, "ethusdt@trade")
    assert {:error, _} = MarketStream.subscribe(stream, "../invalid")

    Process.exit(first_socket, :kill)

    assert_receive {:socket_started, second_socket,
                    "wss://stream.testnet.binance.vision/stream?streams=ethusdt@trade"},
                   2_000

    refute first_socket == second_socket
    GenServer.stop(stream)
  end

  test "encodes WebSocket subscription control frames" do
    state = %{sink: self()}

    assert {:reply, {:text, payload}, ^state} =
             Socket.handle_cast({:control, "SUBSCRIBE", ["btcusdt@trade"], 42}, state)

    assert %{"method" => "SUBSCRIBE", "params" => ["btcusdt@trade"], "id" => 42} =
             Jason.decode!(payload)
  end

  test "reconnects with the same stream set and keeps queued events" do
    test_pid = self()

    socket_start = fn url ->
      socket =
        spawn_link(fn ->
          receive do
            :stop -> :ok
          end
        end)

      send(test_pid, {:socket_started, socket, url})
      send(self(), {:market_connected, socket})
      {:ok, socket}
    end

    {:ok, stream} =
      MarketStream.start_link(streams: ["btcusdt@trade"], socket_start: socket_start)

    assert_receive {:socket_started, first_socket, url}
    send(stream, {:market_event, first_socket, %{"t" => 1}})
    Process.exit(first_socket, :kill)

    assert_receive {:socket_started, second_socket, ^url}, 2_000
    refute first_socket == second_socket
    assert %{reconnects: 1, connected?: true} = MarketStream.stats(stream)
    assert {:ok, %{"t" => 1}} = MarketStream.pop(stream)

    GenServer.stop(stream)
  end

  test "decodes JSON events and counts malformed frames" do
    socket = self()
    state = %{sink: self()}

    assert {:ok, ^state} = Socket.handle_frame({:text, ~s({"e":"trade","p":"1.0"})}, state)
    assert_receive {:market_event, ^socket, %{"e" => "trade", "p" => "1.0"}}

    assert {:ok, ^state} = Socket.handle_frame({:text, "{"}, state)
    assert_receive {:market_decode_error, ^socket}
  end
end
