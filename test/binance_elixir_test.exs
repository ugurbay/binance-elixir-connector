defmodule BinanceElixirTest do
  use ExUnit.Case, async: true

  alias BinanceElixir.{Error, Spot}

  defmodule StubTransport do
    @behaviour BinanceElixir.Transport

    @impl true
    def request(method, url, headers, body, timeout) do
      send(self(), {:request, method, url, headers, body, timeout})
      [response | rest] = Process.get(:responses)
      Process.put(:responses, rest)
      response
    end
  end

  defp client(opts \\ []) do
    BinanceElixir.new(
      Keyword.merge(
        [
          api_key: "key",
          api_secret: "secret",
          transport: StubTransport,
          clock: fn -> 1_700_000_000_000 end,
          sleep: fn delay -> send(self(), {:slept, delay}) end
        ],
        opts
      )
    )
  end

  test "signed request signs precisely the URL payload and sends key" do
    Process.put(:responses, [{:ok, 200, [{"X-MBX-USED-WEIGHT-1M", "7"}], ~s({"balances":[]})}])

    assert {:ok, response} = Spot.account(client())
    assert response.data == %{"balances" => []}
    assert response.rate_limits == %{"x-mbx-used-weight-1m" => "7"}

    assert_receive {:request, :get, url, headers, "", 10_000}
    assert {"x-mbx-apikey", "key"} in headers
    assert %URI{query: query} = URI.parse(url)
    [payload, signature] = String.split(query, "&signature=")
    assert payload == "recvWindow=5000&timestamp=1700000000000"

    assert signature ==
             :crypto.mac(:hmac, :sha256, "secret", payload) |> Base.encode16(case: :lower)
  end

  test "query parameters preserve decimal strings and percent encoding" do
    Process.put(:responses, [{:ok, 200, [], "{}"}])

    assert {:ok, _} =
             Spot.test_order(client(), %{
               symbol: "BTCUSDT",
               quantity: "0.00001000",
               newClientOrderId: "a b"
             })

    assert_receive {:request, :post, url, _, "", _}
    assert url =~ "quantity=0.00001000"
    assert url =~ "newClientOrderId=a+b"
  end

  test "GET retries after a rate limit response" do
    Process.put(:responses, [
      {:ok, 429, [{"Retry-After", "2"}], ~s({"code":-1003,"msg":"too many requests"})},
      {:ok, 200, [], ~s({"serverTime":123})}
    ])

    assert {:ok, %{data: %{"serverTime" => 123}}} = Spot.server_time(client())
    assert_receive {:slept, 2_000}
    assert_receive {:request, :get, _, _, _, _}
    assert_receive {:request, :get, _, _, _, _}
  end

  test "signed GET refreshes timestamp and signature after a retry" do
    Process.put(:responses, [
      {:ok, 429, [{"Retry-After", "0"}], ~s({"code":-1003,"msg":"limit"})},
      {:ok, 200, [], "{}"}
    ])

    Process.put(:next_time, 1_700_000_000_000)

    clock = fn ->
      now = Process.get(:next_time)
      Process.put(:next_time, now + 1_000)
      now
    end

    assert {:ok, _} = Spot.account(client(clock: clock))
    assert_receive {:request, :get, first_url, _, _, _}
    assert_receive {:request, :get, second_url, _, _, _}
    assert first_url =~ "timestamp=1700000000000"
    assert second_url =~ "timestamp=1700000001000"
    refute first_url == second_url
  end

  test "IP ban is not retried" do
    Process.put(:responses, [
      {:ok, 418, [{"Retry-After", "120"}], ~s({"code":-1003,"msg":"banned"})}
    ])

    assert {:error, %Error{status: 418, retry_after: 120}} = Spot.server_time(client())
    assert_receive {:request, :get, _, _, _, _}
    refute_receive {:request, :get, _, _, _, _}
  end

  test "write failures are never retried and report uncertain execution" do
    Process.put(:responses, [{:ok, 500, [], ~s({"code":-1007,"msg":"timeout"})}])

    assert {:error, %Error{status: 500, unknown_execution?: true, client_order_id: order_id}} =
             Spot.place_order(client(), %{
               symbol: "BTCUSDT",
               side: "BUY",
               type: "MARKET",
               quantity: "0.1",
               newClientOrderId: "bot-order-123"
             })

    assert_receive {:request, :post, _, _, _, _}
    refute_receive {:request, :post, _, _, _, _}
    assert order_id == "bot-order-123"
  end

  test "validation rejects missing credentials and reserved signed parameters" do
    assert {:error, %Error{type: :validation}} = Spot.account(client(api_secret: nil))
    assert {:error, %Error{type: :validation}} = Spot.account(client(), %{timestamp: 1})
    assert {:error, %Error{type: :validation}} = Spot.account(client(), [:bad])

    assert {:error, %Error{type: :validation}} =
             Spot.account(client(), symbol: "BTCUSDT", symbol: "ETHUSDT")

    assert {:error, %Error{type: :validation}} =
             Spot.place_order(client(), %{symbol: "BTCUSDT", price: 1.25})

    assert {:error, %Error{type: :validation}} =
             Spot.place_order(client(), %{symbol: "BTCUSDT", quantity: "0.1"})

    assert {:error, %Error{type: :validation}} = BinanceElixir.request(client(), :get, "//evil")
  end

  test "inspecting a client hides API credentials" do
    inspected = inspect(client())
    refute inspected =~ "api_key:"
    refute inspected =~ "api_secret:"
  end
end
