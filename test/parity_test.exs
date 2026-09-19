defmodule BinanceElixir.ParityTest do
  use ExUnit.Case, async: true

  alias BinanceElixir.{Clock, RateLimit, Signature, Spot}
  alias BinanceElixir.Spot.{Events, Filters, MarketStream, Reconciliation, UserStream}

  defmodule Transport do
    @behaviour BinanceElixir.Transport

    @impl true
    def request(method, url, _headers, _body, _timeout) do
      send(self(), {:wire, method, url})
      [response | rest] = Process.get(:responses)
      Process.put(:responses, rest)
      response
    end
  end

  defp symbol_info do
    %{
      "symbol" => "BTCUSDT",
      "status" => "TRADING",
      "isSpotTradingAllowed" => true,
      "quoteOrderQtyMarketAllowed" => true,
      "orderTypes" => ["LIMIT", "MARKET", "STOP_LOSS"],
      "filters" => [
        %{
          "filterType" => "PRICE_FILTER",
          "minPrice" => "0.01",
          "maxPrice" => "1000000",
          "tickSize" => "0.01"
        },
        %{
          "filterType" => "LOT_SIZE",
          "minQty" => "0.001",
          "maxQty" => "100",
          "stepSize" => "0.001"
        },
        %{"filterType" => "MIN_NOTIONAL", "minNotional" => "10", "applyToMarket" => true}
      ]
    }
  end

  defp order do
    %{
      symbol: "BTCUSDT",
      side: "BUY",
      type: "LIMIT",
      timeInForce: "GTC",
      quantity: "0.100",
      price: "200.00",
      newClientOrderId: "id-1"
    }
  end

  defp client(opts \\ []) do
    BinanceElixir.new(
      Keyword.merge(
        [
          api_key: "key",
          api_secret: "secret",
          transport: Transport,
          retries: 0,
          clock: fn -> 1_700_000_000_000 end
        ],
        opts
      )
    )
  end

  test "exact filter preflight accepts a valid order and rejects price and quantity drift" do
    assert {:ok, %{"price" => "200.00"}} = Filters.validate(symbol_info(), order())

    assert {:error, %{type: :validation}} =
             Filters.validate(symbol_info(), %{order() | price: "200.001"})

    assert {:error, %{type: :validation}} =
             Filters.validate(symbol_info(), %{order() | quantity: "0.1001"})

    assert {:error, %{type: :validation}} =
             Filters.validate(symbol_info(), %{order() | price: 200.0})

    assert {:error, %{type: :validation}} =
             Filters.validate(symbol_info(), %{order() | quantity: "0.001"})
  end

  test "market notional requires an explicit reference price" do
    market = %{symbol: "BTCUSDT", side: "BUY", type: "MARKET", quantity: "0.100"}
    assert {:error, %{message: message}} = Filters.validate(symbol_info(), market)
    assert message =~ "reference_price"
    assert {:ok, _} = Filters.validate(symbol_info(), market, reference_price: "200.00")
  end

  test "decimal values and RFC 3986 encoding preserve signed request bytes" do
    Process.put(:responses, [{:ok, 200, [], "{}"}])

    assert {:ok, _} =
             Spot.test_order(client(), %{
               symbol: "BTCUSDT",
               quantity: Decimal.new("0.0100"),
               newClientOrderId: "a b"
             })

    assert_receive {:wire, :post, url}
    assert url =~ "quantity=0.0100"
    assert url =~ "newClientOrderId=a%20b"
  end

  test "429 without Retry-After is returned without retry" do
    Process.put(:responses, [{:ok, 429, [], ~s({"code":-1003,"msg":"limit"})}])
    assert {:error, %{status: 429}} = Spot.server_time(client(retries: 2))
    assert_receive {:wire, :get, _}
    refute_receive {:wire, :get, _}
  end

  test "production order write needs an explicit opt-in" do
    production = client(base_url: "https://api.binance.com")

    assert {:error, %{type: :validation, message: message}} =
             Spot.place_order(production, order())

    assert message =~ "enable_live_trading?"
    refute_receive {:wire, _, _}

    Process.put(:responses, [{:ok, 200, [], ~s({"status":"NEW"})}])
    assert {:ok, _} = Spot.place_order(%{production | enable_live_trading?: true}, order())
    assert_receive {:wire, :post, _}
  end

  test "synchronized clock uses midpoint and rate tracker shares header observations" do
    counter = :atomics.new(1, [])
    :atomics.put(counter, 1, 800)

    base_clock = fn -> :atomics.add_get(counter, 1, 200) end

    {:ok, clock} = Clock.start_link(base_clock: base_clock)
    Process.put(:responses, [{:ok, 200, [], ~s({"serverTime":1500})}])
    assert {:ok, 400} = Clock.synchronize(client(), clock)
    assert Clock.offset_ms(clock) == 400

    {:ok, tracker} = RateLimit.start_link()
    :ok = RateLimit.record_attempt(tracker, 5)

    :ok =
      RateLimit.record_response(tracker, [
        {"X-MBX-USED-WEIGHT-1M", "21"},
        {"X-MBX-ORDER-COUNT-10S", "2"}
      ])

    snapshot = RateLimit.snapshot(tracker)
    assert snapshot.raw_request_count == 1
    assert snapshot.estimated_request_weight == 5
    assert snapshot.request_weight[{1, "m"}] == 21
    assert snapshot.orders[{10, "s"}] == 2
  end

  test "unknown submission is queried and never resubmitted" do
    Process.put(:responses, [
      {:ok, 500, [], ~s({"code":-1007,"msg":"timeout"})},
      {:ok, 400, [], ~s({"code":-2013,"msg":"Order does not exist."})},
      {:ok, 200, [], ~s({"clientOrderId":"id-1","status":"NEW"})}
    ])

    persist = fn record ->
      send(self(), {:persisted, record.client_order_id})
      :ok
    end

    sleep = fn _ -> :ok end

    assert {:ok, %{state: :reconciled, query_attempts: 2, submit_attempts: 1}} =
             Spot.submit_order(client(), symbol_info(), order(),
               persist: persist,
               sleep: sleep,
               query_delay_ms: 0
             )

    assert_receive {:persisted, "id-1"}
    assert_receive {:wire, :post, _}
    assert_receive {:wire, :get, _}
    assert_receive {:wire, :get, _}
    refute_receive {:wire, :post, _}
  end

  test "unresolved order can be reconciled by matching user event" do
    Process.put(:responses, [
      {:ok, 500, [], ~s({"code":-1007,"msg":"timeout"})},
      {:ok, 400, [], ~s({"code":-2013,"msg":"Order does not exist."})}
    ])

    assert {:error, %{state: :unresolved} = lifecycle} =
             Spot.submit_order(client(), symbol_info(), order(),
               persist: fn _ -> :ok end,
               max_query_attempts: 1
             )

    event = %{
      "event" => %{"e" => "executionReport", "s" => "BTCUSDT", "c" => "id-1", "X" => "NEW"}
    }

    assert %{state: :reconciled} = Reconciliation.observe(lifecycle, event)
  end

  test "HMAC WebSocket payload sorts keys and signs the exact bytes" do
    request = UserStream.signed_request(client(), "request-1")
    params = request.params
    payload = "apiKey=key&recvWindow=5000&timestamp=1700000000000"
    assert Signature.ws_payload(Map.delete(params, "signature")) == payload

    assert params["signature"] ==
             :crypto.mac(:hmac, :sha256, "secret", payload) |> Base.encode16(case: :lower)
  end

  test "market and user event normalization preserve unknown fields and exact decimals" do
    market =
      Events.market(%{
        "stream" => "btcusdt@trade",
        "data" => %{"e" => "trade", "p" => "100.0100", "custom" => 7}
      })

    assert market.stream == "btcusdt@trade"
    assert market["p"] == Decimal.new("100.0100")
    assert market["custom"] == 7

    event =
      Events.user(%{
        "subscriptionId" => 2,
        "event" => %{
          "e" => "executionReport",
          "s" => "BTCUSDT",
          "c" => "id-1",
          "X" => "NEW",
          "p" => "100.00"
        }
      })

    assert event.subscription_id == 2
    assert event.client_order_id == "id-1"
    assert event["p"] == Decimal.new("100.00")
    assert event.order_status == "NEW"

    assert %{kind: :response, status: 200} = Events.user(%{"status" => 200, "result" => %{}})
    assert MarketStream.partial_depth("BTCUSDT", 20, 100) == "btcusdt@depth20@100ms"
    assert MarketStream.all_mini_tickers() == "!miniTicker@arr"
  end

  test "Ed25519 PKCS#8 loader and signature match RFC 8032 test vector" do
    seed = Base.decode16!("9D61B19DEFFD5A60BA844AF492EC2CC44449C5697B326919703BAC031CAE7F60")

    der =
      <<0x30, 0x2E, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20, seed::binary>>

    pem = "-----BEGIN PRIVATE KEY-----\n" <> Base.encode64(der) <> "\n-----END PRIVATE KEY-----\n"
    assert {:ok, ^seed} = Signature.seed(pem)

    signed =
      Signature.sign(client(signing_algorithm: :ed25519, private_key: pem, api_secret: nil), "")

    assert Base.decode64!(signed) |> Base.encode16(case: :lower) ==
             "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"

    assert {:error, :invalid_private_key} =
             Signature.seed("-----BEGIN OPENSSH PRIVATE KEY-----bad")
  end
end
