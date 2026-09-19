defmodule BinanceElixir.Spot do
  @moduledoc "Common Binance Spot REST endpoints. All functions return a response or error tuple."

  alias BinanceElixir, as: Client
  alias BinanceElixir.Error

  @spec ping(Client.t()) :: {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def ping(client), do: Client.request(client, :get, "/api/v3/ping")

  @spec server_time(Client.t()) :: {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def server_time(client), do: Client.request(client, :get, "/api/v3/time")

  @spec exchange_info(Client.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def exchange_info(client, params \\ %{}),
    do: Client.request(client, :get, "/api/v3/exchangeInfo", params)

  @spec order_book(Client.t(), String.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def order_book(client, symbol, params \\ %{}),
    do: Client.request(client, :get, "/api/v3/depth", merge(params, symbol: symbol))

  @spec trades(Client.t(), String.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def trades(client, symbol, params \\ %{}),
    do: Client.request(client, :get, "/api/v3/trades", merge(params, symbol: symbol))

  @spec klines(Client.t(), String.t(), String.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def klines(client, symbol, interval, params \\ %{}) do
    Client.request(
      client,
      :get,
      "/api/v3/klines",
      merge(params, symbol: symbol, interval: interval)
    )
  end

  @spec ticker_price(Client.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def ticker_price(client, params \\ %{}),
    do: Client.request(client, :get, "/api/v3/ticker/price", params)

  @spec account(Client.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def account(client, params \\ %{}),
    do: Client.request(client, :get, "/api/v3/account", params, security: :signed)

  @spec open_orders(Client.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def open_orders(client, params \\ %{}),
    do: Client.request(client, :get, "/api/v3/openOrders", params, security: :signed)

  @spec order(Client.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def order(client, params),
    do: Client.request(client, :get, "/api/v3/order", params, security: :signed)

  @spec place_order(Client.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def place_order(client, params) do
    with {:ok, prepared, client_order_id} <- prepare_order(params) do
      case Client.request(client, :post, "/api/v3/order", prepared, security: :signed) do
        {:error, %Error{} = error} -> {:error, %{error | client_order_id: client_order_id}}
        result -> result
      end
    end
  end

  @spec test_order(Client.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def test_order(client, params),
    do: Client.request(client, :post, "/api/v3/order/test", params, security: :signed)

  @spec cancel_order(Client.t(), map() | keyword()) ::
          {:ok, BinanceElixir.Response.t()} | {:error, Error.t()}
  def cancel_order(client, params),
    do: Client.request(client, :delete, "/api/v3/order", params, security: :signed)

  defp merge(params, additions) when is_map(params), do: Map.merge(params, Map.new(additions))
  defp merge(params, additions) when is_list(params), do: Keyword.merge(params, additions)

  defp prepare_order(params) when is_map(params) do
    existing = Map.get(params, :newClientOrderId) || Map.get(params, "newClientOrderId")

    if is_binary(existing) and byte_size(existing) > 0 do
      {:ok, params, existing}
    else
      {:error,
       %Error{
         type: :validation,
         message: "newClientOrderId must be a pre-recorded non-empty string"
       }}
    end
  end

  defp prepare_order(params) when is_list(params) do
    if Keyword.keyword?(params) do
      id = Keyword.get(params, :newClientOrderId)

      if is_binary(id) and byte_size(id) > 0 do
        {:ok, params, id}
      else
        {:error,
         %Error{
           type: :validation,
           message: "newClientOrderId must be a pre-recorded non-empty string"
         }}
      end
    else
      {:error, %Error{type: :validation, message: "order params must be a map or keyword list"}}
    end
  end

  defp prepare_order(_),
    do: {:error, %Error{type: :validation, message: "order params must be a map or keyword list"}}
end
