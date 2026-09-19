defmodule BinanceElixir.Spot.Reconciliation do
  @moduledoc """
  Submits a Spot order once and reconciles uncertain results by client order ID.

  The caller supplies `:persist`, a function that durably reserves the unique
  client order ID before submission. It must refuse duplicate IDs, including
  after application restarts. An unresolved result must remain reserved.
  """

  alias BinanceElixir.Error
  alias BinanceElixir.Spot
  alias BinanceElixir.Spot.Filters

  @spec submit(BinanceElixir.t(), map(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, map()}
  def submit(client, symbol_info, params, opts \\ []) do
    persist = Keyword.get(opts, :persist)
    attempts = Keyword.get(opts, :max_query_attempts, 5)
    delay = Keyword.get(opts, :query_delay_ms, 250)
    max_delay = Keyword.get(opts, :max_query_delay_ms, 2_000)
    sleep = Keyword.get(opts, :sleep, &Process.sleep/1)

    cond do
      not is_function(persist, 1) ->
        rejected("a durable persist callback is required")

      not (is_integer(attempts) and attempts in 1..20 and is_integer(delay) and delay >= 0 and
             is_integer(max_delay) and max_delay >= delay and is_function(sleep, 1)) ->
        rejected("invalid reconciliation policy")

      true ->
        with {:ok, normalized} <-
               Filters.validate(symbol_info, params,
                 reference_price: Keyword.get(opts, :reference_price)
               ),
             {:ok, id} <- client_order_id(normalized),
             :ok <-
               persist.(%{symbol: normalized["symbol"], client_order_id: id, order: normalized}) do
          submit_once(client, normalized, id, attempts, delay, max_delay, sleep)
        else
          {:error, %Error{} = error} ->
            {:error, %{state: :rejected, error: error}}

          {:error, reason} ->
            rejected("order ID could not be durably reserved: #{inspect(reason)}")

          other ->
            rejected("persist callback must return :ok, got #{inspect(other)}")
        end
    end
  end

  @doc "Updates an unresolved result with a matching executionReport event."
  @spec observe(map(), map()) :: map()
  def observe(%{state: state, client_order_id: id, symbol: symbol} = lifecycle, event)
      when state in [:unknown, :unresolved] do
    report = Map.get(event, "event", event)

    event_type = report[:event_type] || report["e"]
    event_symbol = report[:symbol] || report["s"]
    event_id = report[:client_order_id] || report["c"]
    original_id = report[:original_client_order_id] || report["C"]

    if event_type == "executionReport" and event_symbol == symbol and
         (event_id == id or original_id == id) do
      %{lifecycle | state: :reconciled}
      |> Map.put(:order_event, report)
    else
      lifecycle
    end
  end

  def observe(lifecycle, _event), do: lifecycle

  defp submit_once(client, order, id, attempts, delay, max_delay, sleep) do
    symbol = order["symbol"]

    case Spot.place_order(client, order) do
      {:ok, response} ->
        {:ok,
         %{
           state: :confirmed,
           symbol: symbol,
           client_order_id: id,
           order: response.data,
           submit_attempts: 1
         }}

      {:error, %Error{unknown_execution?: true} = error} ->
        reconcile(client, symbol, id, error, 1, attempts, delay, max_delay, sleep)

      {:error, error} ->
        {:error,
         %{
           state: :rejected,
           symbol: symbol,
           client_order_id: id,
           error: error,
           submit_attempts: 1
         }}
    end
  end

  defp reconcile(
         client,
         symbol,
         id,
         initial_error,
         attempt,
         max_attempts,
         delay,
         max_delay,
         sleep
       ) do
    case Spot.order(client, %{symbol: symbol, origClientOrderId: id}) do
      {:ok, response} ->
        {:ok,
         %{
           state: :reconciled,
           symbol: symbol,
           client_order_id: id,
           order: response.data,
           submit_attempts: 1,
           query_attempts: attempt
         }}

      {:error, error} ->
        if attempt == max_attempts or not retryable_query?(error) do
          {:error,
           %{
             state: :unresolved,
             symbol: symbol,
             client_order_id: id,
             error: initial_error,
             query_error: error,
             submit_attempts: 1,
             query_attempts: attempt
           }}
        else
          backoff = min(delay * Integer.pow(2, attempt - 1), max_delay)
          sleep.(max(backoff, (error.retry_after || 0) * 1_000))

          reconcile(
            client,
            symbol,
            id,
            initial_error,
            attempt + 1,
            max_attempts,
            delay,
            max_delay,
            sleep
          )
        end
    end
  end

  defp retryable_query?(%Error{code: -2013}), do: true
  defp retryable_query?(%Error{type: :transport}), do: true
  defp retryable_query?(%Error{status: 429}), do: true

  defp retryable_query?(%Error{status: status}) when is_integer(status) and status >= 500,
    do: true

  defp retryable_query?(_), do: false

  defp client_order_id(%{"newClientOrderId" => id}) when is_binary(id) and byte_size(id) > 0,
    do: {:ok, id}

  defp client_order_id(_),
    do: {:error, %Error{type: :validation, message: "newClientOrderId is required"}}

  defp rejected(message),
    do: {:error, %{state: :rejected, error: %Error{type: :validation, message: message}}}
end
