defmodule BinanceElixir.Spot.Events do
  @moduledoc "Lossless normalization of supported Spot market and account events."

  alias BinanceElixir.Spot.Filters

  @market_decimals ~w(a A b B c C h l o p P q Q v w x)
  @execution_decimals ~w(A B F L l n p P q Q Y Z z)

  @spec market(term()) :: map()
  def market(%{"stream" => stream, "data" => data}) do
    Map.put(market(data), :stream, stream)
  end

  def market(events) when is_list(events),
    do: %{
      kind: :market_event_batch,
      event_type: "miniTickerArray",
      events: Enum.map(events, &market/1)
    }

  def market(event) when is_map(event) do
    event
    |> parse_fields(@market_decimals)
    |> Map.put(:kind, :market_event)
    |> Map.put(:event_type, event["e"] || if(event["lastUpdateId"], do: "depth", else: nil))
  end

  def market(other), do: %{kind: :unknown, raw: other}

  @spec user(term()) :: map()
  def user(%{"status" => status} = response) do
    %{
      kind: :response,
      id: response["id"],
      status: status,
      result: response["result"],
      error: response["error"],
      rate_limits: response["rateLimits"]
    }
  end

  def user(%{"event" => event} = envelope) when is_map(event) do
    normalized = normalize_user_event(event)
    Map.put(normalized, :subscription_id, envelope["subscriptionId"])
  end

  def user(event) when is_map(event), do: normalize_user_event(event)
  def user(other), do: %{kind: :unknown, raw: other}

  defp normalize_user_event(%{"e" => "executionReport"} = event) do
    event
    |> parse_fields(@execution_decimals)
    |> Map.merge(%{
      kind: :user_event,
      event_type: "executionReport",
      symbol: event["s"],
      client_order_id: event["c"],
      original_client_order_id: event["C"],
      execution_type: event["x"],
      order_status: event["X"],
      order_id: event["i"]
    })
  end

  defp normalize_user_event(%{"e" => "outboundAccountPosition"} = event) do
    balances =
      case event["B"] do
        list when is_list(list) -> Enum.map(list, &parse_fields(&1, ~w(f l)))
        other -> other
      end

    event
    |> Map.put("B", balances)
    |> Map.merge(%{kind: :user_event, event_type: "outboundAccountPosition"})
  end

  defp normalize_user_event(%{"e" => "balanceUpdate"} = event) do
    event
    |> parse_fields(~w(d))
    |> Map.merge(%{kind: :user_event, event_type: "balanceUpdate"})
  end

  defp normalize_user_event(event),
    do: Map.merge(event, %{kind: :user_event, event_type: event["e"]})

  defp parse_fields(event, fields) when is_map(event) do
    Enum.reduce(fields, event, fn field, acc ->
      case Map.fetch(acc, field) do
        {:ok, value} ->
          case Filters.decimal(value) do
            {:ok, decimal} -> Map.put(acc, field, decimal)
            :error -> acc
          end

        :error ->
          acc
      end
    end)
  end

  defp parse_fields(other, _fields), do: other
end
