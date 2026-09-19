defmodule BinanceElixir.Spot.Filters do
  @moduledoc "Exact-decimal local preflight for Spot LIMIT, MARKET, and STOP_LOSS orders."

  alias BinanceElixir.Error

  @sides ["BUY", "SELL"]
  @types ["LIMIT", "MARKET", "STOP_LOSS"]
  @time_in_force ["GTC", "IOC", "FOK"]

  @spec validate(map(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, Error.t()}
  def validate(symbol_info, params, opts \\ []) do
    try do
      order = normalize(params)
      validate_shape!(order)
      validate_symbol!(symbol_info, order)
      filters = filter_index!(symbol_info)
      validate_filters!(filters, order, opts)
      {:ok, order}
    catch
      :throw, {:invalid, message} -> {:error, %Error{type: :validation, message: message}}
    end
  end

  @doc "Parses a plain decimal string or integer without accepting floats or exponent notation."
  @spec decimal(String.t() | integer()) :: {:ok, Decimal.t()} | :error
  def decimal(value) when is_integer(value), do: {:ok, Decimal.new(value)}

  def decimal(%Decimal{} = value) do
    if not Decimal.nan?(value) and not Decimal.inf?(value) and
         byte_size(Decimal.to_string(value, :normal)) <= 64,
       do: {:ok, value},
       else: :error
  end

  def decimal(value) when is_binary(value) do
    if byte_size(value) <= 64 and Regex.match?(~r/\A\d+(?:\.\d+)?\z/, value) do
      {:ok, Decimal.new(value)}
    else
      :error
    end
  end

  def decimal(_), do: :error

  defp normalize(params) when is_map(params),
    do: Map.new(params, fn {key, value} -> {to_string(key), value} end)

  defp normalize(params) when is_list(params) do
    if Keyword.keyword?(params),
      do: normalize(Map.new(params)),
      else: invalid!("order params must be a map or keyword list")
  end

  defp normalize(_), do: invalid!("order params must be a map or keyword list")

  defp validate_shape!(order) do
    require_one_of!(order, "side", @sides)
    require_one_of!(order, "type", @types)
    require_string!(order, "symbol")

    if Map.has_key?(order, "quantity") and Map.has_key?(order, "quoteOrderQty") do
      invalid!("quantity and quoteOrderQty cannot be combined")
    end

    case order["type"] do
      "LIMIT" ->
        require_one_of!(order, "timeInForce", @time_in_force)
        positive!(order, "quantity")
        positive!(order, "price")

        if Map.has_key?(order, "stopPrice") or Map.has_key?(order, "quoteOrderQty"),
          do: invalid!("invalid LIMIT parameters")

      "MARKET" ->
        unless Map.has_key?(order, "quantity") or Map.has_key?(order, "quoteOrderQty"),
          do: invalid!("MARKET requires quantity or quoteOrderQty")

        if Map.has_key?(order, "quantity"), do: positive!(order, "quantity")
        if Map.has_key?(order, "quoteOrderQty"), do: positive!(order, "quoteOrderQty")

        if Map.has_key?(order, "price") or Map.has_key?(order, "timeInForce") or
             Map.has_key?(order, "stopPrice"),
           do: invalid!("invalid MARKET parameters")

      "STOP_LOSS" ->
        positive!(order, "quantity")
        positive!(order, "stopPrice")

        if Map.has_key?(order, "price") or Map.has_key?(order, "timeInForce") or
             Map.has_key?(order, "quoteOrderQty"),
           do: invalid!("invalid STOP_LOSS parameters")
    end
  end

  defp validate_symbol!(symbol_info, order) when is_map(symbol_info) do
    if get(symbol_info, "symbol") != order["symbol"],
      do: invalid!("order symbol does not match exchangeInfo")

    if get(symbol_info, "status") != "TRADING", do: invalid!("symbol is not TRADING")
    if get(symbol_info, "isSpotTradingAllowed") == false, do: invalid!("Spot trading is disabled")

    unless order["type"] in (get(symbol_info, "orderTypes") || []),
      do: invalid!("order type is disabled for symbol")

    if Map.has_key?(order, "quoteOrderQty") and
         get(symbol_info, "quoteOrderQtyMarketAllowed") != true do
      invalid!("quoteOrderQty is disabled for symbol")
    end
  end

  defp validate_symbol!(_, _), do: invalid!("symbol_info must be an exchangeInfo symbol")

  defp filter_index!(symbol_info) do
    filters = get(symbol_info, "filters")
    unless is_list(filters), do: invalid!("exchangeInfo filters must be a list")

    Enum.reduce(filters, %{}, fn filter, acc ->
      type = if is_map(filter), do: get(filter, "filterType"), else: nil

      unless is_binary(type) and not Map.has_key?(acc, type),
        do: invalid!("invalid or duplicate Binance filter")

      Map.put(acc, type, filter)
    end)
  end

  defp validate_filters!(filters, order, opts) do
    price_filter = filters["PRICE_FILTER"]
    lot_filter = filters["LOT_SIZE"]
    market_lot_filter = filters["MARKET_LOT_SIZE"]

    if price_filter && order["price"],
      do: range!(order["price"], price_filter, "minPrice", "maxPrice", "tickSize", "price")

    if price_filter && order["stopPrice"],
      do:
        range!(order["stopPrice"], price_filter, "minPrice", "maxPrice", "tickSize", "stopPrice")

    if lot_filter && order["quantity"],
      do: range!(order["quantity"], lot_filter, "minQty", "maxQty", "stepSize", "quantity")

    if market_lot_filter && order["type"] == "MARKET" && order["quantity"] do
      range!(order["quantity"], market_lot_filter, "minQty", "maxQty", "stepSize", "quantity")
    end

    if min_filter = filters["MIN_NOTIONAL"] do
      if order["type"] == "LIMIT" or get(min_filter, "applyToMarket") == true do
        notional = notional!(order, opts)
        minimum!(notional, required_decimal!(min_filter, "minNotional"), "MIN_NOTIONAL")
      end
    end

    if notional_filter = filters["NOTIONAL"] do
      market? = order["type"] != "LIMIT"
      min? = not market? or get(notional_filter, "applyMinToMarket") == true
      max? = not market? or get(notional_filter, "applyMaxToMarket") == true

      if min? or max? do
        notional = notional!(order, opts)

        if min?,
          do: minimum!(notional, required_decimal!(notional_filter, "minNotional"), "NOTIONAL")

        if max?,
          do: maximum!(notional, required_decimal!(notional_filter, "maxNotional"), "NOTIONAL")
      end
    end
  end

  defp range!(value, filter, min_key, max_key, step_key, field) do
    value = exact!(value, field)
    minimum!(value, required_decimal!(filter, min_key), field)
    maximum!(value, required_decimal!(filter, max_key), field)
    step = required_decimal!(filter, step_key)

    if Decimal.gt?(step, 0) and not Decimal.eq?(Decimal.rem(value, step), 0) do
      invalid!("#{field} is not aligned with #{step_key}")
    end
  end

  defp notional!(order, opts) do
    cond do
      order["type"] == "LIMIT" ->
        Decimal.mult(exact!(order["price"], "price"), exact!(order["quantity"], "quantity"))

      order["quoteOrderQty"] ->
        exact!(order["quoteOrderQty"], "quoteOrderQty")

      true ->
        reference = Keyword.get(opts, :reference_price)

        if is_nil(reference),
          do: invalid!("reference_price is required for market notional validation")

        Decimal.mult(exact!(reference, "reference_price"), exact!(order["quantity"], "quantity"))
    end
  end

  defp minimum!(value, minimum, field) do
    if Decimal.gt?(minimum, 0) and Decimal.lt?(value, minimum),
      do: invalid!("#{field} is below minimum")
  end

  defp maximum!(value, maximum, field) do
    if Decimal.gt?(maximum, 0) and Decimal.gt?(value, maximum),
      do: invalid!("#{field} is above maximum")
  end

  defp required_decimal!(map, key), do: exact!(get(map, key), key)

  defp positive!(map, key) do
    unless Decimal.gt?(exact!(map[key], key), 0), do: invalid!("#{key} must be positive")
  end

  defp exact!(value, key) do
    case decimal(value) do
      {:ok, number} -> number
      :error -> invalid!("#{key} must be an exact plain decimal")
    end
  end

  defp require_one_of!(map, key, allowed) do
    unless map[key] in allowed, do: invalid!("#{key} is unsupported")
  end

  defp require_string!(map, key) do
    unless is_binary(map[key]) and byte_size(map[key]) > 0, do: invalid!("#{key} is required")
  end

  defp get(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        case Enum.find(map, fn {entry, _} -> is_atom(entry) and Atom.to_string(entry) == key end) do
          {_, value} -> value
          nil -> nil
        end
    end
  end

  defp invalid!(message), do: throw({:invalid, message})
end
