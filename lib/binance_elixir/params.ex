defmodule BinanceElixir.Params do
  @moduledoc false

  @spec encode(map() | keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def encode(params) when is_map(params) or is_list(params) do
    cond do
      not Enum.all?(params, fn
        {key, _value} when is_atom(key) or is_binary(key) -> true
        _ -> false
      end) ->
        {:error, "params must contain string or atom keys paired with values"}

      length(Enum.uniq_by(params, fn {key, _value} -> to_string(key) end)) != Enum.count(params) ->
        {:error, "duplicate parameter keys are not allowed"}

      true ->
        encode_pairs(params)
    end
  end

  def encode(_), do: {:error, "params must be a map or keyword list"}

  defp encode_pairs(params) do
    params
    |> Enum.sort_by(fn {key, _value} -> to_string(key) end)
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
      case value_to_string(value) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, string} -> {:cont, {:ok, [pair(key, string) | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, pairs} -> {:ok, pairs |> Enum.reverse() |> Enum.join("&")}
      error -> error
    end
  end

  defp pair(key, value),
    do:
      URI.encode(to_string(key), &URI.char_unreserved?/1) <>
        "=" <>
        URI.encode(value, &URI.char_unreserved?/1)

  defp value_to_string(nil), do: {:ok, nil}
  defp value_to_string(true), do: {:ok, "true"}
  defp value_to_string(false), do: {:ok, "false"}
  defp value_to_string(value) when is_binary(value), do: {:ok, value}
  defp value_to_string(value) when is_atom(value), do: {:ok, Atom.to_string(value)}
  defp value_to_string(value) when is_integer(value), do: {:ok, Integer.to_string(value)}

  defp value_to_string(%Decimal{} = value) do
    plain = Decimal.to_string(value, :normal)

    if byte_size(plain) <= 64 and Regex.match?(~r/\A-?\d+(?:\.\d+)?\z/, plain),
      do: {:ok, plain},
      else: {:error, "Decimal must be finite and plain"}
  end

  defp value_to_string(_),
    do:
      {:error,
       "parameter values must be strings, integers, booleans, atoms, or nil; use decimal strings for prices and quantities"}
end
