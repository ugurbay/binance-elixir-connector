defmodule BinanceElixir.Transport do
  @moduledoc "HTTP transport contract; implement this to inject a custom client."

  @callback request(atom(), String.t(), [{String.t(), String.t()}], String.t(), pos_integer()) ::
              {:ok, integer(), [{String.t(), String.t()}], binary()} | {:error, term()}
end
