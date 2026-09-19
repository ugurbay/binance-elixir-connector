defmodule BinanceElixir.Response do
  @moduledoc "Decoded response with HTTP metadata and Binance rate-limit headers."
  defstruct [:data, :status, :headers, :rate_limits]

  @type t :: %__MODULE__{
          data: term(),
          status: integer(),
          headers: map(),
          rate_limits: map()
        }
end
