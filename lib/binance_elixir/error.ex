defmodule BinanceElixir.Error do
  @moduledoc "An HTTP, Binance API, validation, or transport failure."
  defexception [
    :type,
    :status,
    :code,
    :message,
    :retry_after,
    :reason,
    :headers,
    :client_order_id,
    unknown_execution?: false
  ]

  @type t :: %__MODULE__{
          type: atom() | nil,
          status: integer() | nil,
          code: integer() | nil,
          message: String.t() | nil,
          retry_after: non_neg_integer() | nil,
          reason: term(),
          headers: map() | nil,
          client_order_id: String.t() | nil,
          unknown_execution?: boolean()
        }
end
