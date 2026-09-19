defmodule BinanceElixir do
  @moduledoc """
  Binance Spot REST client. Requests return `{:ok, %BinanceElixir.Response{}}` or
  `{:error, %BinanceElixir.Error{}}`.

  Write requests are never automatically retried: after a timeout or HTTP 5xx,
  Binance may have executed an order even though its result is unknown.
  """

  alias BinanceElixir.{Error, Params, RateLimit, Response, Signature}

  @default_base_url "https://testnet.binance.vision"
  @allowed_methods [:get, :post, :put, :delete]
  @allowed_security [:none, :api_key, :signed]

  @derive {Inspect, except: [:api_key, :api_secret, :private_key]}
  defstruct base_url: @default_base_url,
            api_key: nil,
            api_secret: nil,
            signing_algorithm: :hmac,
            private_key: nil,
            enable_live_trading?: false,
            recv_window: 5_000,
            timeout: 10_000,
            retries: 2,
            backoff_ms: 250,
            transport: BinanceElixir.Transport.Httpc,
            rate_tracker: nil,
            clock: nil,
            sleep: &Process.sleep/1

  @type t :: %__MODULE__{}

  @doc "Creates a client. The default REST host is Binance Spot testnet."
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    client = struct!(__MODULE__, opts)
    uri = URI.parse(client.base_url)

    unless uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and
             is_nil(uri.query) and is_nil(uri.fragment) do
      raise ArgumentError, "base_url must be an HTTPS origin without query or fragment"
    end

    unless is_integer(client.recv_window) and client.recv_window > 0 and
             client.recv_window <= 60_000 do
      raise ArgumentError, "recv_window must be between 1 and 60000 milliseconds"
    end

    unless is_integer(client.timeout) and client.timeout > 0 and
             is_integer(client.retries) and client.retries >= 0 and
             is_integer(client.backoff_ms) and client.backoff_ms >= 0 do
      raise ArgumentError, "timeout, retries, and backoff_ms must be non-negative integers"
    end

    unless is_boolean(client.enable_live_trading?) do
      raise ArgumentError, "enable_live_trading? must be a boolean"
    end

    unless client.signing_algorithm in [:hmac, :ed25519] do
      raise ArgumentError, "signing_algorithm must be :hmac or :ed25519"
    end

    if client.signing_algorithm == :ed25519 and
         Signature.seed(client.private_key) == {:error, :invalid_private_key} do
      raise ArgumentError, "private_key must be unencrypted Ed25519 PKCS#8 PEM"
    end

    %{
      client
      | base_url: String.trim_trailing(client.base_url, "/"),
        clock: client.clock || fn -> System.system_time(:millisecond) end
    }
  end

  @doc """
  Sends a Spot REST request. `security` is `:none`, `:api_key`, or `:signed`.
  Signed requests use the configured HMAC SHA-256 or Ed25519 key and a millisecond timestamp.
  """
  @spec request(t(), atom(), String.t(), map() | keyword(), keyword()) ::
          {:ok, Response.t()} | {:error, Error.t()}
  def request(%__MODULE__{} = client, method, path, params \\ %{}, opts \\ []) do
    security = Keyword.get(opts, :security, :none)

    cond do
      method not in @allowed_methods ->
        invalid("unsupported HTTP method")

      security not in @allowed_security ->
        invalid("unsupported security type")

      method in [:post, :put, :delete] and not client.enable_live_trading? and
          not testnet?(client.base_url) ->
        invalid("production writes require enable_live_trading?: true")

      not (is_binary(path) and String.starts_with?(path, "/") and
             not String.starts_with?(path, "//") and not String.contains?(path, ["?", "#"])) ->
        invalid("path must be an absolute API path without query or fragment")

      security != :none and blank?(client.api_key) ->
        invalid("api_key is required")

      security == :signed and client.signing_algorithm == :hmac and blank?(client.api_secret) ->
        invalid("api_secret is required")

      security == :signed and client.signing_algorithm == :ed25519 and blank?(client.private_key) ->
        invalid("private_key is required")

      true ->
        do_request(client, method, path, params, security, client.retries)
    end
  end

  defp build_query(client, params, :signed) do
    with {:ok, pairs} <- normalize_params(params),
         false <-
           Enum.any?(pairs, fn {key, _} ->
             to_string(key) in ["signature", "timestamp", "recvWindow"]
           end),
         {:ok, query} <-
           Params.encode(
             pairs ++ [{"recvWindow", client.recv_window}, {"timestamp", client.clock.()}]
           ) do
      signature = Signature.sign(client, query)
      {:ok, encoded_signature} = Params.encode(%{signature: signature})

      {:ok, query <> "&" <> encoded_signature}
    else
      true -> {:error, "signature, timestamp, and recvWindow are managed by the client"}
      error -> error
    end
  end

  defp build_query(_client, params, _security), do: Params.encode(params)

  defp normalize_params(params) when is_map(params), do: normalize_params(Map.to_list(params))

  defp normalize_params(params) when is_list(params) do
    if Enum.all?(params, fn
         {key, _value} when is_atom(key) or is_binary(key) -> true
         _ -> false
       end) do
      {:ok, params}
    else
      {:error, "params must contain string or atom keys paired with values"}
    end
  end

  defp normalize_params(_), do: {:error, "params must be a map or keyword list"}

  defp request_headers(client, security) do
    [{"accept", "application/json"}] ++
      if(security == :none, do: [], else: [{"x-mbx-apikey", client.api_key}])
  end

  defp do_request(client, method, path, params, security, remaining) do
    with {:ok, query} <- build_query(client, params, security) do
      headers = request_headers(client, security)
      url = client.base_url <> path <> if(query == "", do: "", else: "?" <> query)

      send_request(client, method, path, params, security, remaining, url, headers)
    else
      {:error, reason} -> invalid(reason)
    end
  end

  defp send_request(client, method, path, params, security, remaining, url, headers) do
    if client.rate_tracker,
      do: RateLimit.record_attempt(client.rate_tracker, estimate_weight(method, path, params))

    case client.transport.request(method, url, headers, "", client.timeout) do
      {:ok, status, response_headers, body} ->
        if client.rate_tracker,
          do: RateLimit.record_response(client.rate_tracker, response_headers)

        parsed_headers =
          Map.new(response_headers, fn {k, v} -> {String.downcase(to_string(k)), to_string(v)} end)

        result = decode_response(status, parsed_headers, body, method)

        if method == :get and remaining > 0 and retryable?(status, parsed_headers) do
          client.sleep.(
            retry_delay(parsed_headers, client.backoff_ms, client.retries - remaining)
          )

          do_request(client, method, path, params, security, remaining - 1)
        else
          result
        end

      {:error, reason} ->
        if method == :get and remaining > 0 do
          client.sleep.(backoff(client.backoff_ms, client.retries - remaining))
          do_request(client, method, path, params, security, remaining - 1)
        else
          {:error,
           %Error{
             type: :transport,
             message: "HTTP transport failed",
             reason: reason,
             unknown_execution?: method != :get
           }}
        end
    end
  end

  defp decode_response(status, headers, body, method) do
    decoded = if body == "", do: {:ok, nil}, else: Jason.decode(body)

    case decoded do
      {:ok, data} when status >= 200 and status < 300 ->
        rate_limits =
          Map.filter(headers, fn {name, _} ->
            String.starts_with?(name, "x-mbx-used-weight-") or
              String.starts_with?(name, "x-mbx-order-count-")
          end)

        {:ok, %Response{data: data, status: status, headers: headers, rate_limits: rate_limits}}

      _ ->
        data =
          case decoded do
            {:ok, value} -> value
            _ -> nil
          end

        code = if is_map(data), do: Map.get(data, "code"), else: nil
        message = if is_map(data), do: Map.get(data, "msg"), else: nil

        {:error,
         %Error{
           type: if(status >= 200 and status < 300, do: :decode, else: :api),
           status: status,
           code: code,
           message:
             message ||
               if(status >= 200 and status < 300,
                 do: "invalid JSON response",
                 else: "HTTP #{status}"
               ),
           reason: if(data == nil, do: body, else: data),
           headers: headers,
           retry_after: retry_after(headers),
           unknown_execution?:
             method != :get and
               (status >= 500 or status == 409 or code in [-1006, -1007] or
                  (status >= 200 and status < 300))
         }}
    end
  end

  defp retryable?(429, headers), do: is_integer(retry_after(headers))
  defp retryable?(status, _headers), do: status >= 500

  defp retry_delay(headers, base, attempt) do
    case retry_after(headers) do
      nil -> backoff(base, attempt)
      seconds -> seconds * 1_000
    end
  end

  defp backoff(base, attempt), do: min(base * Integer.pow(2, attempt), 30_000)

  defp retry_after(headers) do
    case Integer.parse(Map.get(headers, "retry-after", "")) do
      {seconds, ""} when seconds >= 0 -> seconds
      _ -> nil
    end
  end

  defp blank?(value), do: not (is_binary(value) and byte_size(value) > 0)

  defp testnet?(url),
    do: URI.parse(url).host in ["testnet.binance.vision", "api1.testnet.binance.vision"]

  defp estimate_weight(method, path, params) do
    limit =
      case Integer.parse(to_string(param(params, "limit") || "")) do
        {value, ""} when value > 0 -> value
        _ -> nil
      end

    case path do
      "/api/v3/ping" ->
        1

      "/api/v3/time" ->
        1

      "/api/v3/exchangeInfo" ->
        20

      "/api/v3/depth" ->
        cond do
          is_nil(limit) -> 5
          limit <= 100 -> 5
          limit <= 500 -> 25
          limit <= 1_000 -> 50
          true -> 250
        end

      "/api/v3/klines" ->
        2

      "/api/v3/ticker/price" ->
        if(param(params, "symbol"), do: 2, else: 4)

      "/api/v3/ticker/bookTicker" ->
        if(param(params, "symbol"), do: 2, else: 4)

      "/api/v3/ticker/24hr" ->
        if(param(params, "symbol"), do: 2, else: 80)

      "/api/v3/account" ->
        20

      "/api/v3/myTrades" ->
        if(param(params, "orderId"), do: 5, else: 20)

      "/api/v3/openOrders" ->
        if(param(params, "symbol"), do: 6, else: 80)

      "/api/v3/order" ->
        if(method == :get, do: 4, else: 1)

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp param(params, name) when is_map(params),
    do:
      Map.get(params, name) ||
        Enum.find_value(params, fn {key, value} -> if to_string(key) == name, do: value end)

  defp param(params, name) when is_list(params),
    do: Enum.find_value(params, fn {key, value} -> if to_string(key) == name, do: value end)

  defp param(_, _), do: nil

  defp invalid(message), do: {:error, %Error{type: :validation, message: message}}
end
