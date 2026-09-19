defmodule BinanceElixir.Transport.Httpc do
  @moduledoc false
  @behaviour BinanceElixir.Transport

  @impl true
  def request(method, url, headers, body, timeout) do
    request_headers = Enum.map(headers, fn {name, value} -> {~c"#{name}", ~c"#{value}"} end)
    url_chars = String.to_charlist(url)

    request =
      if method in [:get, :head] do
        {url_chars, request_headers}
      else
        {url_chars, request_headers, ~c"application/x-www-form-urlencoded", body}
      end

    ssl_options = :httpc.ssl_verify_host_options(true)

    http_options = [
      timeout: timeout,
      connect_timeout: timeout,
      ssl: ssl_options,
      autoredirect: false
    ]

    case :httpc.request(method, request, http_options, body_format: :binary) do
      {:ok, {{_version, status, _reason}, response_headers, response_body}} ->
        {:ok, status, Enum.map(response_headers, fn {k, v} -> {to_string(k), to_string(v)} end),
         response_body}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
