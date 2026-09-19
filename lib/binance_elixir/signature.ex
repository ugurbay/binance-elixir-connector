defmodule BinanceElixir.Signature do
  @moduledoc "HMAC-SHA256 and Ed25519 signing for Binance REST and WebSocket API."

  @ed25519_pkcs8_prefix <<0x30, 0x2E, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70,
                          0x04, 0x22, 0x04, 0x20>>

  @spec sign(BinanceElixir.t(), binary()) :: binary()
  def sign(%{signing_algorithm: :hmac, api_secret: secret}, payload) do
    :crypto.mac(:hmac, :sha256, secret, payload) |> Base.encode16(case: :lower)
  end

  def sign(%{signing_algorithm: :ed25519, private_key: pem}, payload) do
    {:ok, seed} = seed(pem)
    :crypto.sign(:eddsa, :none, payload, [seed, :ed25519]) |> Base.encode64()
  end

  @doc "Extracts the 32-byte Ed25519 seed from unencrypted RFC 8410 PKCS#8 PEM."
  @spec seed(binary()) :: {:ok, binary()} | {:error, :invalid_private_key}
  def seed(pem) when is_binary(pem) do
    case Regex.run(
           ~r/\A\s*-----BEGIN PRIVATE KEY-----\s+([A-Za-z0-9+\/=\s]+?)\s+-----END PRIVATE KEY-----\s*\z/s,
           pem
         ) do
      [_, encoded] ->
        with {:ok, der} <- Base.decode64(String.replace(encoded, ~r/\s+/, "")),
             <<@ed25519_pkcs8_prefix, seed::binary-size(32)>> <- der do
          {:ok, seed}
        else
          _ -> {:error, :invalid_private_key}
        end

      _ ->
        {:error, :invalid_private_key}
    end
  end

  def seed(_), do: {:error, :invalid_private_key}

  @spec ws_payload(map()) :: binary()
  def ws_payload(params) when is_map(params) do
    params
    |> Enum.reject(fn {key, _} -> to_string(key) == "signature" end)
    |> Enum.map(fn {key, value} -> {to_string(key), to_string(value)} end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map_join("&", fn {key, value} -> "#{key}=#{value}" end)
  end
end
