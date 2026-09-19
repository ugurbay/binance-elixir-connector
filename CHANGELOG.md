# Changelog

## 0.1.0

Initial unofficial Binance Spot REST connector:

- HMAC SHA-256 signed requests with Binance timing parameters.
- Public Spot market WebSocket streams with TLS verification, ping/pong, reconnect, renewal, and bounded events.
- Public market data, account queries, order queries, test orders, placement, and cancellation.
- Conservative GET retry handling and no automatic retry of writes.
- Explicit unknown execution errors and caller-provided client order IDs for placement.
- TLS hostname verification, response rate-limit metadata, injectable transport, and unit tests.

This release does not include signed User Data Stream, WebSocket API, Futures, RSA/Ed25519, shared rate-limit coordination, or signed testnet integration tests.
