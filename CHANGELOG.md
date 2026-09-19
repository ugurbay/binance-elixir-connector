# Changelog

## Unreleased

- Added repeatable live Spot testnet checks for public and signed REST, market WebSocket, and a bounded limit-order create/query/cancel cycle. These checks passed with testnet credentials; GitHub CI also passed.

## 0.1.0

Initial unofficial Binance Spot REST connector:

- HMAC SHA-256 signed requests with Binance timing parameters.
- Public Spot market WebSocket streams with TLS verification, ping/pong, reconnect, renewal, and bounded events.
- Public market data, account queries, order queries, test orders, placement, and cancellation.
- Conservative GET retry handling and no automatic retry of writes.
- Explicit unknown execution errors and caller-provided client order IDs for placement.
- TLS hostname verification, response rate-limit metadata, injectable transport, and unit tests.

This tagged release does not include signed User Data Stream, WebSocket API, Futures, RSA/Ed25519, shared rate-limit coordination, or automated signed testnet checks in CI.
