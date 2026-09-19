# Changelog

## 0.2.1

- Prepared the Hex.pm release, updated installation instructions and package documentation metadata.

## 0.2.0

- Default REST host is now Spot testnet. Production writes require an explicit opt-in.
- Added exact decimal order filter preflight and single submission with bounded reconciliation by durable client order ID.
- Added signed User Data Stream via WebSocket API, normalized events, and dynamic public market subscriptions.
- Added Ed25519 signing alongside HMAC, midpoint server time synchronization, and shared rate limit observations.
- Expanded Spot REST endpoint helpers and testnet acceptance scripts.

## 0.1.0

Initial unofficial Binance Spot REST connector:

- HMAC SHA-256 signed requests with Binance timing parameters.
- Public Spot market WebSocket streams with TLS verification, ping/pong, reconnect, renewal, and bounded events.
- Public market data, account queries, order queries, test orders, placement, and cancellation.
- Conservative GET retry handling and no automatic retry of writes.
- Explicit unknown execution errors and caller-provided client order IDs for placement.
- TLS hostname verification, response rate-limit metadata, injectable transport, and unit tests.

This tagged release does not include signed User Data Stream, WebSocket API, Futures, RSA/Ed25519, shared rate-limit coordination, or automated signed testnet checks in CI.
