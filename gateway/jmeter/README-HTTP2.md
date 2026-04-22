# HTTP/2 (h2c) smoke and load

The gateway and services use **RFC 7540 HTTP/2 over cleartext TCP** with **prior knowledge** (no HTTP/1.1 upgrade). Clients must send the connection preface (`PRI * HTTP/2.0\\r\\n...`) before any HTTP/2 frames.

## Quick manual check (curl)

Use curl with HTTP/2 prior knowledge against the gateway (default port **8080**):

```bash
# Example: deposit (requires JSON body matching BankingHttpRoutes)
curl --http2-prior-knowledge -sS -X POST "http://127.0.0.1:8080/api/v1/transactions/deposit" \
  -H "content-type: application/json" \
  -d '{"requestId":"smoke-1","accountId":"acc-1","amount":10}'
```

Other routes follow `/api/v1/...` as documented in `BankingHttpRoutes` in `shared-core` (accounts create/balance/credit/debit, transactions deposit/withdraw/transfer). Legacy path `POST /accounts/deposit` is accepted for compatibility with older JMeter plans.

## JMeter

- Use a **HTTP/2** capable implementation (JMeter 5.6+ with HTTP/2 plugin, or a TCP sampler that sends the binary preface + SETTINGS + HEADERS/DATA frames).
- Target URL: `http://<gateway-host>:8080` with path such as `/api/v1/transactions/deposit`.
- **Do not** use plain HTTP/1.1 `HttpClient4` sampler against `--protocol=http`; the wire format is HTTP/2 binary, not HTTP/1.1 text.

## Stack bring-up

From `versioned-value`:

```bash
./bash.sh http
```

Ensure `ACCOUNT_INSTANCES` / `TRANSACTION_INSTANCES` env vars list at least two endpoints each if you override defaults (`bash.sh` sets CSV pairs for the gateway).
