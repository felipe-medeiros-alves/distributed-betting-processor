# distributed-betting-processor

Serviço Go (Uber Fx) para processamento distribuído de apostas: API HTTP, consumidor SQS FIFO, PostgreSQL, Keycloak e LocalStack.

## Pré-requisitos

- Go 1.23+
- Docker e Docker Compose
- `curl` e `jq` (exemplos abaixo)

## Subir o ambiente

```sh
docker compose -f deploy/docker-compose.yml up --build
```

Serviços:

| Serviço     | URL |
|-------------|-----|
| API         | http://localhost:8080 |
| Keycloak    | http://localhost:8081 (admin/admin) |
| LocalStack  | http://localhost:4566 |
| PostgreSQL  | localhost:5432 (`betting` / `betting`) |

Copie variáveis de exemplo:

```sh
cp .env.example .env
```

## Migrations

No Compose, o serviço `migrate` aplica `migrations/` automaticamente.

Manual:

```sh
export DATABASE_URL=postgres://betting:betting@localhost:5432/betting?sslmode=disable
go run ./cmd/migrate -dir migrations
```

Reversão (requer ferramenta migrate CLI ou equivalente):

```sh
migrate -path migrations -database "$DATABASE_URL" down 1
```

## Executar a aplicação local (fora do Docker)

```sh
export $(grep -v '^#' .env.example | xargs)   # ajuste conforme necessário
go run ./cmd/migrate -dir migrations
go run ./cmd/processor
```

## Token Keycloak (client credentials)

Provedor:

```sh
TOKEN=$(curl -s -X POST 'http://localhost:8081/realms/betting/protocol/openid-connect/token' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials&client_id=provider-a&client_secret=provider-a-secret' \
  | jq -r .access_token)
```

Serviço interno:

```sh
INTERNAL=$(curl -s -X POST 'http://localhost:8081/realms/betting/protocol/openid-connect/token' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials&client_id=internal-service&client_secret=internal-service-secret' \
  | jq -r .access_token)
```

> Ao rodar a API no host, use `OIDC_ISSUER=http://localhost:8081/realms/betting`. No container, o issuer do token deve corresponder ao configurado na API.

## Exemplos HTTP

Abrir carteira:

```sh
curl -s -X POST http://localhost:8080/wallets \
  -H "Authorization: Bearer $INTERNAL" \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

Aposta:

```sh
curl -s -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{
    "providerId":"provider-a",
    "externalTransactionId":"transaction-123",
    "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId":"<WALLET_ID>",
    "roundId":"round-987",
    "gameId":"fortune-chimp",
    "kind":"BET",
    "money":{"amount":"25.00","currency":"BRL"}
  }'
```

Health:

```sh
curl -s http://localhost:8080/health/live
curl -s http://localhost:8080/health/ready
```

## Testes

Unitários:

```sh
go test ./...
go test -race ./internal/domain/...
go vet ./...
```

Integração (PostgreSQL via testcontainers):

```sh
go test -tags=integration -race ./internal/integration/...
```

Múltiplas instâncias: suba vários containers `processor` com portas diferentes ou escale o serviço no Compose e repita os cenários de concorrência descritos no desafio.

## Filas SQS

Provisionadas por `deploy/localstack-init.sh`:

- `wager-transactions.fifo` (+ DLQ `wager-transactions-dlq.fifo`)
- `wallet-events.fifo` (saída da outbox)

`MessageGroupId`: `walletId` na entrada; `aggregateId` na outbox.

## Documentação de decisões

Ver [ARCHITECTURE.md](ARCHITECTURE.md).
