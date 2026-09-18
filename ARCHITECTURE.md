# Architecture

## Money

Valores monetários usam `int64` em centavos (escala fixa 2) e código ISO 4217 no tipo `domain.Money`. Parsing de entradas externas rejeita negativos, notação científica e escala diferente de duas casas. Overflow é verificado em parse e aritmética.

Persistência: colunas `*_minor BIGINT` e `currency CHAR(3)`.

## Transações SQL

Cada caso de uso financeiro abre uma transação PostgreSQL (`UnitOfWork.WithinTx`) que engloba:

- lock da carteira (`SELECT … FOR UPDATE`);
- insert/update da `wager_transactions`;
- update otimista da carteira (`WHERE version = expected`);
- insert append-only no ledger;
- inbox (SQS) e outbox no mesmo commit quando aplicável.

Rejeições de negócio também são persistidas na mesma transação (estado terminal `REJECTED` + evento de outbox).

## Concorrência

Coordenação **por carteira** via lock pessimista na linha de `wallets`. Defesa adicional: coluna `version` incrementada a cada mudança de saldo com `UPDATE … WHERE version = $n`.

Locks globais não são usados. Carteiras distintas processam em paralelo.

## Idempotência

- Unique `(idempotency_key)` e `(provider_id, external_transaction_id)`.
- Hash SHA-256 de JSON canônico (chaves ordenadas) dos campos de negócio, **sem** header `Idempotency-Key`.
- Mesma chave + mesmo hash → replay com `idempotentReplay: true` e saldo observado no processamento original.
- Chave reutilizada com hash diferente → conflito HTTP 409.

## Referências pendentes

`REFUND` / `ROLLBACK` sem referência disponível → `PENDING_REFERENCE` + evento `WagerTransactionPendingReference`. Worker com backoff exponencial (1s base, cap 30s), máximo 20 tentativas / TTL 15 minutos. Esgotado → `REJECTED` com `REFERENCE_NOT_FOUND`.

## Reversões

- Uma reversão bem-sucedida por **tipo** (`REFUND` ou `ROLLBACK`) por referência externa.
- Após `REFUND` ou `ROLLBACK` processado de uma `BET`, outra reversão da mesma aposta é rejeitada (`REFERENCE_ALREADY_REVERSED`).
- Rollback que exigiria débito acima do saldo → `INSUFFICIENT_FUNDS_REVERSAL` (distinto de `INSUFFICIENT_FUNDS`).

## Inbox / Outbox

**Inbox:** unique `(consumer_name, message_id)` na mesma transação do processamento. Mensagem SQS só é removida após commit.

**Outbox:** eventos gravados antes da publicação. Worker usa `FOR UPDATE SKIP LOCKED`, lease por instância, republicação com o mesmo `eventId`. Destino local: fila FIFO `wallet-events.fifo`.

Eventos: `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference`, `WalletBalanceChanged`.

## Autenticação / autorização

Keycloak (OIDC), fluxo `client_credentials`. O `providerId` autorizado vem do client id do token (`azp` / `client_id`). Corpo da requisição deve coincidir.

- `internal-service`: abertura de carteira, leitura, ledger, reconciliação.
- Provedores: `POST /wagering/transactions` e leitura das próprias transações.

## Uber Fx

Composição em `internal/app`: config, pool pgx, serviços, HTTP, consumidor SQS, publishers e workers. `fx.Lifecycle` gerencia start/stop com timeout configurável.

## Limitações / escopo

- Fluxos principais em BRL; tipo `Money` suporta outras moedas com validação de compatibilidade.
- Partidas dobradas e OpenTelemetry não implementados (diferenciais opcionais).
- Em Docker, obtenha tokens Keycloak via host publicado (`localhost:8081`); configure `OIDC_ISSUER` conforme o host usado na emissão do JWT.
