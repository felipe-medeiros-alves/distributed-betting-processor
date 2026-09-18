package observe

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricsOnce sync.Once
	shared      *Metrics
)

type Metrics struct {
	TransactionsTotal      *prometheus.CounterVec
	IdempotentReplays      prometheus.Counter
	ConcurrencyConflicts   prometheus.Counter
	OutboxLag              prometheus.Gauge
	ReconciliationMismatch prometheus.Counter
	DLQMessages            prometheus.Counter
}

func NewMetrics() *Metrics {
	metricsOnce.Do(func() {
		shared = newMetrics()
	})
	return shared
}

func newMetrics() *Metrics {
	return &Metrics{
		TransactionsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_transactions_total",
			Help: "Wager transaction outcomes by status",
		}, []string{"status"}),
		IdempotentReplays: promauto.NewCounter(prometheus.CounterOpts{
			Name: "wager_idempotent_replays_total",
			Help: "Idempotent replays served",
		}),
		ConcurrencyConflicts: promauto.NewCounter(prometheus.CounterOpts{
			Name: "wallet_concurrency_conflicts_total",
			Help: "Optimistic version conflicts",
		}),
		OutboxLag: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_pending_events",
			Help: "Unpublished outbox events",
		}),
		ReconciliationMismatch: promauto.NewCounter(prometheus.CounterOpts{
			Name: "wallet_reconciliation_mismatch_total",
			Help: "Reconciliation mismatches detected",
		}),
		DLQMessages: promauto.NewCounter(prometheus.CounterOpts{
			Name: "sqs_dlq_messages_total",
			Help: "Messages sent to DLQ",
		}),
	}
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
