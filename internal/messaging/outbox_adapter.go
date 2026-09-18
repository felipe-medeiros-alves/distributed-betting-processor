package messaging

import (
	"context"
	"time"

	"github.com/felipemalves/distributed-betting-processor/internal/domain"
	"github.com/felipemalves/distributed-betting-processor/internal/postgres"
	"github.com/google/uuid"
)

type OutboxRepoAdapter struct {
	Repos *postgres.Repos
}

func (a OutboxRepoAdapter) ClaimPending(ctx context.Context, workerID string, lease time.Duration, limit int) ([]domain.OutboxEvent, error) {
	return a.Repos.ClaimOutbox(ctx, workerID, lease, limit)
}

func (a OutboxRepoAdapter) MarkPublished(ctx context.Context, eventID uuid.UUID) error {
	return a.Repos.MarkOutboxPublished(ctx, eventID)
}

func (a OutboxRepoAdapter) ScheduleRetry(ctx context.Context, eventID uuid.UUID, next time.Time) error {
	return a.Repos.ScheduleOutboxRetry(ctx, eventID, next)
}

func (a OutboxRepoAdapter) CountPending(ctx context.Context) (int64, error) {
	return a.Repos.CountPendingOutbox(ctx)
}
