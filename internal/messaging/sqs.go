package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/felipemalves/distributed-betting-processor/internal/application"
	"github.com/felipemalves/distributed-betting-processor/internal/config"
	"github.com/felipemalves/distributed-betting-processor/internal/domain"
	"github.com/felipemalves/distributed-betting-processor/internal/observe"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

type SQSClient struct {
	client *sqs.Client
}

func NewSQSClient(cfg config.Config) (*SQSClient, error) {
	custom := awsconfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
		func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if cfg.AWSEndpoint != "" {
				return aws.Endpoint{URL: cfg.AWSEndpoint, HostnameImmutable: true}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		}))
	loadCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		custom,
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, err
	}
	return &SQSClient{client: sqs.NewFromConfig(loadCfg)}, nil
}

func (c *SQSClient) Ping(ctx context.Context, queueURL string) error {
	if queueURL == "" {
		return nil
	}
	_, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err
}

type WagerConsumer struct {
	client       *sqs.Client
	queueURL     string
	consumerName string
	visibility   int32
	wagers       *application.WagerService
	log          *slog.Logger
	stop         chan struct{}
	done         chan struct{}
}

func NewWagerConsumer(cfg config.Config, sqsClient *SQSClient, wagers *application.WagerService, log *slog.Logger) *WagerConsumer {
	return &WagerConsumer{
		client:       sqsClient.client,
		queueURL:     cfg.SQSQueueURL,
		consumerName: cfg.ConsumerName,
		visibility:   cfg.SQSVisibilitySec,
		wagers:       wagers,
		log:          log,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
}

type wagerEnvelope struct {
	MessageID  string `json:"messageId"`
	Type       string `json:"type"`
	OccurredAt string `json:"occurredAt"`
	Data       struct {
		ProviderID            string          `json:"providerId"`
		ExternalTransactionID string          `json:"externalTransactionId"`
		IdempotencyKey        string          `json:"idempotencyKey"`
		PlayerID              string          `json:"playerId"`
		WalletID              string          `json:"walletId"`
		RoundID               string          `json:"roundId"`
		GameID                string          `json:"gameId"`
		Kind                  string          `json:"kind"`
		Money                 domain.MoneyDTO `json:"money"`
		ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	} `json:"data"`
}

func (c *WagerConsumer) Start(ctx context.Context) {
	if c.queueURL == "" {
		close(c.done)
		return
	}
	go c.loop(ctx)
}

func (c *WagerConsumer) loop(ctx context.Context) {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     10,
			VisibilityTimeout:   c.visibility,
		})
		if err != nil {
			c.log.Error("sqs receive failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		for _, msg := range out.Messages {
			if err := c.handle(ctx, msg); err != nil {
				c.log.Error("sqs handle failed", "error", err)
				continue
			}
			_, _ = c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(c.queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
		}
	}
}

func (c *WagerConsumer) handle(ctx context.Context, msg types.Message) error {
	var env wagerEnvelope
	if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &env); err != nil {
		return err
	}
	messageID := env.MessageID
	if messageID == "" && msg.MessageId != nil {
		messageID = *msg.MessageId
	}
	hash := payloadHash(aws.ToString(msg.Body))
	in, err := envelopeToInput(env)
	if err != nil {
		return err
	}
	_, err = c.wagers.ProcessInboxMessage(ctx, c.consumerName, messageID, hash, in)
	return err
}

func envelopeToInput(env wagerEnvelope) (application.SubmitWagerInput, error) {
	playerID, err := uuid.Parse(env.Data.PlayerID)
	if err != nil {
		return application.SubmitWagerInput{}, err
	}
	walletID, err := uuid.Parse(env.Data.WalletID)
	if err != nil {
		return application.SubmitWagerInput{}, err
	}
	money, err := domain.ParseMoney(env.Data.Money.Amount, env.Data.Money.Currency)
	if err != nil {
		return application.SubmitWagerInput{}, err
	}
	fields := domain.WagerPayloadFields{
		ProviderID:                    env.Data.ProviderID,
		ExternalTransactionID:         env.Data.ExternalTransactionID,
		PlayerID:                      env.Data.PlayerID,
		WalletID:                      env.Data.WalletID,
		RoundID:                       env.Data.RoundID,
		GameID:                        env.Data.GameID,
		Kind:                          env.Data.Kind,
		MoneyAmount:                   env.Data.Money.Amount,
		MoneyCurrency:                 env.Data.Money.Currency,
		ReferenceExternalTransactionID: env.Data.ReferenceExternalTransactionID,
	}
	return application.SubmitWagerInput{
		ProviderID:                    env.Data.ProviderID,
		ExternalTransactionID:         env.Data.ExternalTransactionID,
		IdempotencyKey:                env.Data.IdempotencyKey,
		PlayerID:                      playerID,
		WalletID:                      walletID,
		RoundID:                       env.Data.RoundID,
		GameID:                        env.Data.GameID,
		Kind:                          domain.TransactionKind(env.Data.Kind),
		Money:                         money,
		ReferenceExternalTransactionID: env.Data.ReferenceExternalTransactionID,
		PayloadHash:                   domain.HashWagerPayload(fields),
	}, nil
}

func payloadHash(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func (c *WagerConsumer) Stop() {
	close(c.stop)
	<-c.done
}

type OutboxPublisher struct {
	client    *sqs.Client
	queueURL  string
	repos     outboxRepo
	workerID  string
	lease     time.Duration
	interval  time.Duration
	log       *slog.Logger
	met       *observe.Metrics
	stop      chan struct{}
	done      chan struct{}
}

type outboxRepo interface {
	ClaimPending(ctx context.Context, workerID string, lease time.Duration, limit int) ([]domain.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID uuid.UUID) error
	ScheduleRetry(ctx context.Context, eventID uuid.UUID, next time.Time) error
	CountPending(ctx context.Context) (int64, error)
}

func NewOutboxPublisher(cfg config.Config, sqsClient *SQSClient, repo outboxRepo, met *observe.Metrics, log *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{
		client:   sqsClient.client,
		queueURL: cfg.SQSOutboxQueueURL,
		repos:    repo,
		workerID: uuid.Must(uuid.NewV7()).String(),
		lease:    cfg.OutboxLease,
		interval: cfg.OutboxPollInterval,
		log:      log,
		met:      met,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (p *OutboxPublisher) Start(ctx context.Context) {
	if p.queueURL == "" {
		close(p.done)
		return
	}
	go p.loop(ctx)
}

func (p *OutboxPublisher) loop(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.publishBatch(ctx)
		}
	}
}

func (p *OutboxPublisher) publishBatch(ctx context.Context) {
	events, err := p.repos.ClaimPending(ctx, p.workerID, p.lease, 20)
	if err != nil {
		p.log.Error("outbox claim failed", "error", err)
		return
	}
	n, _ := p.repos.CountPending(ctx)
	p.met.OutboxLag.Set(float64(n))
	for _, ev := range events {
		body, _ := json.Marshal(domain.EventEnvelope{
			EventID:       ev.EventID,
			EventType:     ev.EventType,
			AggregateID:   ev.AggregateID,
			CorrelationID: ev.CorrelationID,
			CausationID:   ev.CausationID,
			OccurredAt:    ev.OccurredAt.UTC().Format(time.RFC3339Nano),
			Version:       ev.Version,
			Data:          ev.Payload,
		})
		_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(p.queueURL),
			MessageBody:            aws.String(string(body)),
			MessageGroupId:         aws.String(ev.AggregateID.String()),
			MessageDeduplicationId: aws.String(ev.EventID.String()),
		})
		if err != nil {
			_ = p.repos.ScheduleRetry(ctx, ev.EventID, time.Now().UTC().Add(5*time.Second))
			continue
		}
		_ = p.repos.MarkPublished(ctx, ev.EventID)
	}
}

func (p *OutboxPublisher) Stop() {
	close(p.stop)
	<-p.done
}

type ReferenceWorker struct {
	wagers   *application.WagerService
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
}

func NewReferenceWorker(wagers *application.WagerService, interval time.Duration) *ReferenceWorker {
	return &ReferenceWorker{
		wagers:   wagers,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (w *ReferenceWorker) Start(ctx context.Context) {
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-ticker.C:
				_ = w.wagers.ResumePendingReferences(ctx, 20)
			}
		}
	}()
}

func (w *ReferenceWorker) Stop() {
	close(w.stop)
	<-w.done
}

func RegisterLifecycle(lc fx.Lifecycle, consumer *WagerConsumer, outbox *OutboxPublisher, ref *ReferenceWorker, shutdown config.Config) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			consumer.Start(ctx)
			outbox.Start(ctx)
			ref.Start(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			stopCtx, cancel := context.WithTimeout(ctx, shutdown.ShutdownTimeout)
			defer cancel()
			consumer.Stop()
			outbox.Stop()
			ref.Stop()
			_ = stopCtx
			return nil
		},
	})
}
