package queuemngr

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"errors"
	"github.com/babylonlabs-io/staking-queue-client/client"
	"github.com/babylonlabs-io/staking-queue-client/config"
)

const timeout = 5 * time.Second

type QueueManager struct {
	ActiveStakingQueue    client.QueueClient
	UnbondingStakingQueue client.QueueClient
	logger                *zap.Logger
}

func NewQueueManager(cfg *config.QueueConfig, logger *zap.Logger) (*QueueManager, error) {
	activeStakingQueue, err := client.NewQueueClient(cfg, client.ActiveStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create active staking queue: %w", err)
	}

	unbondingStakingQueue, err := client.NewQueueClient(cfg, client.UnbondingStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create unbonding staking queue: %w", err)
	}

	return &QueueManager{
		ActiveStakingQueue:    activeStakingQueue,
		UnbondingStakingQueue: unbondingStakingQueue,
		logger:                logger.With(zap.String("module", "queue consumer")),
	}, nil
}

func (qc *QueueManager) Start() error {
	return nil
}

func pushEvent[T any](ctx context.Context, queueClient client.QueueClient, ev T) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	err = queueClient.SendMessage(ctx, messageBody)
	if err != nil {
		return fmt.Errorf("failed to push event: %w", err)
	}

	return nil
}

func (qc *QueueManager) PushActiveStakingEvent(ctx context.Context, ev *client.StakingEvent) error {
	qc.logger.Debug("pushing active staking event", zap.String("tx_hash", ev.StakingTxHashHex))

	err := pushEvent(ctx, qc.ActiveStakingQueue, ev)
	if err != nil {
		return fmt.Errorf("failed to push staking event: %w", err)
	}

	qc.logger.Debug("successfully pushed active staking event", zap.String("tx_hash", ev.StakingTxHashHex))
	return nil
}

func (qc *QueueManager) PushUnbondingStakingEvent(ctx context.Context, ev *client.StakingEvent) error {
	qc.logger.Debug("pushing unbonding staking event", zap.String("tx_hash", ev.StakingTxHashHex))

	err := pushEvent(ctx, qc.UnbondingStakingQueue, ev)
	if err != nil {
		return fmt.Errorf("failed to push staking event: %w", err)
	}

	qc.logger.Debug("successfully pushed unbonding staking event", zap.String("tx_hash", ev.StakingTxHashHex))
	return nil
}

// requeue message
func (qc *QueueManager) ReQueueMessage(ctx context.Context, message client.QueueMessage, queueName string) error {
	switch queueName {
	case client.ActiveStakingQueueName:
		return qc.ActiveStakingQueue.ReQueueMessage(ctx, message)
	case client.UnbondingStakingQueueName:
		return qc.UnbondingStakingQueue.ReQueueMessage(ctx, message)
	default:
		return fmt.Errorf("unknown queue name: %s", queueName)
	}
}

func (qc *QueueManager) Stop() error {
	var activeErr, unbondingErr error

	activeErr = qc.ActiveStakingQueue.Stop()
	unbondingErr = qc.UnbondingStakingQueue.Stop()

	return errors.Join(activeErr, unbondingErr)
}

// Ping checks the health of the RabbitMQ infrastructure.
func (qc *QueueManager) Ping() error {
	queues := []client.QueueClient{
		qc.ActiveStakingQueue,
		qc.UnbondingStakingQueue,
	}

	for _, queue := range queues {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		err := queue.Ping(ctx)
		cancel()
		if err != nil {
			qc.logger.Error("ping failed", zap.String("queue", queue.GetQueueName()), zap.Error(err))
			return err
		}
		qc.logger.Info("ping successful", zap.String("queue", queue.GetQueueName()))
	}
	return nil
}
