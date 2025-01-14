package queuemngr

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/babylonlabs-io/staking-queue-client/client"
	"github.com/babylonlabs-io/staking-queue-client/config"
)

const timeout = 5 * time.Second

type QueueManager struct {
	ActiveStakingQueue       client.QueueClient
	UnbondingStakingQueue    client.QueueClient
	WithdrawableStakingQueue client.QueueClient
	WithdrawnStakingQueue    client.QueueClient
	SlashedStakingQueue      client.QueueClient
	logger                   *zap.Logger
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

	withdrawableStakingQueue, err := client.NewQueueClient(cfg, client.WithdrawableStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create withdrawable staking queue: %w", err)
	}

	withdrawnStakingQueue, err := client.NewQueueClient(cfg, client.WithdrawnStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create withdrawn staking queue: %w", err)
	}

	slashedStakingQueue, err := client.NewQueueClient(cfg, client.SlashedStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create slashed staking queue: %w", err)
	}

	return &QueueManager{
		ActiveStakingQueue:       activeStakingQueue,
		UnbondingStakingQueue:    unbondingStakingQueue,
		WithdrawableStakingQueue: withdrawableStakingQueue,
		WithdrawnStakingQueue:    withdrawnStakingQueue,
		SlashedStakingQueue:      slashedStakingQueue,
		logger:                   logger.With(zap.String("module", "queue consumer")),
	}, nil
}

func (qc *QueueManager) Start() error {
	return nil
}

func PushEvent[T any](queueClient client.QueueClient, ev T) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	err = queueClient.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push event: %w", err)
	}

	return nil
}

func (qc *QueueManager) PushActiveStakingEvent(ev *client.StakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing active staking event", zap.String("tx_hash", ev.StakingTxHashHex))
	err = qc.ActiveStakingQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push staking event: %w", err)
	}
	qc.logger.Info("successfully pushed active staking event", zap.String("tx_hash", ev.StakingTxHashHex))

	return nil
}

func (qc *QueueManager) PushUnbondingStakingEvent(ev *client.StakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing unbonding staking event", zap.String("staking_tx_hash", ev.StakingTxHashHex))
	err = qc.UnbondingStakingQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push unbonding staking event: %w", err)
	}
	qc.logger.Info("successfully pushed unbonding staking event", zap.String("staking_tx_hash", ev.StakingTxHashHex))

	return nil
}

func (qc *QueueManager) PushWithdrawableStakingEvent(ev *client.StakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing withdrawable staking event", zap.String("staking_tx_hash", ev.StakingTxHashHex))
	err = qc.WithdrawableStakingQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push withdrawable staking event: %w", err)
	}
	qc.logger.Info("successfully pushed withdrawable staking event", zap.String("staking_tx_hash", ev.StakingTxHashHex))

	return nil
}

func (qc *QueueManager) PushWithdrawnStakingEvent(ev *client.StakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing withdrawn staking event", zap.String("staking_tx_hash", ev.StakingTxHashHex))
	err = qc.WithdrawnStakingQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push withdrawn staking event: %w", err)
	}
	qc.logger.Info("successfully pushed withdrawn staking event", zap.String("staking_tx_hash", ev.StakingTxHashHex))

	return nil
}

func (qc *QueueManager) PushSlashedStakingEvent(ev *client.StakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing slashed staking event", zap.String("staking_tx_hash", ev.StakingTxHashHex))
	err = qc.SlashedStakingQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push slashed staking event: %w", err)
	}
	qc.logger.Info("successfully pushed slashed staking event", zap.String("staking_tx_hash", ev.StakingTxHashHex))

	return nil
}

// requeue message
func (qc *QueueManager) ReQueueMessage(ctx context.Context, message client.QueueMessage, queueName string) error {
	switch queueName {
	case client.ActiveStakingQueueName:
		return qc.ActiveStakingQueue.ReQueueMessage(ctx, message)
	case client.UnbondingStakingQueueName:
		return qc.UnbondingStakingQueue.ReQueueMessage(ctx, message)
	case client.WithdrawableStakingQueueName:
		return qc.WithdrawableStakingQueue.ReQueueMessage(ctx, message)
	case client.WithdrawnStakingQueueName:
		return qc.WithdrawnStakingQueue.ReQueueMessage(ctx, message)
	case client.SlashedStakingQueueName:
		return qc.SlashedStakingQueue.ReQueueMessage(ctx, message)
	default:
		return fmt.Errorf("unknown queue name: %s", queueName)
	}
}

func (qc *QueueManager) Stop() error {
	if err := qc.ActiveStakingQueue.Stop(); err != nil {
		return err
	}

	if err := qc.UnbondingStakingQueue.Stop(); err != nil {
		return err
	}

	if err := qc.WithdrawableStakingQueue.Stop(); err != nil {
		return err
	}

	if err := qc.WithdrawnStakingQueue.Stop(); err != nil {
		return err
	}

	if err := qc.SlashedStakingQueue.Stop(); err != nil {
		return err
	}

	return nil
}

// Ping checks the health of the RabbitMQ infrastructure.
func (qc *QueueManager) Ping() error {
	queues := []client.QueueClient{
		qc.ActiveStakingQueue,
		qc.UnbondingStakingQueue,
		qc.WithdrawableStakingQueue,
	}

	for _, queue := range queues {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		err := queue.Ping(ctx)
		if err != nil {
			qc.logger.Error("ping failed", zap.String("queue", queue.GetQueueName()), zap.Error(err))
			return err
		}
		qc.logger.Info("ping successful", zap.String("queue", queue.GetQueueName()))
	}
	return nil
}
