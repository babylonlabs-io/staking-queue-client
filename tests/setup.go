package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/babylonlabs-io/staking-queue-client/client"
	"github.com/babylonlabs-io/staking-queue-client/config"
	"github.com/babylonlabs-io/staking-queue-client/queuemngr"
)

type TestServer struct {
	QueueManager *queuemngr.QueueManager
	Conn         *amqp091.Connection
}

func (ts *TestServer) Stop(t *testing.T) {
	if err := ts.QueueManager.Stop(); err != nil && !isConnectionClosedError(err) {
		t.Errorf("failed to stop QueueManager: %v", err)
	}

	if err := ts.Conn.Close(); err != nil && !isConnectionClosedError(err) {
		t.Errorf("failed to close connection: %v", err)
	}
}

func setupTestQueueConsumer(t *testing.T, cfg *config.QueueConfig) *TestServer {
	amqpURI := fmt.Sprintf("amqp://%s:%s@%s", cfg.QueueUser, cfg.QueuePassword, cfg.Url)
	conn, err := amqp091.Dial(amqpURI)
	require.NoError(t, err)
	err = purgeQueues(conn, []string{
		client.ActiveStakingQueueName,
		client.UnbondingStakingQueueName,
		client.WithdrawStakingQueueName,
		client.ExpiredStakingQueueName,
		client.StakingStatsQueueName,
		client.BtcInfoQueueName,
		client.ConfirmedInfoQueueName,
		// purge delay queues too
		client.ActiveStakingQueueName + "_delay",
		client.UnbondingStakingQueueName + "_delay",
		client.WithdrawStakingQueueName + "_delay",
		client.ExpiredStakingQueueName + "_delay",
		client.StakingStatsQueueName + "_delay",
		client.BtcInfoQueueName + "_delay",
		client.ConfirmedInfoQueueName + "_delay",
	})
	require.NoError(t, err)

	// Start the actual queue processing in our codebase
	queues, err := queuemngr.NewQueueManager(cfg, zap.NewNop())
	require.NoError(t, err)

	return &TestServer{
		QueueManager: queues,
		Conn:         conn,
	}
}

// purgeQueues purges all messages from the given list of queues.
func purgeQueues(conn *amqp091.Connection, queues []string) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel in test: %w", err)
	}
	defer ch.Close()

	for _, queue := range queues {
		_, err := ch.QueuePurge(queue, false)
		if err != nil {
			if strings.Contains(err.Error(), "NOT_FOUND") || strings.Contains(err.Error(), "channel/connection is not open") {
				continue
			}
			return fmt.Errorf("failed to purge queue in test %s: %w", queue, err)
		}
	}

	return nil
}

func buildActiveNStakingEvents(stakerHash string, numOfEvent int) []*client.ActiveStakingEvent {
	var activeStakingEvents []*client.ActiveStakingEvent
	for i := 0; i < numOfEvent; i++ {
		activeStakingEvent := client.NewActiveStakingEvent(
			"0x1234567890abcdef"+fmt.Sprint(i),
			stakerHash,
			"0xabcdef1234567890"+fmt.Sprint(i),
			1+uint64(i),
			100+uint64(i),
			time.Now().Unix(),
			200+uint64(i),
			1+uint64(i),
			"0xabcdef1234567890"+fmt.Sprint(i),
			false,
		)

		activeStakingEvents = append(activeStakingEvents, &activeStakingEvent)
	}
	return activeStakingEvents
}

func buildNUnbondingEvents(numOfEvent int) []*client.UnbondingStakingEvent {
	var unbondingEvents []*client.UnbondingStakingEvent
	for i := 0; i < numOfEvent; i++ {
		unbondingEv := client.NewUnbondingStakingEvent(
			"0x1234567890abcdef"+fmt.Sprint(i),
			uint64(i),
			time.Now().Unix(),
			200+uint64(i),
			uint64(0),
			"0xabcdef1234567890"+fmt.Sprint(i),
			"0x1234567890abcdef"+fmt.Sprint(i),
		)
		unbondingEvents = append(unbondingEvents, &unbondingEv)
	}

	return unbondingEvents
}

func buildNWithdrawEvents(numOfEvent int) []*client.WithdrawStakingEvent {
	var withdrawEvents []*client.WithdrawStakingEvent
	for i := 0; i < numOfEvent; i++ {
		withdrawEv := client.NewWithdrawStakingEvent(
			"0x1234567890abcdef"+fmt.Sprint(i),
			"0xabcdef1234567890"+fmt.Sprint(i),
			100+uint64(i),
			"0xghijkl1234567890"+fmt.Sprint(i),
		)
		withdrawEvents = append(withdrawEvents, &withdrawEv)
	}

	return withdrawEvents
}

func buildNExpiryEvents(numOfEvent int) []*client.ExpiredStakingEvent {
	var expiryEvents []*client.ExpiredStakingEvent
	for i := 0; i < numOfEvent; i++ {
		expiryEv := client.NewExpiredStakingEvent(
			"0x1234567890abcdef"+fmt.Sprint(i),
			"active",
		)

		expiryEvents = append(expiryEvents, &expiryEv)
	}

	return expiryEvents
}

func buildNStatsEvents(stakerHash string, numOfEvent int) []*client.StatsEvent {
	var statsEvents []*client.StatsEvent
	for i := 0; i < numOfEvent; i++ {
		activeStakingEvent := client.NewStatsEvent(
			"0x1234567890abcdef"+fmt.Sprint(i),
			stakerHash,
			"0xabcdef1234567890"+fmt.Sprint(i),
			1+uint64(i),
			"active",
			false,
		)

		statsEvents = append(statsEvents, &activeStakingEvent)
	}
	return statsEvents
}

func buildNBtcInfoEvents(numOfEvent int) []*client.BtcInfoEvent {
	var btcInfoEvents []*client.BtcInfoEvent
	for i := 0; i < numOfEvent; i++ {
		btcInfoEv := client.NewBtcInfoEvent(
			100+uint64(i),
			10000+uint64(i)*1000,
			10000+uint64(i)*1000,
		)

		btcInfoEvents = append(btcInfoEvents, &btcInfoEv)
	}

	return btcInfoEvents
}

func buildNConfirmedInfoEvents(numOfEvent int) []*client.ConfirmedInfoEvent {
	var confirmedInfoEvents []*client.ConfirmedInfoEvent
	for i := 0; i < numOfEvent; i++ {
		confirmedInfoEv := client.NewConfirmedInfoEvent(
			100+uint64(i),
			10000+uint64(i)*1000,
		)

		confirmedInfoEvents = append(confirmedInfoEvents, &confirmedInfoEv)
	}

	return confirmedInfoEvents
}

// inspectQueueMessageCount inspects the number of messages in the given queue.
func inspectQueueMessageCount(t *testing.T, conn *amqp091.Connection, queueName string) (int, error) {
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open a channel in test: %v", err)
	}

	q, err := ch.QueueDeclarePassive(queueName, false, false, false, false, nil)
	if err != nil {
		if strings.Contains(err.Error(), "NOT_FOUND") || strings.Contains(err.Error(), "channel/connection is not open") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to inspect queue in test %s: %w", queueName, err)
	}
	return q.Messages, nil
}

// Helper function to check if an error is related to a closed connection
func isConnectionClosedError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "connection is not open") || strings.Contains(err.Error(), "channel/connection is not open"))
}
