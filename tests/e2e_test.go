package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/babylonlabs-io/staking-queue-client/client"
	"github.com/babylonlabs-io/staking-queue-client/config"
	"github.com/babylonlabs-io/staking-queue-client/queuemngr"
)

const (
	mockStakerHash = "0x1234567890abcdef"
)

func TestClientPing(t *testing.T) {
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	// Define a timeout for the context
	timeout := 5 * time.Second

	// Test successful ping with context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := queueManager.StakingQueue.Ping(ctx)
	require.NoError(t, err, "Ping should not return an error")

	// Simulate a closed connection scenario
	err = queueManager.StakingQueue.Stop()
	require.NoError(t, err, "Stop should not return an error")

	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = queueManager.StakingQueue.Ping(ctx)
	require.Error(t, err, "Ping should return an error when connection is closed")
	require.Contains(t, err.Error(), "rabbitMQ connection is closed", "Error message should indicate the connection is closed")
}

func TestPing(t *testing.T) {
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	// Test successful ping for all queues
	err := queueManager.Ping()
	require.NoError(t, err, "Ping should not return an error")

	// Simulate a closed connection scenario for StakingQueue
	err = queueManager.StakingQueue.Stop()
	require.NoError(t, err, "Stop should not return an error")
	err = queueManager.Ping()
	require.Error(t, err, "Ping should return an error when any queue connection is closed")
	require.Contains(t, err.Error(), "rabbitMQ connection is closed", "Error message should indicate which queue failed")
}

func TestSchemaVersionBackwardsCompatibility(t *testing.T) {
	type oldActiveStakingEvent struct {
		EventType             client.EventType `json:"event_type"`
		StakingTxHashHex      string           `json:"staking_tx_hash_hex"`
		StakerPkHex           string           `json:"staker_pk_hex"`
		FinalityProviderPkHex string           `json:"finality_provider_pk_hex"`
		StakingValue          uint64           `json:"staking_value"`
		StakingStartHeight    uint64           `json:"staking_start_height"`
		StakingStartTimestamp int64            `json:"staking_start_timestamp"`
		StakingTimeLock       uint64           `json:"staking_timelock"`
		StakingOutputIndex    uint64           `json:"staking_output_index"`
		StakingTxHex          string           `json:"staking_tx_hex"`
		IsOverflow            bool             `json:"is_overflow"`
	}

	event := &oldActiveStakingEvent{
		EventType:             client.ActiveStakingEventType,
		StakingTxHashHex:      "0x1234567890abcdef",
		StakerPkHex:           "0x1234567890abcdef",
		FinalityProviderPkHex: "0x1234567890abcdef",
		StakingValue:          100,
		StakingStartHeight:    100,
		StakingStartTimestamp: 100,
		StakingTimeLock:       100,
		StakingOutputIndex:    100,
		StakingTxHex:          "0x1234567890abcdef",
		IsOverflow:            false,
	}
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager
	stakingEventReceivedChan, err := queueManager.StakingQueue.ReceiveMessages()
	require.NoError(t, err)

	err = queuemngr.PushEvent(queueManager.StakingQueue, event)
	require.NoError(t, err)
	receivedEv := <-stakingEventReceivedChan
	var stakingEv client.StakingEvent
	err = json.Unmarshal([]byte(receivedEv.Body), &stakingEv)
	require.NoError(t, err)
	require.Equal(t, event.EventType, stakingEv.GetEventType())
	require.Equal(t, event.StakingTxHashHex, stakingEv.GetStakingTxHashHex())
	require.Equal(t, 0, stakingEv.SchemaVersion)
}

func TestStakingEvent(t *testing.T) {
	numStakingEvents := 3
	activeStakingEvents := buildActiveNStakingEvents(mockStakerHash, numStakingEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	stakingEventReceivedChan, err := queueManager.StakingQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range activeStakingEvents {
		err = queueManager.PushStakingEvent(ev)
		require.NoError(t, err)

		receivedEv := <-stakingEventReceivedChan
		var stakingEv client.StakingEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &stakingEv)
		require.NoError(t, err)
		require.Equal(t, ev, &stakingEv)
		require.Equal(t, 0, stakingEv.SchemaVersion)
	}
}

func TestUnbondingEvent(t *testing.T) {
	numUnbondingEvents := 3
	unbondingEvents := buildNUnbondingEvents(numUnbondingEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	unbondingEvReceivedChan, err := queueManager.UnbondingQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range unbondingEvents {
		err = queueManager.PushUnbondingEvent(ev)
		require.NoError(t, err)

		receivedEv := <-unbondingEvReceivedChan
		var unbondingEv client.StakingEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &unbondingEv)
		require.NoError(t, err)
		require.Equal(t, ev, &unbondingEv)
		require.Equal(t, 0, unbondingEv.SchemaVersion)
	}
}

func TestWithdrawEvent(t *testing.T) {
	numWithdrawEvents := 3
	withdrawEvents := buildNWithdrawEvents(numWithdrawEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	withdrawEventsReceivedChan, err := queueManager.WithdrawQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range withdrawEvents {
		err = queueManager.PushWithdrawEvent(ev)
		require.NoError(t, err)

		receivedEv := <-withdrawEventsReceivedChan
		var withdrawEv client.WithdrawStakingEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &withdrawEv)
		require.NoError(t, err)
		require.Equal(t, ev, &withdrawEv)
		require.Equal(t, 1, withdrawEv.SchemaVersion)
		require.NotZero(t, withdrawEv.WithdrawTxBtcHeight)
		require.NotEmpty(t, withdrawEv.WithdrawTxHashHex)
		require.NotEmpty(t, withdrawEv.WithdrawTxHex)
	}
}

func TestStatsEvent(t *testing.T) {
	numEvents := 3
	statsEvents := buildNStatsEvents(mockStakerHash, numEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	eventReceivedChan, err := queueManager.StatsQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range statsEvents {
		err = queuemngr.PushEvent(queueManager.StatsQueue, ev)
		require.NoError(t, err)

		receivedEv := <-eventReceivedChan
		var statsEv client.StatsEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &statsEv)
		require.NoError(t, err)
		require.Equal(t, ev, &statsEv)
		require.Equal(t, 1, statsEv.SchemaVersion)
	}
}

func TestExpiryEvent(t *testing.T) {
	numExpiryEvents := 3
	expiryEvents := buildNExpiryEvents(numExpiryEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	expiryEventsReceivedChan, err := queueManager.ExpiryQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range expiryEvents {
		err = queueManager.PushExpiryEvent(ev)
		require.NoError(t, err)

		receivedEv := <-expiryEventsReceivedChan
		var expiryEvent client.ExpiredStakingEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &expiryEvent)
		require.NoError(t, err)
		require.Equal(t, ev, &expiryEvent)
		require.Equal(t, 0, expiryEvent.SchemaVersion)
	}
}

func TestBtcInfoEvent(t *testing.T) {
	numBtcInfoEvents := 3
	BtcInfoEvents := buildNBtcInfoEvents(numBtcInfoEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	btcInfoEventsReceivedChan, err := queueManager.BtcInfoQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range BtcInfoEvents {
		err = queueManager.PushBtcInfoEvent(ev)
		require.NoError(t, err)

		receivedEv := <-btcInfoEventsReceivedChan
		var BtcInfoEvent client.BtcInfoEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &BtcInfoEvent)
		require.NoError(t, err)
		require.Equal(t, ev, &BtcInfoEvent)
		require.Equal(t, 0, BtcInfoEvent.SchemaVersion)
	}
}

func TestConfirmedInfoEvent(t *testing.T) {
	numConfirmedInfoEvents := 3
	confirmedInfoEvents := buildNConfirmedInfoEvents(numConfirmedInfoEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	confirmedInfoEventsReceivedChan, err := queueManager.ConfirmedInfoQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range confirmedInfoEvents {
		err = queueManager.PushConfirmedInfoEvent(ev)
		require.NoError(t, err)

		receivedEv := <-confirmedInfoEventsReceivedChan
		var confirmedInfoEvent client.ConfirmedInfoEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &confirmedInfoEvent)
		require.NoError(t, err)
		require.Equal(t, ev, &confirmedInfoEvent)
		require.Equal(t, 0, confirmedInfoEvent.SchemaVersion)
	}
}

func TestReQueueEvent(t *testing.T) {
	activeStakingEvents := buildActiveNStakingEvents(mockStakerHash, 1)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	stakingEventReceivedChan, err := queueManager.StakingQueue.ReceiveMessages()
	require.NoError(t, err)

	ev := activeStakingEvents[0]
	err = queueManager.PushStakingEvent(ev)
	require.NoError(t, err)

	var receivedEv client.QueueMessage

	select {
	case receivedEv = <-stakingEventReceivedChan:
	case <-time.After(10 * time.Second): // Wait up to 10 seconds for a message
		t.Fatal("timeout waiting for staking event")
	}

	var stakingEv client.StakingEvent
	err = json.Unmarshal([]byte(receivedEv.Body), &stakingEv)
	require.NoError(t, err)
	require.Equal(t, ev, &stakingEv)
	require.Equal(t, int32(0), receivedEv.RetryAttempts)

	// Now let's requeue the event
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err = queueManager.StakingQueue.ReQueueMessage(ctx, receivedEv)
	require.NoError(t, err)
	time.Sleep(1 * time.Second) // Wait to ensure message has time to move to delayed queue

	// Check that the main queue is empty
	count, err := inspectQueueMessageCount(t, testServer.Conn, client.ActiveStakingQueueName)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	// Make sure it appears in the delayed queue
	delayedQueueCount, err := inspectQueueMessageCount(t, testServer.Conn, client.ActiveStakingQueueName+"_delay")
	require.NoError(t, err)
	require.Equal(t, 1, delayedQueueCount)

	// Checking delayed queue message appearance
	select {
	case requeuedEvent := <-stakingEventReceivedChan:
		require.Nil(t, requeuedEvent, "Event should not be available immediately in the main queue")
	case <-time.After(3 * time.Second): // Wait longer than the delay to ensure the message moves back
	}

	// Now let's wait for the requeued event
	time.Sleep(2 * time.Second) // Wait additional time for delayed message to return
	requeuedEvent := <-stakingEventReceivedChan
	require.NotNil(t, requeuedEvent)
	require.Equal(t, int32(1), requeuedEvent.RetryAttempts)
}
