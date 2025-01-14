package client_test

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/babylonlabs-io/staking-queue-client/client"
)

func TestIncrementRetryAttempts(t *testing.T) {
	const expected int32 = 1

	msg := client.QueueMessage{}
	assert.Equal(t, expected, msg.IncrementRetryAttempts())
	assert.Equal(t, expected, msg.GetRetryAttempts())
}
