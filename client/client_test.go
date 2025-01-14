package client_test

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/babylonlabs-io/staking-queue-client/client"
)

func TestIncrementRetryAttempts(t *testing.T) {
	msg := client.QueueMessage{}
	msg.IncrementRetryAttempts()
	assert.Equal(t, int32(1), msg.GetRetryAttempts())
}
