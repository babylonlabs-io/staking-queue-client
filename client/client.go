package client

import "context"

type QueueMessage struct {
	Body          string
	Receipt       string
	RetryAttempts int32
}

func (m QueueMessage) IncrementRetryAttempts() int32 {
	m.RetryAttempts++
	return m.RetryAttempts
}

func (m QueueMessage) GetRetryAttempts() int32 {
	return m.RetryAttempts
}

// A common interface for queue clients regardless if it's a SQS, RabbitMQ, etc.
type QueueClient interface {
	SendMessage(ctx context.Context, messageBody string) error
	ReceiveMessages() (<-chan QueueMessage, error)
	DeleteMessage(receipt string) error
	Stop() error
	GetQueueName() string
	ReQueueMessage(ctx context.Context, message QueueMessage) error
}

func NewQueueClient(queueURL, user, pass, queueName string) (QueueClient, error) {
	return NewRabbitMqClient(queueURL, user, pass, queueName)
}
