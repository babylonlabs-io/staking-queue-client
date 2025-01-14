package client

const (
	ActiveStakingQueueName       string = "v2_active_staking_queue"
	UnbondingStakingQueueName    string = "v2_unbonding_staking_queue"
	WithdrawableStakingQueueName string = "v2_withdrawable_staking_queue"
	WithdrawnStakingQueueName    string = "v2_withdrawn_staking_queue"
)

const (
	ActiveStakingEventType       EventType = 1
	UnbondingStakingEventType    EventType = 2
	WithdrawableStakingEventType EventType = 3
	WithdrawnStakingEventType    EventType = 4
)

// Event schema versions, only increment when the schema changes
const (
	ActiveStakingEventVersion       int = 0
	UnbondingStakingEventVersion    int = 0
	WithdrawableStakingEventVersion int = 0
	WithdrawnStakingEventVersion    int = 0
)

type EventType int

type EventMessage interface {
	GetEventType() EventType
	GetStakingTxHashHex() string
}

type StakingEvent struct {
	SchemaVersion             int       `json:"schema_version"`
	EventType                 EventType `json:"event_type"`
	StakingTxHashHex          string    `json:"staking_tx_hash_hex"`
	StakerBtcPkHex            string    `json:"staker_btc_pk_hex"`
	FinalityProviderBtcPksHex []string  `json:"finality_provider_btc_pks_hex"`
	StakingAmount             uint64    `json:"staking_amount"`
	StateHistory              []string  `json:"state_history"`
}

func (e StakingEvent) GetEventType() EventType {
	return e.EventType
}

func (e StakingEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewActiveStakingEvent(
	stakingTxHashHex string,
	stakerBtcPkHex string,
	finalityProviderBtcPksHex []string,
	stakingAmount uint64,
	stateHistory []string,
) StakingEvent {
	return StakingEvent{
		SchemaVersion:             ActiveStakingEventVersion,
		EventType:                 ActiveStakingEventType,
		StakingTxHashHex:          stakingTxHashHex,
		StakerBtcPkHex:            stakerBtcPkHex,
		FinalityProviderBtcPksHex: finalityProviderBtcPksHex,
		StakingAmount:             stakingAmount,
		StateHistory:              stateHistory,
	}
}

func NewUnbondingStakingEvent(
	stakingTxHashHex string,
	stakerBtcPkHex string,
	finalityProviderBtcPksHex []string,
	stakingAmount uint64,
	stateHistory []string,
) StakingEvent {
	return StakingEvent{
		SchemaVersion:             UnbondingStakingEventVersion,
		EventType:                 UnbondingStakingEventType,
		StakingTxHashHex:          stakingTxHashHex,
		StakerBtcPkHex:            stakerBtcPkHex,
		FinalityProviderBtcPksHex: finalityProviderBtcPksHex,
		StakingAmount:             stakingAmount,
		StateHistory:              stateHistory,
	}
}

func NewWithdrawableStakingEvent(
	stakingTxHashHex string,
	stakerBtcPkHex string,
	finalityProviderBtcPksHex []string,
	stakingAmount uint64,
	stateHistory []string,
) StakingEvent {
	return StakingEvent{
		SchemaVersion:             WithdrawableStakingEventVersion,
		EventType:                 WithdrawableStakingEventType,
		StakingTxHashHex:          stakingTxHashHex,
		StakerBtcPkHex:            stakerBtcPkHex,
		FinalityProviderBtcPksHex: finalityProviderBtcPksHex,
		StakingAmount:             stakingAmount,
		StateHistory:              stateHistory,
	}
}

func NewWithdrawnStakingEvent(
	stakingTxHashHex string,
	stakerBtcPkHex string,
	finalityProviderBtcPksHex []string,
	stakingAmount uint64,
	stateHistory []string,
) StakingEvent {
	return StakingEvent{
		SchemaVersion:             WithdrawnStakingEventVersion,
		EventType:                 WithdrawnStakingEventType,
		StakingTxHashHex:          stakingTxHashHex,
		StakerBtcPkHex:            stakerBtcPkHex,
		FinalityProviderBtcPksHex: finalityProviderBtcPksHex,
		StakingAmount:             stakingAmount,
		StateHistory:              stateHistory,
	}
}
