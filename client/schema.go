package client

const (
	ActiveStakingQueueName    string = "active_staking_queue"
	UnbondingStakingQueueName string = "unbonding_staking_queue"
	WithdrawStakingQueueName  string = "withdraw_staking_queue"
	ExpiredStakingQueueName   string = "expired_staking_queue"
	StakingStatsQueueName     string = "staking_stats_queue"
	BtcInfoQueueName          string = "btc_info_queue"
	ConfirmedInfoQueueName    string = "confirmed_info_queue"
)

const (
	ActiveStakingEventType    EventType = 1
	UnbondingStakingEventType EventType = 2
	WithdrawStakingEventType  EventType = 3
	ExpiredStakingEventType   EventType = 4
	StatsEventType            EventType = 5
	BtcInfoEventType          EventType = 6
	ConfirmedInfoEventType    EventType = 7
)

// Event schema versions, only increment when the schema changes
const (
	ActiveEventVersion        int = 0
	UnbondingEventVersion     int = 0
	WithdrawEventVersion      int = 1
	ExpiredEventVersion       int = 0
	StatsEventVersion         int = 1
	BtcInfoEventVersion       int = 0
	ConfirmedInfoEventVersion int = 0
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
}

func NewActiveStakingEvent(
	stakingTxHashHex string,
	stakerBtcPkHex string,
	finalityProviderBtcPksHex []string,
	stakingAmount uint64,
) StakingEvent {
	return StakingEvent{
		SchemaVersion:             ActiveEventVersion,
		EventType:                 ActiveStakingEventType,
		StakingTxHashHex:          stakingTxHashHex,
		StakerBtcPkHex:            stakerBtcPkHex,
		FinalityProviderBtcPksHex: finalityProviderBtcPksHex,
		StakingAmount:             stakingAmount,
	}
}

func (e StakingEvent) GetEventType() EventType {
	return ActiveStakingEventType
}

func (e StakingEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewUnbondingStakingEvent(
	stakingTxHashHex string,
	stakerBtcPkHex string,
	finalityProviderBtcPksHex []string,
	stakingAmount uint64,
) StakingEvent {
	return StakingEvent{
		SchemaVersion:             UnbondingEventVersion,
		EventType:                 UnbondingStakingEventType,
		StakingTxHashHex:          stakingTxHashHex,
		StakerBtcPkHex:            stakerBtcPkHex,
		FinalityProviderBtcPksHex: finalityProviderBtcPksHex,
		StakingAmount:             stakingAmount,
	}
}

type WithdrawStakingEvent struct {
	SchemaVersion       int       `json:"schema_version"`
	EventType           EventType `json:"event_type"` // always 3. WithdrawStakingEventType
	StakingTxHashHex    string    `json:"staking_tx_hash_hex"`
	WithdrawTxHashHex   string    `json:"withdraw_tx_hash_hex"`
	WithdrawTxBtcHeight uint64    `json:"withdraw_tx_btc_height"`
	WithdrawTxHex       string    `json:"withdraw_tx_hex"`
}

func (e WithdrawStakingEvent) GetEventType() EventType {
	return WithdrawStakingEventType
}

func (e WithdrawStakingEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewWithdrawStakingEvent(
	stakingTxHashHex string,
	withdrawTxHashHex string,
	withdrawTxBtcHeight uint64,
	withdrawTxHex string,
) WithdrawStakingEvent {
	return WithdrawStakingEvent{
		SchemaVersion:       WithdrawEventVersion,
		EventType:           WithdrawStakingEventType,
		StakingTxHashHex:    stakingTxHashHex,
		WithdrawTxHashHex:   withdrawTxHashHex,
		WithdrawTxBtcHeight: withdrawTxBtcHeight,
		WithdrawTxHex:       withdrawTxHex,
	}
}

type ExpiredStakingEvent struct {
	SchemaVersion    int       `json:"schema_version"`
	EventType        EventType `json:"event_type"` // always 4. ExpiredStakingEventType
	StakingTxHashHex string    `json:"staking_tx_hash_hex"`
	TxType           string    `json:"tx_type"`
}

func (e ExpiredStakingEvent) GetEventType() EventType {
	return ExpiredStakingEventType
}

func (e ExpiredStakingEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewExpiredStakingEvent(stakingTxHashHex string, txType string) ExpiredStakingEvent {
	return ExpiredStakingEvent{
		SchemaVersion:    ExpiredEventVersion,
		EventType:        ExpiredStakingEventType,
		StakingTxHashHex: stakingTxHashHex,
		TxType:           txType,
	}
}

type StatsEvent struct {
	SchemaVersion         int       `json:"schema_version"`
	EventType             EventType `json:"event_type"` // always 5. StatsEventType
	StakingTxHashHex      string    `json:"staking_tx_hash_hex"`
	StakerPkHex           string    `json:"staker_pk_hex"`
	FinalityProviderPkHex string    `json:"finality_provider_pk_hex"`
	StakingValue          uint64    `json:"staking_value"`
	State                 string    `json:"state"`
	IsOverflow            bool      `json:"is_overflow"`
}

func (e StatsEvent) GetEventType() EventType {
	return StatsEventType
}

func (e StatsEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewStatsEvent(
	stakingTxHashHex string,
	stakerPkHex string,
	finalityProviderPkHex string,
	stakingValue uint64,
	state string,
	isOverflow bool,
) StatsEvent {
	return StatsEvent{
		SchemaVersion:         StatsEventVersion,
		EventType:             StatsEventType,
		StakingTxHashHex:      stakingTxHashHex,
		StakerPkHex:           stakerPkHex,
		FinalityProviderPkHex: finalityProviderPkHex,
		StakingValue:          stakingValue,
		State:                 state,
		IsOverflow:            isOverflow,
	}
}

type BtcInfoEvent struct {
	SchemaVersion  int       `json:"schema_version"`
	EventType      EventType `json:"event_type"` // always 6. BtcInfoEventType
	Height         uint64    `json:"height"`
	ConfirmedTvl   uint64    `json:"confirmed_tvl"`
	UnconfirmedTvl uint64    `json:"unconfirmed_tvl"`
}

func (e BtcInfoEvent) GetEventType() EventType {
	return BtcInfoEventType
}

// Not applicable, add it here to implement the EventMessage interface
func (e BtcInfoEvent) GetStakingTxHashHex() string {
	return ""
}

func NewBtcInfoEvent(height, confirmedTvl, unconfirmedTvl uint64) BtcInfoEvent {
	return BtcInfoEvent{
		SchemaVersion:  BtcInfoEventVersion,
		EventType:      BtcInfoEventType,
		Height:         height,
		ConfirmedTvl:   confirmedTvl,
		UnconfirmedTvl: unconfirmedTvl,
	}
}

type ConfirmedInfoEvent struct {
	SchemaVersion int       `json:"schema_version"`
	EventType     EventType `json:"event_type"` // always 7. ConfirmedInfoEventType
	Height        uint64    `json:"height"`
	Tvl           uint64    `json:"tvl"`
}

func (e ConfirmedInfoEvent) GetEventType() EventType {
	return ConfirmedInfoEventType
}

// Not applicable, add it here to implement the EventMessage interface
func (e ConfirmedInfoEvent) GetStakingTxHashHex() string {
	return ""
}

func NewConfirmedInfoEvent(height, tvl uint64) ConfirmedInfoEvent {
	return ConfirmedInfoEvent{
		SchemaVersion: ConfirmedInfoEventVersion,
		EventType:     ConfirmedInfoEventType,
		Height:        height,
		Tvl:           tvl,
	}
}
