package persistent

type Persistent interface {
	UpdateState(state NodeState) error
	GetState(state NodeState) (*NodeState, error)
}
