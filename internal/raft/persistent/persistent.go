package persistent

type Persistent interface {
	UpdateState(state NodeState) error
	GetState() (*NodeState, error)
}
