package paxos

const (
	OK  = "OK"
	NOK = "NOK"
)

type Agreement struct {
	//	seq	int
	value  interface{}
	status Fate
	highP  int
	highA  int
}

// Prepare
type PrepareArgs struct {
	Seq int
	N   int
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

// Prepare
type PrepareReply struct {
	NAccept int
	Value   interface{}
	NHigh   int
	OK      string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type AcceptArgs struct {
	Seq   int
	N     int
	Value interface{}
}
type AcceptReply struct {
	OK    bool
	NHigh int
}

type DecideArgs struct {
	Seq   int
	N     int
	Value interface{}
}
type DecideReply struct {
	OK bool
}
type GetDoneArgs struct {
}
type GetDoneReply struct {
	DoneSeq int
}
