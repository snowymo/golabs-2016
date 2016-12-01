package shardkv

import (
	"shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNoSnap     = "ErrNoSnap"
	NOK           = "NOK"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id    int64 // unique id
	Shard int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id    int64 // unique id
	Shard int
}

type GetReply struct {
	Err   Err
	Value string
}

type UpdateArgs struct {
	SnapNo int
	Id     int64 // unique id
}

type UpdateReply struct {
	Uidmap map[int64]int //uidmap = make(map[int64]bool)
	DB     map[string]string
	U2S    map[int64]int
	Err    Err
}

type UpSnapArgs struct {
	Config shardmaster.Config
	Id     int64 // unique id
}

type UpSnapReply struct {
	Err Err
}

const MAXUID = 4294967296
