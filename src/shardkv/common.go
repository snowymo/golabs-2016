package shardkv

import "shardmaster"
import (
	"paxos"
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

type BKArgs struct {
	Config    shardmaster.Config
	LastLogId int
	Uidmap    map[int64]int //uidmap = make(map[int64]bool)
	LogCache  map[int]Op    // logCache = make(map[int]Op)
	ValidList map[int]bool  // validList = make(map[int]bool, 0)
	MinDone   int
	//PX        **paxos.Paxos
}

type BKReply struct {
	Err Err
}

const MAXUID = 4294967296
