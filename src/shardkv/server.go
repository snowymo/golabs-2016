package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type rpcFunc func(Op, int, *GetReply)

type Op struct {
	// Your definitions here.
	Oper  string
	Key   string
	Value string
	Uid   int64
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	config shardmaster.Config
}

func (kv *ShardKV) rpcRoutine(curProposal Op, insid int, reply *GetReply, afterProp rpcFunc) {
	DPrintf("%v RPC me:%d id-%d\tkey-%v\tvalue-%v\n", curProposal.Oper, kv.me, insid, curProposal.Key, curProposal.Value)

	kv.px.Start(insid, curProposal)

	to := 10 * time.Millisecond
	for {
		status, proposal := kv.px.Status(insid)
		if status == paxos.Decided {
			if _, isop := proposal.(Op); isop {
				// check if it is exact the same propose
				if proposal.(Op) == curProposal {
					// kv.CheckMinDone(insid, curProposal)
					// kv.freeMem()
					afterProp(curProposal, insid, reply)
					return
				} else {
					// wrong proposal
					insid = kv.px.Max() + 1
					DPrintf("%v RPC me:%d id-%d\tkey-%v\tvalue-%v\n", curProposal.Oper, kv.me, insid, curProposal.Key, curProposal.Value)
					kv.px.Start(insid, curProposal)
					to = 10 * time.Millisecond
				}
			} else {
				DPrintf("%v RPC me:%d id-%d\tWrong op type\n", curProposal.Oper, kv.me, insid)
			}
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) emptyRpc(curProposal Op, insid int, reply *GetReply) {
	//kv.interpolateLog(insid, curProposal)
	if curProposal.Oper == "Get" {

	}
	reply.Err = OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if the shard belongs to the group id contain current server
	if kv.gid != kv.config.Shards[args.Shard] {
		reply.Err = ErrWrongGroup
		return nil
	}

	curProposal := Op{"Get", args.Key, "", args.Id}
	insid := kv.px.Max() + 1

	kv.rpcRoutine(curProposal, insid, reply, kv.emptyRpc)

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if the shard belongs to the group id contain current server
	if kv.gid != kv.config.Shards[args.Shard] {
		reply.Err = ErrWrongGroup
		return nil
	}

	curProposal := Op{args.Op, args.Key, args.Value, args.Id}
	insid := kv.px.Max() + 1

	tmpReply := &GetReply{}
	kv.rpcRoutine(curProposal, insid, tmpReply, kv.emptyRpc)
	reply.Err = tmpReply.Err

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	curCfg := kv.sm.Query(-1)
	if kv.config.Num != curCfg.Num {
		//DPrintf("me:%d-%d\tchange config from %v to %v\n", kv.gid, kv.me, kv.config, curCfg)
		kv.config.Num = curCfg.Num
		kv.config.Shards = curCfg.Shards
		shardmaster.CopyMap(kv.config.Groups, curCfg.Groups)
		//DPrintf("me:%d\tafter change config to %v\n", kv.me, kv.config)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	kv.config = shardmaster.Config{-1, [shardmaster.NShards]int64{}, map[int64][]string{}}
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
