package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import (
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs    []Config // indexed by config num
	lastLogIdx int
}

type Op struct {
	// Your data here.
	Oper string // put get app leave join move query
	// Key      string   // for put/get/app
	// Value    string   // for put/get/app
	GID      int64    // for put/get/app Merge with GID     int64    // for shard unique replica group ID
	Servers  []string // for shard  group server ports
	ShardNum int      // for shard Move Merge with Num		int      // for shard Query desired config number
}

type rpcFunc func(Op, int, *QueryReply)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		// log.Printf(format, a...)
		fmt.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) printConfig(format string, i int) {
	DPrintf(format)
	DPrintf("config %d Num-%d Shards-%v\n", i, sm.configs[i].Num, sm.configs[i].Shards)
	DPrintf("\n")
	// Num    int                // config number
	// Shards [NShards]int64     // shard -> gid
	//Groups map[int64][]string // gid -> servers[]
}

func (sm *ShardMaster) printConfigs(format string) {
	DPrintf(format)
	DPrintf("sm: %d configs\n", sm.me)
	for i, _ := range sm.configs {
		DPrintf("config %d Num-%d Shards-%v\n", i, sm.configs[i].Num, sm.configs[i].Shards)
	}
	DPrintf("\n")
	// Num    int                // config number
	// Shards [NShards]int64     // shard -> gid
	//Groups map[int64][]string // gid -> servers[]
}

func (sm *ShardMaster) rpcRoutine(curProposal Op, insid int, reply *QueryReply, afterProp rpcFunc) {
	if curProposal.Oper != "Query" || curProposal.ShardNum != -1 {
		DPrintf("%v RPC me:%d id-%d\tGID-%d\t\n", curProposal.Oper, sm.me, insid, curProposal.GID)
	}

	sm.px.Start(insid, curProposal)

	to := 10 * time.Millisecond
	for {
		status, proposal := sm.px.Status(insid)
		if status == paxos.Decided {
			if _, isop := proposal.(Op); isop {
				// check if it is exact the same propose
				if (proposal.(Op).GID == curProposal.GID) && (proposal.(Op).Oper == curProposal.Oper) && (proposal.(Op).ShardNum == curProposal.ShardNum) {
					// kv.CheckMinDone(insid, curProposal)
					// kv.freeMem()
					afterProp(curProposal, insid, reply)
					return
				} else {
					// wrong proposal
					insid = sm.px.Max() + 1
					DPrintf("%v RPC me:%d id-%d\tGID-%d\n", curProposal.Oper, sm.me, insid, curProposal.GID)
					sm.px.Start(insid, curProposal)
					to = 10 * time.Millisecond
				}
			} else {
				DPrintf("%v RPC me:%d id-%d\tGID-%d\tWrong op type\n", curProposal.Oper, sm.me, insid, curProposal.GID)
			}
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) organizeShards(GID int64, prevCfg *Config, oper string) {
	if oper == "Join" {
		// do the reverse way for leave
		// find the max count of group, split one to GID, stop when delta between max count and GID count is 1
		agg := map[int64]int{}
		maxGroupCnt := 0
		maxGroupId := int64(-1)
		for _, g := range prevCfg.Shards {
			if g == GID {
				// if GID already exist then skip
				return
			}
			agg[g]++
		}
		for {
			// find min and assign assign one empty
			maxGroupCnt = 0
			maxGroupId = int64(-1)

			for k, v := range agg {
				if maxGroupCnt < v {
					maxGroupCnt = v
					maxGroupId = k
				}
			}
			//DPrintf("in Join agg:%v maxGroupCnt:%d maxGroupId:%d agg[%d]:%d\n", agg, maxGroupCnt, maxGroupId, GID, agg[GID])
			if agg[GID] >= (maxGroupCnt - 1) {
				break
			}
			for i, g := range prevCfg.Shards {
				if g <= 0 {
					prevCfg.Shards[i] = GID
					agg[GID]++
					agg[g]--
				} else if g == maxGroupId {
					prevCfg.Shards[i] = GID
					agg[g]--
					agg[GID]++
					break
				}
			}
		}
	} else if oper == "Leave" {
		// record all new empty shard index
		tba := []int{}
		for i, v := range prevCfg.Shards {
			if v == GID {
				tba = append(tba, i)
			}
		}
		//DPrintf("in Leave tba:%v\n", tba)
		agg := map[int64]int{}
		for _, g := range prevCfg.Shards {
			if g != GID {
				agg[g]++
			}
		}
		for _, s := range tba {
			// find min and assign assign one empty
			minGroupCnt := NShards * NShards
			minGroupId := int64(-1)
			//DPrintf("in Leave agg:%v\n", agg)
			for k, _ := range prevCfg.Groups {
				if minGroupCnt > agg[k] {
					minGroupCnt = agg[k]
					minGroupId = k
				}
			}
			prevCfg.Shards[s] = minGroupId
			agg[minGroupId]++
			//DPrintf("in Leave change %d %v\n", s, prevGrp)
		}
	}

}

func CopyMap(dstMap map[int64][]string, srcMap map[int64][]string) {
	for k, v := range srcMap {
		dstMap[k] = v
	}
}

func (sm *ShardMaster) joinRpc(curProposal Op) {
	newconfig := Config{len(sm.configs), sm.configs[len(sm.configs)-1].Shards, map[int64][]string{}}

	CopyMap(newconfig.Groups, sm.configs[len(sm.configs)-1].Groups)
	newconfig.Groups[curProposal.GID] = curProposal.Servers
	//sm.printConfigs("before organize\n")
	sm.printConfig("before joinRpc organize\n", len(sm.configs)-1)
	sm.organizeShards(curProposal.GID, &newconfig, curProposal.Oper)
	sm.configs = append(sm.configs, newconfig)
	//sm.printConfigs("after joinRpc organize\n")
	sm.printConfig("after joinRpc organize\n", len(sm.configs)-1)

}

func (sm *ShardMaster) checkMinDone(insid int) {
	// ask [min,insid]'s status to get log, if it is cfg related then continue, otherwise stop then call Done
	//DPrintf("here1\n")
	pre_insid := sm.px.Min()
	for ; pre_insid < insid; pre_insid++ {
		//DPrintf("here2 %d\n", pre_insid)
		err, pre_v := sm.px.Status(pre_insid)
		if err == paxos.Decided {
			_, ok := pre_v.(Op)
			if !ok {
				break
			}
		} else if err != paxos.Forgotten {
			break
		}
	}
	//DPrintf("here3 %d\n", pre_insid)
	sm.px.Done(pre_insid)
	//DPrintf("here4 %d\n", pre_insid)
	sm.px.Min()
}

func (sm *ShardMaster) emptyRpc(curProposal Op, insid int, reply *QueryReply) {
	sm.interpolateLog(insid, curProposal)
	sm.checkMinDone(insid)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	curProposal := Op{"Join", args.GID, args.Servers, -1}
	insid := sm.px.Max() + 1

	sm.rpcRoutine(curProposal, insid, nil, sm.emptyRpc)
	return nil
}

func (sm *ShardMaster) leaveRpc(curProposal Op) {
	newconfig := Config{len(sm.configs), sm.configs[len(sm.configs)-1].Shards, map[int64][]string{}}

	CopyMap(newconfig.Groups, sm.configs[len(sm.configs)-1].Groups)
	delete(newconfig.Groups, curProposal.GID)

	sm.printConfig("before leaveRpc organize\n", len(sm.configs)-1)
	//sm.printConfigs("before leaveRpc organize\n")
	sm.organizeShards(curProposal.GID, &newconfig, curProposal.Oper)
	sm.configs = append(sm.configs, newconfig)
	//sm.printConfigs("after leaveRpcorganize\n")
	sm.printConfig("after leaveRpc organize\n", len(sm.configs)-1)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	curProposal := Op{"Leave", args.GID, nil, -1}
	insid := sm.px.Max() + 1

	sm.rpcRoutine(curProposal, insid, nil, sm.emptyRpc)

	return nil
}

func (sm *ShardMaster) moveRpc(curProposal Op) {
	newconfig := Config{len(sm.configs), sm.configs[len(sm.configs)-1].Shards, map[int64][]string{}}
	CopyMap(newconfig.Groups, sm.configs[len(sm.configs)-1].Groups)

	sm.printConfigs("before moving\n")

	newconfig.Shards[curProposal.ShardNum] = curProposal.GID
	sm.configs = append(sm.configs, newconfig)

	sm.printConfigs("after moving\n")
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	curProposal := Op{"Move", args.GID, nil, args.Shard}
	insid := sm.px.Max() + 1

	sm.rpcRoutine(curProposal, insid, nil, sm.emptyRpc)

	return nil
}

func (sm *ShardMaster) interpolateLog(insid int, curLog Op) {
	to := 10 * time.Millisecond

	// step 1: remove seq with duplicate ids
	for pre_insid := sm.lastLogIdx + 1; pre_insid <= insid; pre_insid++ {
		//DPrintf("interpretLog %d: call Status\t%d\n", kv.me, pre_insid)
		err, pre_v := sm.px.Status(pre_insid)
		// if it is old seq then we ask it to propose
		sm.px.Start(pre_insid, pre_v)
		for err != 1 {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			err, pre_v = sm.px.Status(pre_insid)
		}

		if _, isop := pre_v.(Op); isop {
			// step 2: interpret the log before that point to make sure its key/value db reflects all recent put()s
			if pre_v.(Op).Oper == "Join" {
				sm.joinRpc(pre_v.(Op))
			} else if pre_v.(Op).Oper == "Leave" {
				sm.leaveRpc(pre_v.(Op))
			} else if pre_v.(Op).Oper == "Move" {
				sm.moveRpc(pre_v.(Op))
			}
			sm.lastLogIdx = pre_insid
		}
	}
	// step 3, forget logs before last query
	// for pre_insid := insid - 1; pre_insid >= 0; pre_insid-- {
	// 	err, pre_v := sm.px.Status(pre_insid)
	// 	if err == paxos.Decided && pre_v.(Op).Oper == "Query" {
	// 		if curLog.Oper != "Query" {
	// 			DPrintf("me:%d Call Done %d\n", sm.me, (insid - 1))
	// 			sm.px.Done(insid - 1)
	// 			sm.px.Min()
	// 		}

	// 	}
	// }
}

func (sm *ShardMaster) queryRpc(curProposal Op, insid int, reply *QueryReply) {
	// reply should assigned to some configuration
	//DPrintf("query %d config size:%d\n", curProposal.ShardNum, len(sm.configs))
	// do the interpolating
	sm.interpolateLog(insid, curProposal)
	sm.checkMinDone(insid)
	if (curProposal.ShardNum == -1) || (curProposal.ShardNum > len(sm.configs)) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[curProposal.ShardNum]
	}

}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	curProposal := Op{"Query", -1, nil, args.Num}
	//DPrintf("Query proposal with num %d %v\n", args.Num, curProposal)
	insid := sm.px.Max() + 1

	sm.rpcRoutine(curProposal, insid, reply, sm.queryRpc)

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.lastLogIdx = -1

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
