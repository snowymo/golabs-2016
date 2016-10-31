package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "strconv"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const DEBUG = false

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

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
	Seq string
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

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	agreem     map[int]*Agreement
	doneSeq    int
	proposalNo int
	propWseq   map[int]int
}

func (px *Paxos) print() {
	if DEBUG {
		fmt.Printf("me:\t%d\n", px.me)
		fmt.Printf("propose no:%d\n", px.proposalNo)
		for k, v := range px.propWseq {
			fmt.Printf("seq-%d: prop-%v\n", k, v)
		}
		//fmt.Println("decision:")
		for k, v := range px.agreem {
			fmt.Printf("decision:\tseq-%d: v-%v\tsta-%d\thighp-%d\thighA-%d\n", k, v.value, v.status, v.highP, v.highA)
		}

		fmt.Println("\n")
	}

}

func (px *Paxos) debugPrintln(a ...interface{}) {
	if DEBUG {
		fmt.Println(a...)
	}
}

func (px *Paxos) debugPrintf(format string, a ...interface{}) {
	if DEBUG {
		fmt.Printf(format, a...)
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//	px.agreem[seq] = Agreement
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq >= px.doneSeq {
		_, ok := px.agreem[seq]
		if !ok {
			px.agreem[seq] = &Agreement{v, Pending, -1, -1}
		}
	}
	px.debugPrintf("Start: before propose %d\t%v\n", seq, v)
	go px.Propose(seq, v)
	return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	px.doneSeq = seq
	// discard
	for k, v := range px.agreem {
		if k < seq {
			v.status = Forgotten
		}
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	maxseq := -1
	for k, _ := range px.agreem {
		if k > maxseq {
			maxseq = k
		}
	}
	return maxseq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.doneSeq == -1 {
		return -1
	}
	return px.doneSeq + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	agr, ok := px.agreem[seq]
	if ok {
		return agr.status, agr.value
	}
	return Pending, nil
}

// func (px *Paxos) GetValueFromProp(propno int) interface{} {
// 	for _, v := range px.agreem {
// 		if v.prono == propno {
// 			return v.value
// 		}
// 	}
// 	return "404"
// }

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.debugPrintf("Prepare RPC %d: seq-%v with prop-%d\n", px.me, args.Seq, args.N)
	//px.print()
	seq, _ := strconv.Atoi(args.Seq)
	agr, ok := px.agreem[seq]
	if ok && px.agreem[seq].status != Pending {
		px.debugPrintf("Prepare RPC %d: NOT PENDING for seq-%v\n", px.me, args.Seq)
		reply.OK = NOK
		reply.NHigh = -1
		return nil
	}

	if !ok {
		px.debugPrintf("Prepare RPC %d: new for this seq-%v\n", px.me, args.Seq)
		px.agreem[seq] = &Agreement{"", Pending, args.N, -1}
		agr = px.agreem[seq]

		if px.proposalNo < args.N {
			px.proposalNo = args.N
		}

		reply.OK = OK
		reply.NHigh = args.N
	} else if args.N > agr.highP {
		px.debugPrintf("Prepare RPC %d: seq-%v incoming is bigger prop-%v > high-%d\n", px.me, args.Seq, args.N, agr.highP)

		if px.proposalNo < args.N {
			px.proposalNo = args.N
		}

		px.agreem[seq].highP = args.N

		reply.OK = OK
		reply.NHigh = args.N
		//reply.NAccept = px.agreem[args.seq].highA
		//reply.Value = px.agreem[args.seq].value
	} else {
		px.debugPrintf("Prepare RPC %d: seq-%v mine is bigger prop-%d <= high-%d\n", px.me, args.Seq, args.N, agr.highP)
		reply.OK = NOK
		reply.NHigh = px.agreem[seq].highP
	}
	//px.debugPrintf("Prepare RPC %d: cur high prop-%d\n", px.me, px.agreem[args.seq].highP)
	px.print()
	return nil
}

// phase prepare
func (px *Paxos) preparePhase(seq int, value interface{}) (int, interface{}) {
	if px.agreem[seq].status != Pending {
		return 0, 0
	}

	args := &PrepareArgs{strconv.Itoa(seq), px.propWseq[seq]}
	//args.N =
	//args.seq =
	var reply PrepareReply
	px.debugPrintf("preparePhase: prepare args seq-%v\tval-%v\tprop-%d\n", args.Seq, value, args.N)
	prepareCnt := 0
	highA := seq
	highV := value

	// send to itself
	px.debugPrintln("preparePhase: prepare itself")
	px.Prepare(args, &reply)
	if reply.OK == OK {
		prepareCnt++
	}
	// send prepare rpc to other peers
	//px.debugPrintf("preparePhase: prepare peers seq-%d\tval-%v\tprop-%d len:%d\n", args.seq, value, args.N, len(px.peers))
	for i, p := range px.peers {
		if i != px.me {
			//px.debugPrintf("preparePhase: in range peers %d:%v before call\n", i, p)
			//args := &PrepareArgs{}
			args.N = px.propWseq[seq]
			args.Seq = strconv.Itoa(seq)
			//var reply PrepareReply
			px.debugPrintf("preparePhase: prepare peers seq-%v\tval-%v\tprop-%d idx:%d\n", args.Seq, value, args.N, i)
			ok := call(p, "Paxos.Prepare", args, &reply)
			px.debugPrintf("reply:%v %d\n", reply.OK, reply.NHigh)
			if ok && reply.OK == OK {
				prepareCnt++
				if reply.NAccept > highA {
					highA = reply.NAccept
					highV = reply.Value
				}
			}
		}
	}
	px.debugPrintf("preparePhase: finish prepare peers maj:%d\n", prepareCnt)
	return prepareCnt, highV
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.N >= px.agreem[args.Seq].highP {
		px.debugPrintf("Accept RPC %d: seq-%d incoming is bigger %d >= %d\n", px.me, args.Seq, args.N, px.agreem[args.Seq].highP)
		px.agreem[args.Seq].highP = args.N
		px.agreem[args.Seq].highA = args.N
		px.agreem[args.Seq].value = args.Value
		reply.OK = true
	} else {
		px.debugPrintf("Accept RPC %d: seq-%d mine is bigger %d < %d\n", px.me, args.Seq, args.N, px.agreem[args.Seq].highP)
		reply.OK = false
		reply.NHigh = px.agreem[args.Seq].highP
	}
	px.print()
	return nil
}

func (px *Paxos) acceptPhase(seq int, value interface{}) int {
	args := &AcceptArgs{}
	args.Seq = seq
	args.N = px.proposalNo
	args.Value = value
	var reply AcceptReply

	acceptCnt := 0

	// send to itself
	px.Accept(args, &reply)
	if reply.OK {
		acceptCnt++
	}
	// send accept rpc to other peers
	for i, p := range px.peers {
		if i != px.me {
			ok := call(p, "Paxos.Accept", args, &reply)
			if ok && reply.OK {
				acceptCnt++
			}
		}
	}
	px.debugPrintf("preparePhase: finish Accept peers maj:%d\n", acceptCnt)
	return acceptCnt
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.debugPrintf("Decide RPC %d: seq-%d\t%v\n", px.me, args.Seq, args.Value)
	_, ok := px.agreem[args.Seq]
	if ok {
		px.agreem[args.Seq].status = Decided
		px.agreem[args.Seq].highA = args.N
		px.agreem[args.Seq].highP = args.N
		px.agreem[args.Seq].value = args.Value
	} else {
		px.agreem[args.Seq] = &Agreement{args.Value, Decided, args.N, args.N}
	}
	px.print()
	reply.OK = true
	return nil
}

func (px *Paxos) decidePhase(seq int, value interface{}) {
	args := &DecideArgs{}
	args.Seq = seq
	args.N = px.proposalNo
	args.Value = value
	var reply DecideReply

	// send to itself
	px.Decide(args, &reply)
	// if reply.OK {
	// 	acceptCnt++
	// }
	// send accept rpc to other peers
	for i, p := range px.peers {
		if i != px.me {
			call(p, "Paxos.Decide", args, &reply)
			// if ok && reply.OK {
			// 	acceptCnt++
			// }
		}
	}
	//return acceptCnt
}

func (px *Paxos) generateNo(seq int) int {
	px.mu.Lock()
	defer px.mu.Unlock()
	//px.debugPrintf("Propose ProposalNo\t%d\n", px.proposalNo)
	px.proposalNo = px.proposalNo + rand.Int()%3
	//px.agreem[seq].highP = px.proposalNo
	px.debugPrintf("Propose ProposalNo\t%d\n", px.proposalNo)
	px.propWseq[seq] = px.proposalNo
	return px.proposalNo
}

// do propose
func (px *Paxos) Propose(seq int, value interface{}) {
	if px.agreem[seq].status == Pending {
		// choose n, unique and higher than any n seen so far

		//px.agreem[seq].prono = px.proposalNo
		px.debugPrintf("Propose %d: seq-%d before prepare phase\n", px.me, seq)
		px.generateNo(seq)
		prepareCnt, value := px.preparePhase(seq, value)
		acceptCnt := 0
		// send accept rpc to other peers
		if prepareCnt > len(px.peers)/2.0 {
			px.debugPrintln("Propose: before accept phase")
			acceptCnt = px.acceptPhase(seq, value)
		}
		if acceptCnt > len(px.peers)/2.0 {
			// send decide to all
			px.debugPrintln("Propose: before decide phase")
			px.decidePhase(seq, value)
		}
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.agreem = make(map[int]*Agreement)
	px.doneSeq = -1
	px.proposalNo = -1
	px.propWseq = make(map[int]int)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
