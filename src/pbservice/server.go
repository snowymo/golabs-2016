package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import (
	"bytes"
	"strconv"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	curView  viewservice.View
	kv       map[string]string
	uid      map[int64]string
	uidMutex sync.Mutex
}

func (pb *PBServer) print() {
	if DEBUG {
		fmt.Println("me:" + pb.me)
		fmt.Printf("curView:%d pri:\t%v\tback:\t%v\n", pb.curView.Viewnum, pb.curView.Primary, pb.curView.Backup)
		fmt.Println("kv:")
		for k, v := range pb.kv {
			fmt.Printf("%s:%s\t", k, v)
		}
		fmt.Println("\n")
	}

}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.isPrimary() {
		key := args.Key

		v, ok := pb.kv[key]

		if ok {
			reply.Value = v
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrWrongServer
	}
	pb.debugPrintf("GET isPri:%t\tkey:%v\tvalue:%v\n", pb.isPrimary(), args.Key, reply.Value)
	return nil
}

func (pb *PBServer) isPrimary() bool {
	if pb.curView.Primary == pb.me {
		return true
	} else {
		return false
	}
}

func (pb *PBServer) debugPrintln(a ...interface{}) {
	if DEBUG {
		fmt.Println(a...)
	}
}

func (pb *PBServer) debugPrintf(format string, a ...interface{}) {
	if DEBUG {
		fmt.Printf(format, a...)
	}
}

func (pb *PBServer) putAppendHelp(args *PutAppendArgs, reply *PutAppendReply) {
	key, value := args.Key, args.Value
	v, ok := pb.kv[key]
	if ok {
		var buffer bytes.Buffer
		if args.Op == "Append" {
			buffer.WriteString(v)
		}
		buffer.WriteString(value)
		pb.kv[key] = buffer.String()
		reply.Err = OK
	} else {
		pb.kv[key] = value
		reply.Err = ErrNoKey
	}
	pb.uid[args.Id] = args.Key + args.Value
	pb.print()
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()

	pb.debugPrintf("RPC PutAppend before dbsize:%d\t%t\t%v:%v\n", len(pb.kv), pb.isPrimary(), args.Key, args.Value)

	if pb.isPrimary() {
		//key, value := args.Key, args.Value
		if pb.uid[args.Id] == "" {
			pb.putAppendHelp(args, reply)
			// send to backup
			// pb.mu.Unlock()
			// pb.mu.Lock()
			if pb.curView.Backup != "" {
				okUpdate := false
				//curBackup := pb.curView.Backup
				//curPrimary := pb.curView.Primary
				// args.Update = false
				for !okUpdate && pb.curView.Backup != "" {
					//pb.debugPrintln("test4")
					// if curPrimary != pb.curView.Primary {
					// 	curPrimary = pb.curView.Primary
					// 	// backup is changed outside, we need to forward the rpc to primary either
					// 	if pb.isPrimary() {
					// 		if pb.uid[args.Id] == "" {
					// 			pb.debugPrintln("server reput append " + curPrimary + " " + args.Key + " " + args.Value + " " + strconv.FormatInt(args.Id, 10))
					// 			pb.putAppendHelp(args, reply)
					// 		} else {
					// 			ok := false
					// 			for !ok || (reply.Err == ErrWrongServer) {
					// 				pb.debugPrintln("server resend put append " + curPrimary + " " + args.Key + " " + args.Value + " " + strconv.FormatInt(args.Id, 10))
					// 				ok = call(curPrimary, "PBServer.PutAppend", args, &reply)
					// 			}
					// 		}
					// 	}
					// }
					pb.mu.Unlock()
					pb.mu.Lock()
					pb.debugPrintln("forward to backup:" + pb.curView.Backup + "\t" + args.Key + ":" + args.Value)
					okUpdate = call(pb.curView.Backup, "PBServer.Update", args, &reply)

				}
			}
		} else {
			reply.Err = OK
		}

	} else {
		reply.Err = ErrWrongServer
	}

	defer pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Update(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if DEBUG {
		fmt.Printf("RPC Update before:%s dbsize:%d %t\t%v:%v\n", pb.curView.Backup, len(pb.kv), pb.isPrimary(), args.Key, args.Value)
	}
	if !pb.isPrimary() {
		key, value := args.Key, args.Value
		if args.Update || pb.uid[args.Id] == "" {
			v, ok := pb.kv[key]
			if ok {
				var buffer bytes.Buffer
				if args.Op == "Append" {
					buffer.WriteString(v)
				}
				buffer.WriteString(value)
				pb.kv[key] = buffer.String()
				// if args.Op != "Append" {
				// 	v = ""
				// }
				// pb.kv[key] = v + value
				reply.Err = OK
			} else {
				pb.kv[key] = value
				reply.Err = ErrNoKey
			}
			pb.uid[args.Id] = args.Key + args.Value
		}
	} else {
		reply.Err = ErrWrongServer
	}
	pb.print()
	// if DEBUG {
	// 	fmt.Printf("RPC Update after:%s dbsize:%d\n", pb.curView.Backup, len(pb.kv))
	// }
	return nil
}

func (pb *PBServer) Updateuid(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//pb.debugPrintf("RPC Update UID before:%s dbsize:%d %t\n", pb.curView.Backup, len(pb.kv), pb.isPrimary())

	if !pb.isPrimary() {
		//key, value := args.Key, args.Value
		ikey, _ := strconv.ParseInt(args.Key, 10, 64)
		if args.Update || pb.uid[ikey] == "" {
			pb.uid[ikey] = "t"
		}
	} else {
		reply.Err = ErrWrongServer
	}
	//pb.print()
	// if DEBUG {
	// 	fmt.Printf("RPC Update after:%s dbsize:%d\n", pb.curView.Backup, len(pb.kv))
	// }
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	// args := &viewservice.PingArgs{}
	// args.Me = pb.me
	// args.Viewnum = pb.curView.Viewnum
	// var reply viewservice.PingReply
	pb.mu.Lock()
	defer pb.mu.Unlock()
	ok := false
	var v viewservice.View
	for !ok {
		v, ok = pb.vs.Get()
	}

	v, _ = pb.vs.Ping(v.Viewnum)
	pb.debugPrintf("pb tick after ping:\t%v\tcurviewno:\t%d\trealviewno:%d\n", pb.vs.Getme(), pb.curView.Viewnum, v.Viewnum)
	// if v.Viewnum != pb.curView.Viewnum {
	// 	pb.debugPrintf("pb tick after ping again:\t%v\tcurviewno:\t%d\trealviewno:%d\n", pb.vs.Getme(), pb.curView.Viewnum, v.Viewnum)
	// 	v, _ = pb.vs.Ping(v.Viewnum)
	// }

	if v.Viewnum != pb.curView.Viewnum {
		// if view changed:
		//   transition to new view.
		//   manage transfer of state from primary to new backup.
		//for v.Viewnum != pb.curView.Viewnum {
		pb.curView = v
		// 	v, _ = pb.vs.Ping(pb.curView.Viewnum)
		// }

		//fmt.Println("change view in tick")
		if pb.isPrimary() && pb.curView.Backup != "" {
			//fmt.Printf("forward whole db to backup:%d\n", len(pb.kv))

			args := &PutAppendArgs{}
			args.Op = "Put"
			args.Update = true
			var reply PutAppendReply
			for k, v := range pb.kv {
				args.Key = k
				args.Value = v
				//fmt.Printf("foward ing\t%s:%s\n", k, v)
				okUpdate := false
				pb.print()
				pb.debugPrintf("backup in tick:%v\n", pb.curView.Backup)
				for !okUpdate && pb.curView.Backup != "" {
					okUpdate = call(pb.curView.Backup, "PBServer.Update", args, &reply)
				}

			}
			for k, v := range pb.uid {
				args.Key = strconv.FormatInt(k, 10)
				args.Value = v
				//fmt.Printf("foward ing\t%s:%s\n", k, v)
				okUid := false
				for !okUid && pb.curView.Backup != "" {
					okUid = call(pb.curView.Backup, "PBServer.Updateuid", args, &reply)
				}
			}

		}
	}

	//ok := call(pb.vs, "ViewServer.Ping", args, &reply)
	// if ok == false {
	// 	//pb.curView = View{}
	// } else {
	// 	pb.curView = v
	// }
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.curView = viewservice.View{0, "", ""}
	pb.kv = make(map[string]string)
	pb.uid = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
