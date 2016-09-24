package mapreduce

import "container/list"
import "fmt"
//import "time"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr * MapReduce) updateRegChannel(){
	for !mr.bDone {
		select{
			case address := <- mr.registerChannel:
				mr.Workers[address] = &WorkerInfo{address}	
			case address := <- mr.idleChannel:
				mr.Workers[address] = &WorkerInfo{address}	
			default:
				//time.Sleep(50 * time.Millisecond)
		}
		
	}
}

func getTaskIdx(tasks []string) int{
	for idx, statue := range tasks{
		if statue == ""{
			return idx
		}
	}
	return len(tasks)
}

func (mr * MapReduce) assignWork (jobType JobType, tasks []string, nCount int, nOtherPhrase int){
	for{
		for _, w := range mr.Workers {
			DPrintf("DoWork: assign %s to %s\n", jobType, w.address)
			taskIdx := getTaskIdx(tasks)

			if taskIdx < nCount{
				jobArgs := &DoJobArgs{mr.file, jobType, taskIdx, nOtherPhrase}
				reply := &DoJobReply{}
				tasks[taskIdx] = "processing"
				res := call(w.address, "Worker.DoJob", &jobArgs, &reply)

				if res == true{
					if reply.OK {
						// job success
						tasks[taskIdx] = "done"
					} else{
						// job failed
						tasks[taskIdx] = ""	
					}
					mr.idleChannel <- w.address
				} else{
					// worker failed
					tasks[taskIdx] = ""	
					delete (mr.Workers,w.address)
				}
			} else{
				DPrintf("DoWork: %s DONE\n", jobType)
				if jobType == Reduce{
					mr.bDone = true
				}
				return
			}
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// update worker from register channel
	go mr.updateRegChannel()
	// assign job to map
	mr.assignWork(Map, mr.mapTaskList, mr.nMap , mr.nReduce)
	// assign job to reduce
	mr.assignWork(Reduce, mr.reduceTaskList, mr.nReduce , mr.nMap)

	return mr.KillWorkers()
}
