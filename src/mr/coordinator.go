package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// task state
const UN_STARTED = 0
const STARTED = 1
const FINISHED = 2

// workers grpc args
const ASK_MAP = 3
const ASK_REDUCE = 4
const FINISH = 5

// coordinator grpc replys
const MAP_TASK = 6
const REDUCE_TASK = 7

const TIMEOUT = 10

type Coordinator struct {
	lock         sync.Mutex
	nMap         int
	nReduce      int
	tasks        []string
	mapStates    []int
	mapCount     int
	reduceStates []int
	reduceCount  int
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// check worker's request
	if args.QueryID == FINISH {
		c.reduceStates[args.TaskID] = FINISHED
		c.reduceCount--
	} else if args.QueryID == ASK_REDUCE {
		c.mapStates[args.TaskID] = FINISHED
		c.mapCount--
	}

	// send tasks to worker
	if c.mapCount > 0 {
		// Send map task to workers
		for i, state := range c.mapStates {
			if state == UN_STARTED {
				c.mapStates[i] = STARTED
				reply.TaskType = MAP_TASK
				reply.TaskID = i
				reply.Task = c.tasks[i]
				reply.NReduce = c.nReduce
				fmt.Println("Map task states", c.mapStates)
				break
			}
		}

	} else if c.reduceCount > 0 {
		// Send reduce task to workers
		for i, state := range c.reduceStates {
			if state == UN_STARTED {
				c.reduceStates[i] = STARTED
				reply.TaskType = REDUCE_TASK
				reply.TaskID = i
				reply.NMap = c.nMap
				fmt.Println("Reduce task states", c.reduceStates)
				break
			}
		}
	} else {
		reply.TaskType = FINISH
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceCount == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.lock = sync.Mutex{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.tasks = files
	c.mapCount = c.nMap
	c.mapStates = make([]int, c.nMap)
	c.reduceCount = nReduce
	c.reduceStates = make([]int, nReduce)

	c.server()
	return &c
}
