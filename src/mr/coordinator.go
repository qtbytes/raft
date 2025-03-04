package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// const UN_STARTED = 0
// const MAP_FINISHED = 1
// const REDUCE_FINISHED = 2
const ASK_MAP = 3
const FINISH_MAP = 4
const FINISH_REDUCE = 5

type Coordinator struct {
	nReduce      int
	tasks        []string
	mapStates    []bool
	mapCount     int
	reduceStates []bool
	reduceCount  int
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {

	// check worker's request
	if args.query_id == FINISH_REDUCE {
		c.reduceStates[args.task_id] = true
		c.reduceCount--
	} else if args.query_id == FINISH_MAP {
		c.mapStates[args.task_id] = true
		c.mapCount--
	}

	// send tasks to worker
	if c.mapCount > 0 {
		// Send map task to workers
		for i, finish := range c.mapStates {
			if !finish {
				reply.task_id = i
				reply.task = c.tasks[i]
				reply.nReduce = c.nReduce
				break
			}
		}

	} else if c.reduceCount > 0 {
		// Send map task to workers
		for i, finish := range c.reduceStates {
			if !finish {
				reply.task_id = i
				reply.task = ""
				reply.nReduce = c.nReduce
				break
			}
		}
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

	c.nReduce = nReduce
	c.tasks = files
	c.mapCount = len(files)
	c.mapStates = make([]bool, len(files))
	c.reduceCount = nReduce
	c.reduceStates = make([]bool, nReduce)

	c.server()
	return &c
}
