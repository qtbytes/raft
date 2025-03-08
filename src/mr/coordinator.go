package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// task state
const UN_STARTED = 0
const STARTED = 1
const FINISHED = 2

// workers grpc args
const ASK_TASK = 3
const FINISH_TASK = 4

// coordinator grpc replys
const MAP_TASK = 6
const REDUCE_TASK = 7

const TIMEOUT = 10
const DEBUG = true

// Output for coordinator
func Red(s string) string {
	return fmt.Sprintf("\033[31m%s\033[0m", s)
}

// Output for worker
func Green(s string) string {
	return fmt.Sprintf("\033[32m%s\033[0m", s)
}

type Coordinator struct {
	cond        *sync.Cond
	nMap        int
	nReduce     int
	tasks       []string
	taskStates  []int
	mapCount    int
	reduceCount int
	taskExpiry  []time.Time
}

func (c *Coordinator) checkTimeouts() {
	for {
		time.Sleep(time.Second)

		c.cond.L.Lock()
		for i := range c.taskStates {
			if c.taskStates[i] == STARTED && time.Since(c.taskExpiry[i]).Seconds() >= TIMEOUT {
				c.taskStates[i] = UN_STARTED
				c.cond.Broadcast()
				if DEBUG {
					if i >= c.nMap {
						log.Println(Red(fmt.Sprintf("Reduce task %v timeout, reset to UNSTARTED", i-c.nMap)))
					} else {
						log.Println(Red(fmt.Sprintf("Map task %v timeout, reset to UNSTARTED", i)))
					}
				}
			}
		}
		c.cond.L.Unlock()
		if c.Done() {
			return
		}
	}
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	// Check worker's request
	if args.QueryID == FINISH_TASK {
		i := args.TaskID
		if args.TaskType == REDUCE_TASK {
			i += c.nMap
		}
		if c.taskStates[i] != FINISHED {
			if args.TaskType == REDUCE_TASK {
				c.taskStates[i] = FINISHED
				c.reduceCount--
				// if c.reduceCount == 0 {
				c.cond.Broadcast()
				// }
			} else {
				c.taskStates[i] = FINISHED
				c.mapCount--
				// if c.mapCount == 0 {
				c.cond.Broadcast()
				// }
			}
		}
	}
	for c.mapCount > 0 {
		// Send map task to workers
		for i := range c.nMap {
			if c.taskStates[i] == UN_STARTED || c.taskStates[i] == STARTED && time.Since(c.taskExpiry[i]).Seconds() >= TIMEOUT {
				c.taskStates[i] = STARTED
				c.taskExpiry[i] = time.Now()
				if DEBUG {
					log.Println(Red(fmt.Sprintf("Send Map task %v to worker", i)))
					log.Println(Red("Map task states"), c.taskStates[:c.nMap])
				}

				reply.TaskType = MAP_TASK
				reply.TaskID = i
				reply.Task = c.tasks[i]
				reply.NReduce = c.nReduce
				return nil
			}
		}
		// Another possibility is for the relevant RPC handler in the coordinator
		// to have a loop that waits, either with time.Sleep() or sync.Cond.
		// Go runs the handler for each RPC in its own thread,
		// so the fact that one handler is waiting needn't prevent the coordinator
		// from processing other RPCs.
		c.cond.Wait()
	}

	for c.reduceCount > 0 {
		// Send reduce task to workers
		for i := range c.nReduce {
			j := i + c.nMap
			if c.taskStates[j] == UN_STARTED || c.taskStates[j] == STARTED && time.Since(c.taskExpiry[j]).Seconds() >= TIMEOUT {
				c.taskStates[j] = STARTED
				c.taskExpiry[j] = time.Now()
				if DEBUG {
					log.Println(Red(fmt.Sprintf("Send reduce task %v to worker", i)))
					log.Println(Red("Reduce task states"), c.taskStates[c.nMap:])
				}

				reply.TaskType = REDUCE_TASK
				reply.TaskID = i
				reply.NMap = c.nMap
				return nil
			}
		}
		c.cond.Wait()
	}
	log.Println(Red("All jobs done!"))
	reply.TaskType = FINISHED
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
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	return c.reduceCount == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Init coordinator state

	c.cond = sync.NewCond(&sync.Mutex{})
	// Map task
	c.nMap = len(files)
	c.tasks = files
	c.mapCount = c.nMap
	// Reduce task
	c.nReduce = nReduce
	c.reduceCount = nReduce
	// Task states
	c.taskStates = make([]int, c.nMap+c.nReduce)
	c.taskExpiry = make([]time.Time, c.nMap+c.nReduce)

	// Check timeouts
	go c.checkTimeouts()
	c.server()
	return &c
}
