package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// Workers will sometimes need to wait, e.g.
		// reduces can't start until the last map has finished.
		// One possibility is for workers to periodically ask the coordinator for work,
		// sleeping with time.Sleep() between each request.
		// Another possibility is for the relevant RPC handler in the coordinator
		// to have a loop that waits, either with time.Sleep() or sync.Cond.
		// Go runs the handler for each RPC in its own thread,
		// so the fact that one handler is waiting needn't prevent the coordinator
		// from processing other RPCs.

		// ask task from coordinator
		reply := CallExample(ASK_MAP, -1)
		if reply.TaskType == FINISH {
			return
		}

		// Map task
		if reply.TaskType == MAP_TASK {
			//
			// read each input file,
			// pass it to Map,
			// Save Map output to mr-X.
			//
			n := reply.NReduce
			buckets := make([][]KeyValue, n)
			for i := range buckets {
				buckets[i] = make([]KeyValue, 0)
			}
			filename := reply.Task

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			for _, kv := range kva {
				i := ihash(kv.Key) % n
				buckets[i] = append(buckets[i], kv)
			}

			// TODO: Use temp files to avoid crash when writting
			for i, bucket := range buckets {
				// Save results of MAP task to mr-X-Y
				filename := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
				file, err = os.CreateTemp("/tmp/", filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				enc := json.NewEncoder(file)
				for _, kv := range bucket {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode %v to json", kv)
					}
				}
				file.Close()
				os.Rename(file.Name(), filename)
			}
			// Send filepath back to coordiantor
			reply = CallExample(ASK_REDUCE, reply.TaskID)
		}

		if reply.TaskType == REDUCE_TASK {

			kva := []KeyValue{}
			for i := range reply.NMap {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-X.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			reply = CallExample(FINISH, reply.TaskID)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func CallExample(query_id, TaskID int) ExampleReply {

	// declare an argument structure.
	args := ExampleArgs{query_id, TaskID}

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		if query_id == ASK_MAP {
			fmt.Printf("Worker ask for map task %v\n", reply.TaskID)
		} else if query_id == ASK_REDUCE {
			fmt.Printf("Worker ask for reduce task %v\n", reply.TaskID)
		} else {
			fmt.Printf("Worker finished reduce task %v\n", args.TaskID)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
