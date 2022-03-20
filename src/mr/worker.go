package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.SetOutput(ioutil.Discard)
	for {
		time.Sleep(1 * time.Second)
		// 构建一个空的 task
		task := Task{}
		// 申请 task
		call("Coordinator.RequestTask", &Placeholder{}, &task)
		if task.Operation == ToWait {
			continue
		}

		if task.IsMap {
			//log.Printf("received map job %s", task.Map.Filename)
			err := handleMap(task, mapf)
			if err != nil {
				return
			}
		} else {
			//log.Printf("received reduce job %d %v", task.Reduce.Id, task.Reduce.IntermediateFilenames)
			err := handleReduce(task, reducef)
			if err != nil {
				return
			}
		}
	}
}

func handleReduce(task Task, reducef func(string, []string) string) error {
	var kva []KeyValue
	// 遍历所有的 Reduce 任务
	for _, filename := range task.Reduce.IntermediateFilenames {
		// 读取生成中间文件的内容
		iFile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(iFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err = iFile.Close()
		if err != nil {
			log.Fatalf("cannot close %v", filename)
		}
	}

	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", task.Reduce.Id)
	temp, err := os.CreateTemp(".", oname)
	if err != nil {
		log.Fatalf("cannot create reduce result tempfile %s", oname)
		return err
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, _ = fmt.Fprintf(temp, "%v %v\n", kva[i].Key, output)

		i = j
	}
	err = os.Rename(temp.Name(), oname)
	if err != nil {
		return err
	}

	call("Coordinator.Finish", &FinishArgs{IsMap: false, Id: task.Reduce.Id}, &Placeholder{})
	return nil
}

func handleMap(task Task, mapf func(string, string) []KeyValue) interface{} {
	filename := task.Map.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()

	// map 函数处理
	kva := mapf(filename, string(content))
	var encoders []*json.Encoder
	for i := 0; i < task.NReduce; i++ {
		f, err := os.Create(fmt.Sprintf("mr-%d-%d", task.Map.Id, i))
		if err != nil {
			log.Fatalf("cannot create intermediate result file")
		}
		encoders = append(encoders, json.NewEncoder(f))
	}

	for _, kv := range kva {
		_ = encoders[ihash(kv.Key)%task.NReduce].Encode(&kv)
	}

	call("Coordinator.Finish", &FinishArgs{IsMap: true, Id: task.Map.Id}, &Placeholder{})
	return nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
	os.Exit(0)
	return false
}
