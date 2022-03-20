package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type JobState int

const (
	Mapping JobState = iota
	Reducing
	Done
)

type Coordinator struct {
	State        JobState      // 当前 job 状态
	NReduce      int           //
	MapTasks     []*MapTask    // 传址 map task
	ReduceTasks  []*ReduceTask // 传址 reduce task
	MappedTaskId map[int]struct{}
	MaxTaskId    int
	Mutex        sync.Mutex
	WorkerCount  int
	ExcitedCount int
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.State == Done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:      nReduce,
		MaxTaskId:    0,
		MappedTaskId: make(map[int]struct{}),
	}

	// 初始化 map 任务
	for _, f := range files {
		c.MapTasks = append(c.MapTasks,
			&MapTask{TaskMeta: TaskMeta{State: Pending}, Filename: f})
	}

	// 初始化 task 任务
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks,
			&ReduceTask{TaskMeta: TaskMeta{State: Pending, Id: i}})
	}

	// 表明 map 阶段
	c.State = Mapping
	c.server()
	return &c
}

func (c *Coordinator) Finish(args *FinishArgs, _ *Placeholder) error {
	if args.IsMap {
		// 遍历所有的 map 任务
		for _, task := range c.MapTasks {
			if task.Id == args.Id {
				task.State = Finished
				//log.Printf("finished task %d, total %d", task.Id, len(c.MapTasks))
				c.MappedTaskId[task.Id] = struct{}{}
				break
			}
		}

		// 判断是否存在还有未完成的 Map 任务
		// 一旦发现没有完成的立即退出
		for _, t := range c.MapTasks {
			if t.State != Finished {
				return nil
			}
		}
		// 全部完成，转入 Reduce 阶段
		c.State = Reducing
	} else {
		// 处理 Reduce 任务
		for _, task := range c.ReduceTasks {
			//识别到该任务的 id 修改状态
			if task.Id == args.Id {
				task.State = Finished
				break
			}
		}
		// 查看剩余任务是否全部完成
		for _, t := range c.ReduceTasks {
			if t.State != Finished {
				return nil
			}
		}
		// 所有任务都已经完成
		c.State = Done
	}
	return nil
}

const TIMEOUT = 10 * time.Second

func (c *Coordinator) RequestTask(_ *Placeholder, reply *Task) error {
	reply.Operation = ToWait

	if c.State == Mapping {
		for _, task := range c.MapTasks {
			now := time.Now()
			c.Mutex.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = now
				task.State = Executing
				c.MaxTaskId++
				task.Id = c.MaxTaskId
				c.Mutex.Unlock()
				//log.Printf("assigned map task %d %s", task.Id, task.Filename)

				reply.Operation = ToRun
				reply.IsMap = true
				reply.NReduce = c.NReduce
				reply.Map = *task
				return nil
			}
			c.Mutex.Unlock()
		}
	} else if c.State == Reducing {
		for _, task := range c.ReduceTasks {
			now := time.Now()
			c.Mutex.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			if task.State == Pending {
				c.Mutex.Unlock()
				task.StartTime = now
				task.State = Executing
				task.IntermediateFilenames = nil
				for id := range c.MappedTaskId {
					task.IntermediateFilenames = append(task.IntermediateFilenames, fmt.Sprintf("mr-%d-%d", id, task.Id))
				}
				//log.Printf("assigned reduce task %d", task.Id)

				reply.Operation = ToRun
				reply.IsMap = false
				reply.NReduce = c.NReduce
				reply.Reduce = *task
				return nil
			}
			c.Mutex.Unlock()
		}
	}
	return nil
}
