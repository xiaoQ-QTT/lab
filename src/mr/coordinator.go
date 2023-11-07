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

type Task struct {
	Key          int
	Status       TaskStatus
	FileName     interface{} // 数据地址
	AllocateTime time.Time   // 任务的分配时间
}

// 如果使用更细粒度的锁，比如为每个变量都设置一个锁，这样会麻烦很多吗，而且容易错问题！
type Coordinator struct {
	mu sync.Mutex

	// map任务队列
	MapTaskList []Task

	// map任务完成数
	mapTaskOverNum int

	// reduce任务队列
	ReduceTaskList []Task

	// reducer任务完成数
	ReduceTaskOverNum int

	// 当前处理阶段：map，reduce，over
	Stage TaskType

	// map任务产生的中间文件
	MiddleFileNames [][]string
}

func (c *Coordinator) AssignLeisureTask(args *RequestTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 处于那个阶段
	if c.Stage != OVER {
		var taskList []Task
		if c.Stage == REDUCE {
			taskList = c.ReduceTaskList
		} else {
			taskList = c.MapTaskList
		}
		// 安排空闲任务
		for i := range taskList {
			if taskList[i].Status == IDLE { // 有空闲的
				// 更新该task状态和安排时间，将该任务安排给这个worker
				taskList[i].Status = PROCESSING
				taskList[i].AllocateTime = time.Now()

				reply.TaskKey = taskList[i].Key
				reply.ReduceNumber = len(c.ReduceTaskList)
				reply.TaskType = c.Stage
				reply.FileName = taskList[i].FileName
				return nil
			}
		}
		// 没有空闲任务
		reply.TaskType = SLEEP
	} else { // over
		reply.TaskType = OVER
	}
	return nil
}

// reduce任务结束
func (c *Coordinator) ReduceTaskOver(args *ReduceTaskOverArgs, reply *TaskOverReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查该任务是否已经完成
	if c.ReduceTaskList[args.ReduceTaskKey].Status == FINISH {
		fmt.Printf("该工作已经完成：这是可能是因为前一个负责该任务的worker超时被标记为离线了，但是它后面又先完成任务了")
		return nil
	}

	// 任务设置为完成
	c.ReduceTaskList[args.ReduceTaskKey].Status = FINISH

	// reduce完成数加一
	c.ReduceTaskOverNum++

	// reduce任务是否全部完成
	if c.ReduceTaskOverNum == len(c.ReduceTaskList) {
		c.Stage = OVER
	}

	return nil
}

// MapTaskOver map任务结束
func (c *Coordinator) MapTaskOver(args *MapTaskOverArgs, reply *TaskOverReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查该任务是否已经完成
	if c.MapTaskList[args.MapTaskKey].Status == FINISH {
		fmt.Printf("该工作已经完成：这是可能是因为前一个负责该任务的worker超时被标记为离线了，但是它后面又先完成任务了")
		return nil
	}

	// 保存任务中间文件路径，这个在 任务设置为完成 前面，这样即使函数中间崩溃也没有问题！！！
	for key, value := range args.FileNameMap {
		c.ReduceTaskList[key].FileName = append(c.ReduceTaskList[key].FileName.([]string), value)
	}

	// 任务设置为完成
	c.MapTaskList[args.MapTaskKey].Status = FINISH

	c.mapTaskOverNum++
	// map任务是否全部完成
	if c.mapTaskOverNum == len(c.MapTaskList) {
		c.Stage = REDUCE
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := false
	if c.Stage == OVER {
		ret = true
	}
	return ret
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// 1. 初始化
	c.MapTaskList = make([]Task, len(files))
	c.MiddleFileNames = make([][]string, nReduce)
	c.ReduceTaskList = make([]Task, nReduce)
	c.Stage = MAP
	// map任务初始化
	for index, filename := range files {
		c.MapTaskList[index] = Task{
			Key:      index,
			Status:   IDLE,
			FileName: filename,
		}
	}
	// reduce任务初始化
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskList[i] = Task{
			Key:      i,
			Status:   IDLE,
			FileName: make([]string, 0),
		}
	}

	// 2.定时检查任务的安排的时间是否超过10秒
	go func() {
		for {
			c.mu.Lock()
			switch c.Stage {
			case MAP:
				for i := range c.MapTaskList {
					if c.MapTaskList[i].Status == PROCESSING && time.Now().Sub(c.MapTaskList[i].AllocateTime) > 10*time.Second {
						c.MapTaskList[i].Status = IDLE
					}
				}
			case REDUCE:
				for i := range c.ReduceTaskList {
					if c.ReduceTaskList[i].Status == PROCESSING && time.Now().Sub(c.ReduceTaskList[i].AllocateTime) > 10*time.Second {
						c.ReduceTaskList[i].Status = IDLE
					}
				}
			}
			c.mu.Unlock()
			time.Sleep(1 * time.Second) // 定期检查间隔3秒
		}
	}()

	c.server()
	return &c
}
