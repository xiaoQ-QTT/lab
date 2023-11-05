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

type Slaver struct {
	Id              string
	Status          int       // 0空闲，1工作中，2故障
	lastConnectTime time.Time // slaver上次联系时间
}

type Task struct {
	Key          int
	Status       int         // 0空闲，1处理中，2完成
	OwenSlaverId string      // 如果已被分配，处理该任务的workerId
	FileName     interface{} // 数据地址
}

type Coordinator struct {
	// workers信息
	SlaversMutex sync.Mutex
	Slavers      map[string]Slaver

	// map任务队列
	MapTaskListMutex sync.Mutex
	MapTaskList      []Task

	// map任务完成数
	mapTaskOverNumMutex sync.Mutex
	mapTaskOverNum      int

	// reduce任务队列
	ReduceTaskListMutex sync.Mutex
	ReduceTaskList      []Task

	// reducer任务完成数
	ReduceTaskOverNumMutex sync.Mutex
	ReduceTaskOverNum      int

	// 当前处理阶段：map，reduce，over
	StageMutex sync.Mutex
	Stage      string

	// map任务产生的中间文件
	MiddleFileNamesMutex sync.Mutex
	MiddleFileNames      [][]string
}

// PrintCoordinatorData 打印数据
/*
func PrintCoordinatorData(c *Coordinator) {
	fmt.Println("Slavers:")
	c.SlaversMutex.Lock()
	for id, slaver := range c.Slavers {
		fmt.Printf("ID: %s, Status: %d\n", id, slaver.Status)
	}
	c.SlaversMutex.Unlock()

	fmt.Println("MapTaskList:")
	c.MapTaskListMutex.Lock()
	for i := 0; i < len(c.MapTaskList); i++ {
		fmt.Printf("ID: %d, OwenSlaverId: %s, Status: %d, fileName: %s\n", (&c.MapTaskList[i]).Key, (&c.MapTaskList[i]).OwenSlaverId, (&c.MapTaskList[i]).Status, (&c.MapTaskList[i]).FileName.(string))
	}
	defer c.MapTaskListMutex.Unlock()

	c.mapTaskOverNumMutex.Lock()
	fmt.Printf("mapTaskOverNum: %d\n", c.mapTaskOverNum)
	c.mapTaskOverNumMutex.Unlock()

	fmt.Println("ReduceTaskList:")
	c.ReduceTaskListMutex.Lock()
	for _, task := range c.ReduceTaskList {
		if task.FileName != nil {
			fmt.Printf("ID: %d, OwenSlaverId: %s, Status: %d, fileName: %s\n", task.Key, task.OwenSlaverId, task.Status, task.FileName.([]string))
		}
	}
	c.ReduceTaskListMutex.Unlock()

	c.ReduceTaskOverNumMutex.Lock()
	fmt.Printf("reduceTaskOverNum: %d\n", c.ReduceTaskOverNum)
	c.ReduceTaskOverNumMutex.Unlock()

	fmt.Printf("Stage: %s\n", c.Stage)

	fmt.Println("MiddleFileNames:")

	c.MiddleFileNamesMutex.Lock()
	for _, fileNames := range c.MiddleFileNames {
		fmt.Println(fileNames)
	}
	c.MiddleFileNamesMutex.Unlock()
}*/

func (c *Coordinator) AssignLeisureTask(args *IDArgs, reply *GetTaskReply) error {
	// 是否是未记录的worker
	c.SlaversMutex.Lock()
	if _, ok := c.Slavers[args.Id]; !ok {
		c.Slavers[args.Id] = Slaver{
			args.Id, 0, time.Now(),
		}
	}
	c.SlaversMutex.Unlock()
	// 更新该worker的时间
	c.UpdateLastTime(args.Id)

	// 处于那个阶段
	c.StageMutex.Lock()
	defer c.StageMutex.Unlock()
	if c.Stage != "over" { // map阶段或者reduce阶段
		// 是否有空闲任务
		var taskList []Task
		if c.Stage == "reduce" {
			c.ReduceTaskListMutex.Lock()
			defer c.ReduceTaskListMutex.Unlock()
			taskList = c.ReduceTaskList
		} else {
			c.MapTaskListMutex.Lock()
			defer c.MapTaskListMutex.Unlock()
			taskList = c.MapTaskList
		}

		for i := range taskList {
			if taskList[i].Status == 0 { // 有空闲的
				// 更新该task状态，将该任务安排给这个worker
				taskList[i].Status = 1
				taskList[i].OwenSlaverId = args.Id

				reply.TaskKey = taskList[i].Key
				reply.Status = 1
				reply.ReduceNumber = len(c.ReduceTaskList)
				reply.TaskType = c.Stage
				reply.FileName = taskList[i].FileName

				return nil
			}
		}
		// 没有空闲任务
		reply.Status = 0
	} else { // over
		reply.TaskType = "over"
	}
	return nil
}

func (c *Coordinator) UpdateLastTime(workerId string) {
	c.SlaversMutex.Lock()
	slaver := c.Slavers[workerId]
	slaver.lastConnectTime = time.Now()
	c.SlaversMutex.Unlock()
}

// ReduceTaskOver reduce任务结束
func (c *Coordinator) ReduceTaskOver(args *ReduceTaskOverArgs, reply *StatusReply) error {

	// 检查该任务是否已经完成
	c.ReduceTaskListMutex.Lock()
	for i := range c.ReduceTaskList {
		if c.ReduceTaskList[i].Key == args.ReduceTaskKey && c.ReduceTaskList[i].Status == 2 {
			fmt.Printf("该工作已经完成：这是可能是因为前一个负责该任务的worker超时被标记为离线了，但是它后面又先完成任务了")
			return nil
		}
	}
	c.ReduceTaskListMutex.Unlock()
	// 更新时间
	c.UpdateLastTime(args.Id)

	// worker设置为空闲，不过后续用不到
	c.SlaversMutex.Lock()
	if _, ok := c.Slavers[args.Id]; ok {
		map1 := c.Slavers[args.Id]
		map1.Status = 1
	}
	c.SlaversMutex.Unlock()

	// 任务设置为完成
	c.ReduceTaskListMutex.Lock()
	for i := range c.ReduceTaskList {
		if c.ReduceTaskList[i].OwenSlaverId == args.Id && c.ReduceTaskList[i].Key == args.ReduceTaskKey {
			c.ReduceTaskList[i].Status = 2
		}
	}
	c.ReduceTaskListMutex.Unlock()

	// reduce完成数加一
	c.ReduceTaskOverNumMutex.Lock()
	c.ReduceTaskOverNum++

	// reduce任务是否全部完成
	c.StageMutex.Lock()
	if c.ReduceTaskOverNum == len(c.ReduceTaskList) {
		c.Stage = "over"
	}
	c.StageMutex.Unlock()
	c.ReduceTaskOverNumMutex.Unlock()
	reply.Status = 1

	// PrintCoordinatorData(c)
	return nil
}

// MapTaskOver map任务结束
func (c *Coordinator) MapTaskOver(args *MapTaskOverArgs, reply *StatusReply) error {
	// 检查该任务是否已经完成
	c.MapTaskListMutex.Lock()
	for i := range c.MapTaskList {
		if c.MapTaskList[i].Key == args.MapTaskKey && c.MapTaskList[i].Status == 2 {
			fmt.Printf("该工作已经完成：这是可能是因为前一个负责该任务的worker超时被标记为离线了，但是它后面又先完成任务了")
			return nil
		}
	}
	c.MapTaskListMutex.Unlock()

	// 更新时间
	c.UpdateLastTime(args.Id)

	// 保存该map任务产生的中间
	c.MiddleFileNamesMutex.Lock()
	for key, value := range args.FileNames {
		c.MiddleFileNames[key] = append(c.MiddleFileNames[key], value)
	}
	c.MiddleFileNamesMutex.Unlock()

	// worker设置为空闲
	c.SlaversMutex.Lock()
	if _, ok := c.Slavers[args.Id]; ok {
		map1 := c.Slavers[args.Id]
		map1.Status = 1
	}
	c.SlaversMutex.Unlock()

	// 任务设置为完成
	c.MapTaskListMutex.Lock()
	for i := range c.MapTaskList {
		if c.MapTaskList[i].OwenSlaverId == args.Id && c.MapTaskList[i].Key == args.MapTaskKey {
			c.MapTaskList[i].Status = 2
		}
	}
	c.MapTaskListMutex.Unlock()

	c.mapTaskOverNumMutex.Lock()
	c.mapTaskOverNum++
	// map任务是否全部完成
	if c.mapTaskOverNum == len(c.MapTaskList) {
		c.StageMutex.Lock()
		c.Stage = "reduce"
		c.StageMutex.Unlock()
		// 初始化reduceTaskList
		c.ReduceTaskListMutex.Lock()
		c.MiddleFileNamesMutex.Lock()
		for key, fileNameList := range c.MiddleFileNames {
			c.ReduceTaskList[key].Key = key
			c.ReduceTaskList[key].Status = 0
			c.ReduceTaskList[key].FileName = fileNameList
		}
		c.MiddleFileNamesMutex.Unlock()
		c.ReduceTaskListMutex.Unlock()
	}
	c.mapTaskOverNumMutex.Unlock()

	// PrintCoordinatorData(c)
	reply.Status = 1
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
	ret := false

	c.StageMutex.Lock()
	if c.Stage == "over" {
		ret = true
	}
	c.StageMutex.Unlock()
	return ret
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// 1. 初始化
	c.Slavers = make(map[string]Slaver)
	c.MapTaskList = make([]Task, len(files))
	c.mapTaskOverNum = 0
	c.MiddleFileNames = make([][]string, nReduce)
	c.ReduceTaskList = make([]Task, nReduce)
	c.ReduceTaskOverNum = 0
	c.Stage = "map"
	index := 0
	// map任务初始化
	for _, filename := range files {
		c.MapTaskList[index].Key = index
		c.MapTaskList[index].Status = 0
		c.MapTaskList[index].FileName = filename
		index++
	}
	c.server()

	// 2.定时检查worker状态
	go func() {
		for {
			c.SlaversMutex.Lock()
			for j := range c.Slavers {
				elapsed := time.Since(c.Slavers[j].lastConnectTime)
				if elapsed > 10*time.Second {
					// 将该slaver设置为故障状态，并将其处理的任务设置为空闲
					temp := c.Slavers[j]
					temp.Status = 2
					if c.Stage == "map" {
						c.MapTaskListMutex.Lock()
						for i := range c.MapTaskList {
							if c.MapTaskList[i].OwenSlaverId == c.Slavers[j].Id && c.MapTaskList[i].Status == 1 {
								c.MapTaskList[i].Status = 0 // 将该worker负责的map任务的状态设置为空闲
							}
						}
						c.MapTaskListMutex.Unlock()
					} else {
						c.ReduceTaskListMutex.Lock()
						for i := range c.ReduceTaskList {
							if c.ReduceTaskList[i].OwenSlaverId == c.Slavers[j].Id && c.ReduceTaskList[i].Status == 1 {
								c.ReduceTaskList[i].Status = 0 // 将该worker负责的reduce任务的状态设置为空闲
							}
						}
						c.ReduceTaskListMutex.Unlock()
					}
				}
			}
			c.SlaversMutex.Unlock()
			time.Sleep(3 * time.Second) // 定期检查间隔3秒
		}
	}()

	return &c
}
