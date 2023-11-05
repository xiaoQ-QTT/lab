package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type IDArgs struct {
	Id string
}

type StatusReply struct {
	Status int // 0失败、1成功
}

type GetTaskReply struct {
	Status       int         // 0没有，1为有
	TaskType     string      // map、reduce、over
	FileName     interface{} // 待处理文件名，map阶段为string类型，reduce阶段为[]string类型
	ReduceNumber int         // reduce任务数
	TaskKey      int         // map任务的key
}

type MapTaskOverArgs struct {
	Id         string
	FileNames  []string
	MapTaskKey int
}

type ReduceTaskOverArgs struct {
	Id            string
	ReduceTaskKey int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
