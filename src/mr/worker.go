package mr

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 生成Id，fileNames保存map任务产生的文件路径（这里就是文件名）
	var fileNames []map[int]string
	// worker结束之前应该 删除中间文件
	// deleteFiles(fileNames)

	id, err := generateUUID()
	if err != nil {
		log.Fatal(fmt.Println("Failed to generate UUID:", err))
	}
	// 循环作业
	for {
		// 请求任务
		reply := GetLeisureTask(id)

		switch reply.TaskType {
		case OVER:
			return
		case MAP:
			doMapTask(id, &reply, fileNames, mapf)
		case REDUCE:
			doReduceTask(id, &reply, reducef)
		case SLEEP:
			// 没有空闲任务，休眠0.45秒
			time.Sleep(time.Millisecond * 450)
		}

	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// generateUUId
func generateUUID() (string, error) {
	// 获取当前时间戳
	timestamp := time.Now().UnixNano()

	// 生成一个包含 8 个随机字节的切片
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", err
	}

	// 组合时间戳和随机数生成 UUID
	uuidBytes := make([]byte, 16)
	copy(uuidBytes[:8], randomBytes)
	copy(uuidBytes[8:], fmt.Sprintf("%016x", timestamp))

	// 将字节切片转换为字符串表示形式
	uuidStr := hex.EncodeToString(uuidBytes)

	return uuidStr, nil
}

func deleteFiles(fileNames []map[int]string) error {
	for _, fileNameMap := range fileNames {
		for _, fileName := range fileNameMap {
			err := os.Remove(fileName)
			if err != nil {
				return fmt.Errorf("无法删除文件 %s: %v", fileName, err)
			}
			fmt.Printf("成功删除文件 %s\n", fileName)
		}
	}
	return nil
}

func doMapTask(id string, reply *GetTaskReply, fileNames []map[int]string, mapf func(string, string) []KeyValue) {
	// 从文件读入数据-->使用map函数处理
	file, err := os.Open(reply.FileName.(string))
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName.(string), string(content))

	// 将kv数据写入文件中
	tempNames := WriteFile(kva, reply.ReduceNumber, reply.TaskKey)
	fileNames = append(fileNames, tempNames) // 保存这个worker完成的所有任务的中间文件路径

	// 告诉master，完成任务，发送生成的中间文件位置
	mapTaskOver(id, tempNames, reply.TaskKey)
}

func doReduceTask(id string, reply *GetTaskReply, reducef func(string, []string) string) {
	// 从多个文件读入数据
	kva := make([]KeyValue, 0)
	for _, fileName := range reply.FileName.([]string) {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		// 读取k/v
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// 排序
	sort.Sort(ByKey(kva))

	// 处理kva，处理成reducef需要的数据，并使用reducef处理，使用临时文件
	fileName := "mr-out-" + strconv.Itoa(reply.TaskKey)
	tempFile, err := os.CreateTemp("", "temp")
	defer tempFile.Close()

	if err != nil {
		log.Fatalf("Error creating temporary file: %s", err)
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	err = os.Rename(tempFile.Name(), fileName)

	// 告诉master，reduce任务结束
	args := ReduceTaskOverArgs{id, reply.TaskKey}
	reply1 := TaskOverReply{}
	call("Coordinator.ReduceTaskOver", &args, &reply1)
}

func mapTaskOver(uuId string, fileNames map[int]string, key int) {
	args := MapTaskOverArgs{uuId, fileNames, key}
	reply := TaskOverReply{}
	call("Coordinator.MapTaskOver", &args, &reply)
}

// WriteFile 写入到文件中
func WriteFile(kva []KeyValue, reduceNumber int, mapTaskKey int) map[int]string {
	// 根据key，使用ihash方法将值分为10部分
	mapKva := make(map[int][]KeyValue)
	for _, kv := range kva {
		fileKey := ihash(kv.Key) % reduceNumber
		_, ok := mapKva[fileKey]
		if !ok {
			mapKva[fileKey] = []KeyValue{kv}
		} else {
			mapKva[fileKey] = append(mapKva[fileKey], kv)
		}
	}

	// 写入临时文件，成功后改名
	fileNames := make(map[int]string, reduceNumber) // 最终文件名
	for key, kvList := range mapKva {
		tempFile, err := os.CreateTemp("", "temp")
		if err != nil {
			log.Fatalf("Error creating temporary file: %s", err)
		}
		enc := json.NewEncoder(tempFile)

		for _, kv := range kvList {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Error creating temporary file: %s", err)
			}
		}
		fileNames[key] = fmt.Sprintf("mr-%d-%d", mapTaskKey, key)
		err = os.Rename(tempFile.Name(), fileNames[key])
		tempFile.Close()
	}
	return fileNames
}

// GetLeisureTask 询问是否有空闲的任务可分配
func GetLeisureTask(uuId string) GetTaskReply {
	reply := GetTaskReply{}
	args := RequestTaskArgs{}
	call("Coordinator.AssignLeisureTask", &args, &reply)
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
