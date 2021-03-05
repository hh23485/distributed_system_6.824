package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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

type ExecJob AcquireResponse

type ExecJobOutput FinishRequest

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// continue to fetch job
	// call finish job
	for {
		fmt.Printf("try to fetch jobs...\n")
		job, finished := FetchJobFromMaster()
		if finished || !job.Valid {
			fmt.Printf("worker exit...\n")
			return
		}

		if job == nil {
			continue
		}

		var jobResult *ExecJobOutput
		var err error
		if job.JobType == JOB_TYPE_MAP {
			if jobResult, err = ExecMapJob(mapf, job); err != nil {
				fmt.Printf("run map job error, jobId=%v, error=%v", job.JobId, err)
				continue
			}
			// notify finished
		} else {
			if jobResult, err = ExecReduceJob(reducef, job); err != nil {
				fmt.Printf("run reduce job error, jobId=%v, error=%v", job.JobId, err)
				continue
			}
		}

		MarkJobFinished(jobResult)
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func FetchJobFromMaster() (*ExecJob, bool) {

	// declare an argument structure.
	acqReq := AcquireRequest{}

	acqResp := AcquireResponse{}

	// send the RPC request, wait for the reply.
	call("Coordinator.AcquireJob", acqReq, &acqResp)

	if acqResp.Finished {
		return nil, true
	}

	//job := &ExecJob{
	//	Source:          acqResp.Source,
	//	JobId:           acqResp.JobId,
	//	JobType:         acqResp.JobType,
	//	M:               acqResp.M,
	//	R:               acqResp.R,
	//	Err:             nil,
	//	Finished:        acqResp.Finished,
	//	PartitionNumber: acqResp.PartitionNumber,
	//}

	fmt.Printf("get job from master %v\n", acqResp)

	return (*ExecJob)(&acqResp), false
}

func ExecMapJob(mapf func(string, string) []KeyValue, job *ExecJob) (*ExecJobOutput, error) {
	intermediate := map[int][]KeyValue{}
	M := job.M
	fileName := job.Source[0]
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v, err=%v", fileName, err)
		return nil, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v, err=%v", fileName, err)
		return nil, err
	}
	file.Close()

	kva := mapf(fileName, string(content))
	for _, kv := range kva {
		partition := ihash(kv.Key) % job.PartitionNumber
		if intermediate[partition] == nil {
			intermediate[partition] = []KeyValue{}
		}
		intermediate[partition] = append(intermediate[partition], kv)
	}

	jobResult := &ExecJobOutput{
		JobId:   job.JobId,
		JobType: job.JobType,
		Output:  []string{},
	}
	// write to M-X files
	for partition, kvList := range intermediate {
		// skip empty partitions
		if len(kvList) == 0 {
			continue
		}

		filename := fmt.Sprintf("mr-%d-%d", M, partition)
		// add result to output file list
		jobResult.Output = append(jobResult.Output, filename)

		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot open %v, err=%v", filename, err)
			return nil, err
		}
		for _, kv := range kvList {
			content := fmt.Sprintf("%v,%v\n", kv.Key, kv.Value)
			file.WriteString(content)
		}
		file.Sync()
		file.Close()
		abs, _ := filepath.Abs(fileName)
		log.Printf("Map Generated file, %s", abs)
	}
	return jobResult, nil
}

func ExecReduceJob(reducef func(string, []string) string, job *ExecJob) (*ExecJobOutput, error) {
	intermediate := []KeyValue{}
	for _, fileName := range job.Source {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open file, jobId=%v, file=%v", job.JobId, fileName)
			return nil, err
		}
		buf := bufio.NewReader(file)

		for {
			content, err := buf.ReadString('\n')
			content = strings.TrimSpace(content)
			if len(content) == 0 || err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("read intermediate result from file failed, jobId=%v, file=%v, error=%v", job.JobId, fileName, err)
				return nil, err
			}
			kv := strings.FieldsFunc(content, func(r rune) bool { return r == ',' })
			intermediate = append(intermediate, KeyValue{Key: kv[0], Value: kv[1]})
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", job.R)
	ofile, _ := os.Create(oname)

	jobResult := &ExecJobOutput{
		JobId:   job.JobId,
		JobType: job.JobType,
		Output:  []string{oname},
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	abs, _ := filepath.Abs(oname)
	log.Printf("Reduce Generated file, %s", abs)
	return jobResult, nil
}

func MarkJobFinished(jobResult *ExecJobOutput) error {
	resp := FinishResponse{}
	return call("Coordinator.FinishJob", jobResult, resp)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
