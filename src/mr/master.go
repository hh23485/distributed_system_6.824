package mr

import (
	"fmt"
	"github.com/satori/go.uuid"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type JobType int32

const JOB_TYPE_MAP JobType = 1
const JOB_TYPE_REDUCE JobType = 2

type JobStatusType int32

const JOB_STATUS_QUEUED JobStatusType = 1
const JOB_STATUS_FINISHED JobStatusType = 2
const JOB_STATUS_FAIL JobStatusType = 3
const JOB_STATUS_TIMEOUT JobStatusType = 4
const JOB_STATUS_RUNNING JobStatusType = 5

type WorkerStatus struct {
}

type Job struct {
	M         int
	R         int
	status    JobStatusType
	jobType   JobType
	startTime time.Time
	endTime   time.Time
	source    []string
	id        string
}

type Master struct {
	JobAssigner     *JobAssigner
	mutex           sync.Mutex
	partitionNumber int

	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.R = args.M + 1
//	return nil
//}

func (m *Master) AcquireJob(req AcquireRequest, resp *AcquireResponse) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fmt.Printf("Master: Request from work to get jobs\n")

	// if finished, return nil
	if m.JobAssigner.DoneLocked() {
		resp.Finished = true
		log.Printf("Master: all job finished")
		return nil
	}

	var job *Job
	if m.JobAssigner.MapJobDoneLocked() {
		job = m.JobAssigner.FetchQueuedReduceJobLocked()
		resp.JobType = JOB_TYPE_REDUCE
	} else {
		job = m.JobAssigner.FetchQueuedMapJobLocked()
		resp.JobType = JOB_TYPE_MAP
	}
	log.Printf("rest job numbers: %v", m.JobAssigner.GetRestQueuedMapSize())
	if job != nil {
		fmt.Printf("Master: Get job from assigner, jobId=%v, jobType=%v, jobStatus=%v\n", job.id, job.jobType, job.status)
		resp.JobId = job.id
		resp.Source = job.source
		resp.PartitionNumber = m.partitionNumber
		resp.M = job.M
		resp.Finished = false
		resp.Valid = true
		if job.jobType == JOB_TYPE_REDUCE {
			resp.R = job.R
		}
		// mark start
		m.JobAssigner.MarkJobStartedLocked(job)
		return nil
	} else {
		log.Printf("failed to fetch job, rest job count? %v", m.JobAssigner.GetRestSizeLocked())
		return fmt.Errorf("failed to fetch job, try it again")
	}
}

func (m *Master) FinishJob(req FinishRequest, resp *FinishResponse) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.JobAssigner.MarkJobFinishedLocked(req.JobType, req.JobId, req.Output)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	fmt.Printf("master begin to run...\n")
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.JobAssigner == nil {
		return false
	}
	return m.JobAssigner.DoneLocked()
}

func (m *Master) checkTask() {
	// load
	go func() {
		for true {
			time.Sleep(1 * time.Second)
			m.JobAssigner.ReScheduleTimeoutJobsLocked()
		}
	}()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	// create master
	m := Master{
		partitionNumber: nReduce,
	}
	jobAssigner := &JobAssigner{}

	// initialize job assigner container
	jobAssigner.Initialize()

	// create map jobs
	for index, file := range files {
		jobId, err := uuid.NewV4()
		if err != nil {
			fmt.Printf("Generate job id went wrong: %s\n", err)
			return nil
		}
		jobAssigner.AddNewMapJob(&Job{
			jobType: JOB_TYPE_MAP,
			status:  JOB_STATUS_QUEUED,
			source:  []string{file},
			id:      jobId.String(),
			M:       index,
			R:       -1,
		})
	}

	// construct master
	m.JobAssigner = jobAssigner

	fmt.Printf("Create master")
	m.checkTask()
	m.server()
	return &m
}
