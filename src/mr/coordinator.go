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

	uuid "github.com/satori/go.uuid"
)

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

type Coordinator struct {
	// Your definitions here.
	JobAssigner     *JobAssigner
	mutex           sync.Mutex
	partitionNumber int
}

func (m *Coordinator) AcquireJob(req AcquireRequest, resp *AcquireResponse) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fmt.Printf("Coordinator: Request from work to get jobs\n")

	// if finished, return nil
	if m.JobAssigner.DoneLocked() {
		resp.Finished = true
		log.Printf("Coordinator: all job finished")
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
		fmt.Printf("Coordinator: Get job from assigner, jobId=%v, jobType=%v, jobStatus=%v\n", job.id, job.jobType, job.status)
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

func (m *Coordinator) FinishJob(req FinishRequest, resp *FinishResponse) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.JobAssigner.MarkJobFinishedLocked(req.JobType, req.JobId, req.Output)
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	fmt.Printf("master begin to run...\n")
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Coordinator) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.JobAssigner == nil {
		return false
	}
	return m.JobAssigner.DoneLocked()
}

func (m *Coordinator) checkTask() {
	// load
	go func() {
		for true {
			time.Sleep(1 * time.Second)
			m.JobAssigner.ReScheduleTimeoutJobsLocked()
		}
	}()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		partitionNumber: nReduce,
	}
	jobAssigner := &JobAssigner{}

	// initialize job assigner container
	jobAssigner.Initialize()

	// create map jobs
	for index, file := range files {
		jobId := uuid.NewV4()
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
