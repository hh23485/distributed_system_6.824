package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

type JobAssigner struct {
	Jobs                  map[JobType]map[JobStatusType]map[string]*Job
	PartitionedReduceJobs map[int]*Job
	mutex                 sync.Mutex
}

func (m *JobAssigner) Initialize() {
	m.Jobs = map[JobType]map[JobStatusType]map[string]*Job{
		JOB_TYPE_MAP: {
			JOB_STATUS_QUEUED:   make(map[string]*Job),
			JOB_STATUS_RUNNING:  make(map[string]*Job),
			JOB_STATUS_FINISHED: make(map[string]*Job),
		},
		JOB_TYPE_REDUCE: {
			JOB_STATUS_QUEUED:   make(map[string]*Job),
			JOB_STATUS_RUNNING:  make(map[string]*Job),
			JOB_STATUS_FINISHED: make(map[string]*Job),
		},
	}
	m.PartitionedReduceJobs = map[int]*Job{}
}

func (m *JobAssigner) GetRestSizeLocked() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.GetRestSize()
}

func (m *JobAssigner) GetRestSize() int {
	return len(m.Jobs[JOB_TYPE_MAP][JOB_STATUS_QUEUED]) + len(m.Jobs[JOB_TYPE_MAP][JOB_STATUS_RUNNING]) + len(m.Jobs[JOB_TYPE_REDUCE][JOB_STATUS_RUNNING]) + len(m.Jobs[JOB_TYPE_REDUCE][JOB_STATUS_QUEUED])
}

func (m *JobAssigner) GetRestQueuedMapSize() int {
	return len(m.Jobs[JOB_TYPE_MAP][JOB_STATUS_QUEUED])
}

func (m *JobAssigner) FetchQueuedMapJobLocked() *Job {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.FetchQueuedMapJob()
}

func (m *JobAssigner) FetchQueuedMapJob() *Job {
	return m.fetchQueueJob(JOB_TYPE_MAP)
}

func (m *JobAssigner) FetchQueuedReduceJobLocked() *Job {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.FetchQueuedReduceJob()
}

func (m *JobAssigner) FetchQueuedReduceJob() *Job {
	if !m.MapJobDone() {
		return nil
	}
	return m.fetchQueueJob(JOB_TYPE_REDUCE)
}

// thread un-safe
func (m *JobAssigner) getFirstJob(jobType JobType, statusType JobStatusType) *Job {
	typedJobs := m.Jobs[jobType]
	jobsInStatus := typedJobs[statusType]
	if len(jobsInStatus) == 0 {
		return nil
	}
	var firstJob *Job
	for _, job := range jobsInStatus {
		firstJob = job
	}
	return firstJob
}

func (m *JobAssigner) removeFromJobList(jobType JobType, statusType JobStatusType, jobId string) (job *Job, err error) {
	typedJobs := m.Jobs[jobType]
	jobsInStatus := typedJobs[statusType]
	if len(jobsInStatus) == 0 {
		err = fmt.Errorf("empty job list %v, %v", jobType, statusType)
		return
	}
	if _, ok := jobsInStatus[jobId]; ok {
		job = jobsInStatus[jobId]
		delete(jobsInStatus, jobId)
		return
	}
	err = fmt.Errorf("can not find jobId in job list %v, %v", jobType, statusType)
	return
}

func (m *JobAssigner) addToJobList(jobType JobType, statusType JobStatusType, jobId string, job *Job) error {
	typedJobs := m.Jobs[jobType]
	jobsInStatus := typedJobs[statusType]
	//if len(jobsInStatus) == 0 {
	//	return fmt.Errorf("empty job list %v, %v", jobType, statusType)
	//}
	if _, ok := jobsInStatus[jobId]; ok {
		return fmt.Errorf("jobId %v, has already in job list %v, %v", jobId, jobType, statusType)
	}
	jobsInStatus[jobId] = job
	return nil
}

func (m *JobAssigner) fetchQueueJob(jobType JobType) *Job {
	// get specific type job list
	firstJob := m.getFirstJob(jobType, JOB_STATUS_QUEUED)
	if firstJob == nil {
		return nil
	}
	// remove from job set
	m.removeFromJobList(jobType, JOB_STATUS_QUEUED, firstJob.id)

	return firstJob
}

func (m *JobAssigner) MarkJobFinishedLocked(jobType JobType, jobId string, output []string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.MarkJobFinished(jobType, jobId, output)
}

func (m *JobAssigner) MarkJobFinished(jobType JobType, jobId string, output []string) error {
	log.Printf("Master: mark job finished, jobId=%v", jobId)
	//if len(jobId) == 0 {
	//	log.Printf("un expected job id")
	//	return nil
	//}
	job, err := m.removeFromJobList(jobType, JOB_STATUS_RUNNING, jobId)
	if job == nil {
		log.Printf("Get timeout result, ignore, jobId: %v", jobId)
		return nil
	}
	job.endTime = time.Now()
	if err != nil {
		fmt.Printf("mark job finished failed, error=%v\n", err)
		return err
	}
	err = m.addToJobList(jobType, JOB_STATUS_FINISHED, jobId, job)
	if err != nil {
		fmt.Printf("mark job finished failed, error=%v\n", err)
		return err
	}

	if job.jobType == JOB_TYPE_MAP {
		m.collectMapOutput(job, output)
		if m.MapJobDone() {
			for _, job := range m.PartitionedReduceJobs {
				m.addToJobList(JOB_TYPE_REDUCE, JOB_STATUS_QUEUED, job.id, job)
			}
		}
	}

	return nil
}

func (m *JobAssigner) MarkJobStartedLocked(job *Job) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	job.startTime = time.Now()
	m.addToJobList(job.jobType, JOB_STATUS_RUNNING, job.id, job)
	return nil
}

func (m *JobAssigner) DoneLocked() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.GetRestSize() == 0
}

func (m *JobAssigner) MapJobDoneLocked() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.MapJobDone()
}

func (m *JobAssigner) MapJobDone() bool {
	return len(m.Jobs[JOB_TYPE_MAP][JOB_STATUS_QUEUED])+len(m.Jobs[JOB_TYPE_MAP][JOB_STATUS_RUNNING]) == 0
}

func (m *JobAssigner) AddNewMapJob(job *Job) {
	m.Jobs[JOB_TYPE_MAP][JOB_STATUS_QUEUED][job.id] = job
}

func (m *JobAssigner) AddNewReduceJob(job *Job) {
	m.Jobs[JOB_TYPE_REDUCE][JOB_STATUS_QUEUED][job.id] = job
}

// thread un-safe
func (m *JobAssigner) collectMapOutput(job *Job, output []string) {
	for _, file := range output {
		token := strings.Split(file, "-")
		partition, _ := strconv.Atoi(token[2])
		if m.PartitionedReduceJobs[partition] == nil {
			jobId := uuid.NewV4()
			m.PartitionedReduceJobs[partition] = &Job{
				status:  JOB_STATUS_QUEUED,
				jobType: JOB_TYPE_REDUCE,
				source:  []string{},
				id:      jobId.String(),
				M:       -1,
				R:       partition,
			}
		}
		m.PartitionedReduceJobs[partition].source = append(m.PartitionedReduceJobs[partition].source, file)
	}
}

func (m *JobAssigner) ReScheduleTimeoutJobsLocked() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	log.Printf("begin to check out of date jobs")
	// get all running task
	for jobId, theJob := range m.Jobs[JOB_TYPE_MAP][JOB_STATUS_RUNNING] {
		if time.Now().Sub(theJob.startTime) < time.Second*5 {
			continue
		}
		job, err := m.removeFromJobList(JOB_TYPE_MAP, JOB_STATUS_RUNNING, jobId)
		if err != nil {
			log.Printf("remove job from running list failed, jobId=%v, err=%v", jobId, err)
			return err
		}
		err = m.addToJobList(JOB_TYPE_MAP, JOB_STATUS_QUEUED, jobId, job)
		if err != nil {
			log.Printf("add job to queue list failed, jobId=%v, err=%v", jobId, err)
			return err
		}
	}
	// check if running out of 10s
	for jobId, theJob := range m.Jobs[JOB_TYPE_REDUCE][JOB_STATUS_RUNNING] {
		if time.Now().Sub(theJob.startTime) < time.Second*5 {
			continue
		}
		job, err := m.removeFromJobList(JOB_TYPE_REDUCE, JOB_STATUS_RUNNING, jobId)
		if err != nil {
			log.Printf("remove job from running list failed, jobId=%v, err=%v", jobId, err)
			return err
		}
		err = m.addToJobList(JOB_TYPE_REDUCE, JOB_STATUS_QUEUED, jobId, job)
		if err != nil {
			log.Printf("add job to queue list failed, jobId=%v, err=%v", jobId, err)
			return err
		}
	}
	return nil
	// assign back to queued
}
