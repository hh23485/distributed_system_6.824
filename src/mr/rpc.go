package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type MasterDoneCheckRequest struct {}
type MasterDoneCheckResponse struct {}

type AcquireRequest struct {
}

type AcquireResponse struct {
	Source          []string
	JobId           string
	JobType         JobType
	M               int
	R               int
	Err             error
	Finished        bool
	Valid        	bool
	PartitionNumber int
}

type FinishRequest struct {
	JobId   string
	JobType JobType
	Output  []string
}

type FinishResponse struct {
	Err error
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
