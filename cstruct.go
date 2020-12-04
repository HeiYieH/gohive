package gohive

import (
	"time"

	inf "github.com/uxff/gohive/tcliservice"
)

//--------------------------------
// SASL status definition
const (
	START = iota + 1
	OK
	BAD
	ERROR
	COMPLETE
	HIVESASL   = "SASL"
	HIVENOSASL = "NOSASL"
)

// Represents job status, including success state and time the
// status was updated.
type Status struct {
	state *inf.TOperationState
	Error error
	At    time.Time
}

var (
	DefaultOptions = Options{PollIntervalSeconds: 2, BatchSize: 1000, QueryTimeout: 100}
)

// Options for opened Hive sessions.
type Options struct {
	PollIntervalSeconds int64
	BatchSize           int64
	QueryTimeout        int64
}
