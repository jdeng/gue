package gue

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/vgarvardt/gue/v5/adapter"
)

// JobPriority is the wrapper type for Job.Priority
type JobPriority int16

// Some shortcut values for JobPriority that can be any, but chances are high that one of these will be the most used.
const (
	JobPriorityHighest JobPriority = -32768
	JobPriorityHigh    JobPriority = -16384
	JobPriorityDefault JobPriority = 0
	JobPriorityLow     JobPriority = 16384
	JobPriorityLowest  JobPriority = 32767
)

type Job interface {
	ID() string
	Type() string
	Queue() string
	Priority() JobPriority
	RunAt() time.Time

	Tx() adapter.Tx
	Complete(ctx context.Context) error
	Delete(ctx context.Context) error
	Done(ctx context.Context) error
	Error(ctx context.Context, err error) error
}

// BasicJob is a single unit of work for Gue to perform.
type BasicJob struct {
	// mID is the unique database mID of the Job. It is ignored on job creation.
	mID ulid.ULID

	// mQueue is the name of the queue. It defaults to the empty queue "".
	mQueue string

	// mPriority is the priority of the Job. The default priority is 0, and a
	// lower number means a higher priority.
	//
	// The highest priority is JobPriorityHighest, the lowest one is JobPriorityLowest
	mPriority JobPriority

	// mRunAt is the time that this job should be executed. It defaults to now(),
	// meaning the job will execute immediately. Set it to a value in the future
	// to delay a job's execution.
	mRunAt time.Time

	// mType maps job to a worker func.
	mType string

	// Args for the job.
	Args []byte

	// ErrorCount is the number of times this job has attempted to run, but failed with an error.
	// It is ignored on job creation.
	// This field is initialised only when the Job is being retrieved from the DB and is not
	// being updated when the current Job handler errored.
	ErrorCount int32

	// LastError is the error message or stack trace from the last time the job failed. It is ignored on job creation.
	// This field is initialised only when the Job is being retrieved from the DB and is not
	// being updated when the current Job run errored. This field supposed to be used mostly for the debug reasons.
	LastError sql.NullString

	mu      sync.Mutex
	deleted bool
	tx      adapter.Tx
	backoff Backoff
	logger  adapter.Logger
}

func (j *BasicJob) Type() string {
	return j.mType
}

func (j *BasicJob) ID() string {
	return j.mID.String()
}

func (j *BasicJob) Queue() string {
	return j.mQueue
}

func (j *BasicJob) Priority() JobPriority {
	return j.mPriority
}

func (j *BasicJob) RunAt() time.Time {
	return j.mRunAt
}

// Tx returns DB transaction that this job is locked to. You may use
// it as you please until you call Done(). At that point, this transaction
// will be committed. This function will return nil if the Job's
// transaction was closed with Done().
func (j *BasicJob) Tx() adapter.Tx {
	return j.tx
}

// Delete marks this job as complete by deleting it from the database.
//
// You must also later call Done() to return this job's database connection to
// the pool. If you got the job from the worker - it will take care of cleaning up the job and resources,
// no need to do this manually in a WorkFunc.
func (j *BasicJob) Delete(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.deleted {
		return nil
	}

	_, err := j.tx.Exec(ctx, `DELETE FROM gue_jobs WHERE job_id = $1`, j.ID())
	if err != nil {
		return err
	}

	j.deleted = true
	return nil
}

// Done commits transaction that marks job as done. If you got the job from the worker - it will take care of
// cleaning up the job and resources, no need to do this manually in a WorkFunc.
func (j *BasicJob) Done(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.tx == nil {
		// already marked as done
		return nil
	}

	if err := j.tx.Commit(ctx); err != nil {
		return err
	}

	j.tx = nil

	return nil
}

func (j *BasicJob) Complete(ctx context.Context) error {
	return j.Delete(ctx)
}

// Error marks the job as failed and schedules it to be reworked. An error
// message or backtrace can be provided as msg, which will be saved on the job.
// It will also increase the error count.
//
// This call marks job as done and releases (commits) transaction,
// so calling Done() is not required, although calling it will not cause any issues.
// If you got the job from the worker - it will take care of cleaning up the job and resources,
// no need to do this manually in a WorkFunc.
func (j *BasicJob) Error(ctx context.Context, jErr error) (err error) {
	defer func() {
		doneErr := j.Done(ctx)
		if doneErr != nil {
			err = fmt.Errorf("failed to mark job as done (original error: %v): %w", err, doneErr)
		}
	}()

	errorCount := j.ErrorCount + 1
	now := time.Now().UTC()
	newRunAt := j.calculateErrorRunAt(jErr, now, errorCount)
	if newRunAt.IsZero() {
		j.logger.Info(
			"Got empty new run at for the errored job, discarding it",
			adapter.F("job-type", j.mType),
			adapter.F("job-queue", j.mQueue),
			adapter.F("job-errors", errorCount),
			adapter.Err(jErr),
		)
		err = j.Delete(ctx)
		return
	}

	_, err = j.tx.Exec(
		ctx,
		`UPDATE gue_jobs SET error_count = $1, run_at = $2, last_error = $3, updated_at = $4 WHERE job_id = $5`,
		errorCount, newRunAt, jErr.Error(), now, j.ID(),
	)

	return err
}

func (j *BasicJob) calculateErrorRunAt(err error, now time.Time, errorCount int32) time.Time {
	errReschedule, ok := err.(ErrJobReschedule)
	if ok {
		return errReschedule.rescheduleJobAt()
	}

	backoff := j.backoff(int(errorCount))
	if backoff < 0 {
		return time.Time{}
	}

	return now.Add(backoff)
}
