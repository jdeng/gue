package gue

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/vgarvardt/gue/v5/adapter"
)

// ErrMissingType is returned when you attempt to enqueue a job with no Type
// specified.
var ErrMissingType = errors.New("job type must be specified")

var (
	attrJobType = attribute.Key("job-type")
	attrSuccess = attribute.Key("success")
)

// Client is a Gue client that can add jobs to the queue and remove jobs from
// the queue.
type Client struct {
	pool    adapter.ConnPool
	logger  adapter.Logger
	id      string
	backoff Backoff
	meter   metric.Meter

	entropy io.Reader

	enqueuer JobEnqueuer
	locker   JobLocker

	mEnqueue metric.Int64Counter
	mLockJob metric.Int64Counter
}

type JobEnqueuer interface {
	Enqueue(ctx context.Context, j Job, q adapter.Queryable) error
}

type NopJobEnqueuer struct{}

func (n *NopJobEnqueuer) Enqueue(ctx context.Context, j Job, q adapter.Queryable) error {
	return errors.New("not implemented")
}

type JobLocker interface {
	LockJob(ctx context.Context, queue string) (Job, error)
	LockNextScheduledJob(ctx context.Context, queue string) (Job, error)
	LockJobByID(ctx context.Context, id string) (Job, error)
}

type BasicJobEnqueuer struct {
	pool    adapter.ConnPool
	entropy io.Reader
}

type BasicJobLocker struct {
	pool    adapter.ConnPool
	backoff Backoff
	logger  adapter.Logger

	mLockJob metric.Int64Counter
}

// NewClient creates a new Client that uses the pgx pool.
func NewClient(pool adapter.ConnPool, options ...ClientOption) (*Client, error) {
	instance := Client{
		pool:    pool,
		logger:  adapter.NoOpLogger{},
		id:      RandomStringID(),
		backoff: DefaultExponentialBackoff,
		meter:   noop.NewMeterProvider().Meter("noop"),
		entropy: &ulid.LockedMonotonicReader{
			MonotonicReader: ulid.Monotonic(rand.Reader, 0),
		},
	}

	for _, option := range options {
		option(&instance)
	}

	if instance.enqueuer == nil {
		instance.enqueuer = &BasicJobEnqueuer{pool: pool, entropy: instance.entropy}
	}

	if instance.locker == nil {
		instance.locker = &BasicJobLocker{
			pool:     pool,
			backoff:  instance.backoff,
			logger:   instance.logger,
			mLockJob: instance.mLockJob,
		}
	}

	instance.logger = instance.logger.With(adapter.F("client-id", instance.id))

	return &instance, instance.initMetrics()
}

// Enqueue adds a job to the queue.
func (c *Client) Enqueue(ctx context.Context, j *BasicJob) error {
	return c.execEnqueue(ctx, j, c.pool)
}

// EnqueueTx adds a job to the queue within the scope of the transaction.
// This allows you to guarantee that an enqueued job will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueTx(ctx context.Context, j *BasicJob, tx adapter.Tx) error {
	return c.execEnqueue(ctx, j, tx)
}

// EnqueueBatch adds a batch of jobs. Operation is atomic, so either all jobs are added, or none.
func (c *Client) EnqueueBatch(ctx context.Context, jobs []*BasicJob) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not begin transaction")
	}

	for i, j := range jobs {
		if err := c.execEnqueue(ctx, j, tx); err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("Could not properly rollback transaction", adapter.Err(err))
			}
			return fmt.Errorf("could not enqueue job from the batch [idx %d]: %w", i, err)
		}
	}

	return tx.Commit(ctx)
}

// EnqueueBatchTx adds a batch of jobs within the scope of the transaction.
// This allows you to guarantee that an enqueued batch will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueBatchTx(ctx context.Context, jobs []*BasicJob, tx adapter.Tx) error {
	for i, j := range jobs {
		if err := c.execEnqueue(ctx, j, tx); err != nil {
			return fmt.Errorf("could not enqueue job from the batch [idx %d]: %w", i, err)
		}
	}

	return nil
}

func (c *Client) execEnqueue(ctx context.Context, j Job, q adapter.Queryable) (err error) {
	err = c.enqueuer.Enqueue(ctx, j, q)
	c.logger.Debug(
		"Tried to enqueue a job",
		adapter.Err(err),
		adapter.F("queue", j.Queue()),
		adapter.F("id", j.ID()),
	)

	c.mEnqueue.Add(ctx, 1, metric.WithAttributes(attrJobType.String(j.Type()), attrSuccess.Bool(err == nil)))
	return err
}

func (jq *BasicJobEnqueuer) Enqueue(ctx context.Context, xj Job, q adapter.Queryable) (err error) {
	j, ok := xj.(*BasicJob)
	if !ok {
		return errors.New("not a BasicJob")
	}

	if j.mType == "" {
		return ErrMissingType
	}

	now := time.Now().UTC()

	runAt := j.mRunAt
	if runAt.IsZero() {
		j.mRunAt = now
	}

	if j.Args == nil {
		j.Args = []byte{}
	}

	if j.mID, err = ulid.New(ulid.Timestamp(now), jq.entropy); err != nil {
		return fmt.Errorf("could not generate new Job ULID ID: %w", err)
	}
	_, err = q.Exec(ctx, `INSERT INTO gue_jobs
(job_id, queue, priority, run_at, job_type, args, created_at, updated_at)
VALUES
($1, $2, $3, $4, $5, $6, $7, $7)
`, j.ID(), j.mQueue, j.mPriority, j.mRunAt, j.mType, j.Args, now)

	return err
}

func (jq *BasicJobLocker) LockJob(ctx context.Context, queue string) (Job, error) {
	sql := `SELECT job_id, queue, priority, run_at, job_type, args, error_count, last_error
FROM gue_jobs
WHERE queue = $1 AND run_at <= $2
ORDER BY priority ASC
LIMIT 1 FOR UPDATE SKIP LOCKED`

	return jq.execLockJob(ctx, true, sql, queue, time.Now().UTC())
}

// LockJob attempts to retrieve a Job from the database in the specified queue.
// If a job is found, it will be locked on the transactional level, so other workers
// will be skipping it. If no job is found, nil will be returned instead of an error.
//
// This function cares about the priority first to lock top priority jobs first even if there are available ones that
// should be executed earlier but with the lower priority.
//
// Because Gue uses transaction-level locks, we have to hold the
// same transaction throughout the process of getting a job, working it,
// deleting it, and releasing the lock.
//
// After the Job has been worked, you must call either Job.Done() or Job.Error() on it
// in order to commit transaction to persist Job changes (remove or update it).
func (c *Client) LockJob(ctx context.Context, queue string) (Job, error) {
	return c.locker.LockJob(ctx, queue)
}

func (c *Client) LockNextScheduledJob(ctx context.Context, queue string) (Job, error) {
	return c.locker.LockNextScheduledJob(ctx, queue)
}

func (c *Client) LockJobByID(ctx context.Context, id string) (Job, error) {
	return c.locker.LockJobByID(ctx, id)
}

// LockJobByID attempts to retrieve a specific Job from the database.
// If the job is found, it will be locked on the transactional level, so other workers
// will be skipping it. If the job is not found, an error will be returned
//
// Because Gue uses transaction-level locks, we have to hold the
// same transaction throughout the process of getting the job, working it,
// deleting it, and releasing the lock.
//
// After the Job has been worked, you must call either Job.Done() or Job.Error() on it
// in order to commit transaction to persist Job changes (remove or update it).
func (c *BasicJobLocker) LockJobByID(ctx context.Context, id string) (Job, error) {
	sql := `SELECT job_id, queue, priority, run_at, job_type, args, error_count, last_error
FROM gue_jobs
WHERE job_id = $1 FOR UPDATE SKIP LOCKED`

	return c.execLockJob(ctx, false, sql, id)
}

// LockNextScheduledJob attempts to retrieve the earliest scheduled Job from the database in the specified queue.
// If a job is found, it will be locked on the transactional level, so other workers
// will be skipping it. If no job is found, nil will be returned instead of an error.
//
// This function cares about the scheduled time first to lock earliest to execute jobs first even if there are ones
// with a higher priority scheduled to a later time but already eligible for execution
//
// Because Gue uses transaction-level locks, we have to hold the
// same transaction throughout the process of getting a job, working it,
// deleting it, and releasing the lock.
//
// After the Job has been worked, you must call either Job.Done() or Job.Error() on it
// in order to commit transaction to persist Job changes (remove or update it).
func (c *BasicJobLocker) LockNextScheduledJob(ctx context.Context, queue string) (Job, error) {
	sql := `SELECT job_id, queue, priority, run_at, job_type, args, error_count, last_error
FROM gue_jobs
WHERE queue = $1 AND run_at <= $2
ORDER BY run_at, priority ASC
LIMIT 1 FOR UPDATE SKIP LOCKED`

	return c.execLockJob(ctx, true, sql, queue, time.Now().UTC())
}

func (c *BasicJobLocker) execLockJob(ctx context.Context, handleErrNoRows bool, sql string, args ...any) (Job, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		c.mLockJob.Add(ctx, 1, metric.WithAttributes(attrJobType.String(""), attrSuccess.Bool(false)))
		return nil, err
	}

	j := BasicJob{tx: tx, backoff: c.backoff, logger: c.logger}

	err = tx.QueryRow(ctx, sql, args...).Scan(
		&j.mID,
		&j.mQueue,
		&j.mPriority,
		&j.mRunAt,
		&j.mType,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
	)
	if err == nil {
		c.mLockJob.Add(ctx, 1, metric.WithAttributes(attrJobType.String(j.mType), attrSuccess.Bool(true)))
		return &j, nil
	}

	rbErr := tx.Rollback(ctx)
	if handleErrNoRows && err == adapter.ErrNoRows {
		return nil, rbErr
	}

	return nil, fmt.Errorf("could not lock a job (rollback result: %v): %w", rbErr, err)
}

func (c *Client) initMetrics() (err error) {
	if c.mEnqueue, err = c.meter.Int64Counter(
		"gue_client_enqueue",
		metric.WithDescription("Number of jobs being enqueued"),
		metric.WithUnit("1"),
	); err != nil {
		return fmt.Errorf("could not register mEnqueue metric: %w", err)
	}

	if c.mLockJob, err = c.meter.Int64Counter(
		"gue_client_lock_job",
		metric.WithDescription("Number of jobs being locked (consumed)"),
		metric.WithUnit("1"),
	); err != nil {
		return fmt.Errorf("could not register mLockJob metric: %w", err)
	}

	return nil
}
