// orchestrator
// Orchestrator handle the job queue scheduling. It will run the job in the queue when a slot is available.
// Current implementation there's only one slot available.
// The design can be this simple because it's tied specific for this usecase only.
package orchestrator

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

type QueueStatus string

type RunFunc func(job Job) error

const (
	QueueStatusPending  QueueStatus = "pending"
	QueueStatusRunning  QueueStatus = "running"
	QueueStatusFinished QueueStatus = "finished"
	QueueStatusFailed   QueueStatus = "failed"
)

type Queue struct {
	Jobs         map[string]Job
	jobMu        sync.Mutex
	runningJobs  string
	runningJobMu sync.Mutex

	RunFunc RunFunc
}

func NewQueue(runFunc RunFunc) *Queue {
	return &Queue{
		Jobs:    make(map[string]Job),
		RunFunc: runFunc,
	}
}

type Job struct {
	ID            string
	DateStart     time.Time
	DateEnd       time.Time
	Status        string
	ReportCSVURL  string
	SummaryCSVURL string
	Concurrency   int
	CreatedAt     time.Time
	FinishedAt    time.Time
}

func (q *Queue) AddJob(dateStart, dateEnd time.Time, concurrency int) error {
	job := Job{
		ID:          uuid.New().String(),
		DateStart:   dateStart,
		DateEnd:     dateEnd,
		Status:      string(QueueStatusPending),
		CreatedAt:   time.Now(),
		Concurrency: concurrency,
	}
	q.jobMu.Lock()
	if _, exists := q.Jobs[job.ID]; exists {
		q.jobMu.Unlock()
		return fmt.Errorf("job already exists")
	}
	q.Jobs[job.ID] = job
	q.jobMu.Unlock()

	return nil
}

func (q *Queue) updateJobStatus(id string, status QueueStatus) error {
	q.jobMu.Lock()
	defer q.jobMu.Unlock()
	job, exists := q.Jobs[id]
	if !exists {
		return fmt.Errorf("job not found")
	}
	job.Status = string(status)
	q.Jobs[id] = job

	return nil
}

func (q *Queue) getNextJob() (Job, error) {
	for _, job := range q.Jobs {
		if job.Status == string(QueueStatusPending) {
			return job, nil
		}
	}
	return Job{}, nil
}

// Running and state machine is here
func (q *Queue) runNextJob() error {
	job, err := q.getNextJob()
	if err != nil {
		return err
	}

	if job.ID == "" {
		return nil
	}

	q.updateJobStatus(job.ID, QueueStatusRunning)

	err = q.RunFunc(job)
	if err != nil {
		q.updateJobStatus(job.ID, QueueStatusFailed)
		return err
	}

	q.updateJobStatus(job.ID, QueueStatusFinished)

	q.runningJobMu.Lock()
	q.runningJobs = ""
	q.runningJobMu.Unlock()

	return nil
}

func (q *Queue) GetAllJobs() []Job {
	jobs := make([]Job, 0, len(q.Jobs))
	for _, job := range q.Jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

func (q *Queue) Start() {
	for {
		if q.runningJobs != "" {
			continue
		}

		err := q.runNextJob()
		if err != nil {
			log.Println("Error running job:", err)
		}

		time.Sleep(1 * time.Second)
	}
}
