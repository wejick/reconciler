package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

func AccessLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func InitHTTPHandler(port string) {
	http.Handle("/reconcile/", AccessLogMiddleware(http.DefaultServeMux))

	http.Handle("/reconcile/addqueue", AccessLogMiddleware(http.HandlerFunc(AddQueueHandler)))
	http.Handle("/reconcile/status", AccessLogMiddleware(http.HandlerFunc(GetAllJobsHandler)))

	http.Handle("/reconcile/report/", AccessLogMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// only get the filename
		filename := strings.Split(r.URL.Path, "/")[3] // TODO unsafe
		filePath := "report/" + filename
		http.ServeFile(w, r, filePath)
	})))

	log.Println("Starting HTTP server on port", port)
	log.Fatal(http.ListenAndServe(port, nil))
}

// POST /reconcile/addqueue
// {
// "begin_date":"2006-01-01",
// "end_date":"2006-01-01",
// "concurrency":2
// }

type AddQueueRequest struct {
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Concurrency int    `json:"concurrency"`
}

func AddQueueHandler(w http.ResponseWriter, r *http.Request) {
	// only post method
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var req AddQueueRequest
	err = json.Unmarshal(body, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// parse date
	beginDate, err := time.Parse("2006-01-02", req.BeginDate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	endDate, err := time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = queue.AddJob(beginDate, endDate, req.Concurrency)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GET /reconcile/status
//
//	{[
//	{
//	runid : "1",
//	status : "done/waiting/inprocess",
//	report_file : "/report/report_file.csv",
//	unmatch_list : "/report/unmatch_file.csv",
//	}
//	]}
type JobStatus struct {
	RunID       string `json:"runid"`
	Status      string `json:"status"`
	SummaryFile string `json:"report_file"`
	UnmatchList string `json:"unmatch_list"`
}

func GetAllJobsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobs := queue.GetAllJobs()
	jobStatuses := make([]JobStatus, len(jobs))
	for i, job := range jobs {
		jobStatuses[i] = JobStatus{
			RunID:       job.ID,
			Status:      job.Status,
			SummaryFile: fmt.Sprintf("/reconcile/report/summary_%s.csv", job.ID),
			UnmatchList: fmt.Sprintf("/reconcile/report/unmatched_%s.csv", job.ID),
		}
	}
	json.NewEncoder(w).Encode(jobStatuses)
}
