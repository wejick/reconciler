package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

func InitHTTPHandler(port string) {
	http.HandleFunc("/reconcile/addqueue", AddQueueHandler)

	go func() {
		log.Println("Starting HTTP server on port", port)
		log.Fatal(http.ListenAndServe(port, nil))
	}()
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
