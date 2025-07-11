package main

import (
	"fmt"

	"github.com/wejick/reconciler/internal/loader"
	"github.com/wejick/reconciler/internal/mapreduce"
	"github.com/wejick/reconciler/internal/model"
	"github.com/wejick/reconciler/internal/orchestrator"
)

var queue *orchestrator.Queue

func main() {

	queue = orchestrator.NewQueue(runReconcile)
	go queue.Start()

	// queue.AddJob(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), 2)

	InitHTTPHandler(":8080")
}

func runReconcile(job orchestrator.Job) error {
	txns, err := loader.LoadTransactions(job.DateStart, job.DateEnd)
	if err != nil {
		return err
	}

	bankStatements, err := loader.LoadBankStatements(job.DateStart, job.DateEnd)
	if err != nil {
		return err
	}

	txMapper := mapreduce.MapTransaction(txns)
	bankMapper := mapreduce.MapBankStatement(bankStatements)
	txGroups := mapreduce.GetAllTransactionGroup(txMapper)
	bankGroups := mapreduce.GetAllBankStatementGroup(bankMapper)
	groups := mapreduce.CombineAndDedupList(txGroups, bankGroups)
	txUnmatch, bankUnmatch := runReducer(groups, txMapper, bankMapper, job.Concurrency)

	totalTransactionsAmount := loader.GetTotalAmount(txns)
	totalBankStatementsAmount := loader.GetBankTotalAmount(bankStatements)

	summary := model.ReconciliationSummary{
		TotalTransactionsProcessed: len(txns),
		TotalMatchedTransactions:   len(txns) - len(txUnmatch),
		TotalUnmatchedTransactions: len(txUnmatch) + len(bankUnmatch),
		TotalDiscrepancies:         totalTransactionsAmount - totalBankStatementsAmount,
	}

	totalUnmatched := append(txUnmatch, bankUnmatch...)
	loader.WriteUnmatchedResults(totalUnmatched, fmt.Sprintf("report/unmatched_%s.csv", job.ID))
	loader.WriteSummary([]model.ReconciliationSummary{summary}, fmt.Sprintf("report/summary_%s.csv", job.ID))

	return nil
}

func runReducer(groups []string, txMapper map[string][]model.Transaction, bankMapper map[string][]model.BankStatement, runnerNum int) (txUnmatch, bankUnmatch []model.UnmatchedResult) {
	ch := make(chan [][]model.UnmatchedResult, runnerNum)

	// we need to split the groups into runnerNum parts
	groupsPerRunner := len(groups) / runnerNum
	for i := 0; i < runnerNum; i++ {
		start := i * groupsPerRunner
		end := start + groupsPerRunner
		if i == runnerNum-1 {
			end = len(groups)
		}
		go func() {
			txUnmatch, bankUnmatch := mapreduce.Reduce(groups[start:end], txMapper, bankMapper)
			ch <- [][]model.UnmatchedResult{txUnmatch, bankUnmatch}
		}()
	}

	for i := 0; i < runnerNum; i++ {
		select {
		case result := <-ch:
			txUnmatch = append(txUnmatch, result[0]...)
			bankUnmatch = append(bankUnmatch, result[1]...)
		}
	}

	return
}
