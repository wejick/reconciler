package main

import (
	"fmt"
	"log"
	"time"

	"github.com/wejick/reconciler/internal/loader"
	"github.com/wejick/reconciler/internal/mapreduce"
	"github.com/wejick/reconciler/internal/model"
	"github.com/wejick/reconciler/internal/orchestrator"
)

var queue *orchestrator.Queue

func main() {

	queue = orchestrator.NewQueue(runReconcile)
	go queue.Start()

	InitHTTPHandler(":8080")

	txns, err := loader.LoadTransactions(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		log.Fatal(err)
	}
	bankStatements, err := loader.LoadBankStatements(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		log.Fatal(err)
	}

	txMapper := mapreduce.MapTransaction(txns)
	bankMapper := mapreduce.MapBankStatement(bankStatements)
	txGroups := mapreduce.GetAllTransactionGroup(txMapper)
	bankGroups := mapreduce.GetAllBankStatementGroup(bankMapper)
	groups := mapreduce.CombineAndDedupList(txGroups, bankGroups)
	txUnmatch, bankUnmatch := runReducer(groups, txMapper, bankMapper, 2)

	fmt.Println("txUnmatch", txUnmatch)
	fmt.Println("bankUnmatch", bankUnmatch)

	totalTransactionsAmount := loader.GetTotalAmount(txns)
	totalBankStatementsAmount := loader.GetBankTotalAmount(bankStatements)

	summary := model.ReconciliationSummary{
		TotalTransactionsProcessed: len(txns),
		TotalMatchedTransactions:   len(txns) - len(txUnmatch),
		TotalUnmatchedTransactions: len(txUnmatch) + len(bankUnmatch),
		TotalDiscrepancies:         totalTransactionsAmount - totalBankStatementsAmount,
	}

	dateNow := time.Now().Format("2006-01-02")
	totalUnmatched := append(txUnmatch, bankUnmatch...)
	loader.WriteUnmatchedResults(totalUnmatched, fmt.Sprintf("report/unmatched_%s.csv", dateNow))

	fmt.Printf("Summary: %+v\n", summary)
	fmt.Printf("report file: %s\n", fmt.Sprintf("report/unmatched_%s.csv", dateNow))
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

	fmt.Printf("Summary: %+v\n", summary)

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
