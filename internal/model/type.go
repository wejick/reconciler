package model

import "time"

type TransactionType int

const (
	TransactionTypeCredit TransactionType = 1
	TransactionTypeDebit  TransactionType = 2
)

type Transaction struct {
	TrxID           string              `csv:"trxID"`
	Amount          float64             `csv:"amount"`
	Type            TransactionType     `csv:"type"`
	TransactionTime DateTimeTransaction `csv:"transactionTime"`
}

type BankStatement struct {
	UniqueIdentifier string                `csv:"unique_identifier"`
	Amount           float64               `csv:"amount"`
	Date             DateTimeBankStatement `csv:"date"`
	Bank             string                `csv:"bank"`
}

//Summary

type ReconciliationSummary struct {
	TotalTransactionsProcessed int     `csv:"total_transactions_processed"`
	TotalMatchedTransactions   int     `csv:"total_matched_transactions"`
	TotalUnmatchedTransactions int     `csv:"total_unmatched_transactions"`
	TotalDiscrepancies         float64 `csv:"total_discrepancies"`
}

type UnmatchedResult struct {
	TrxID            string          `csv:"trxID"`
	UniqueIdentifier string          `csv:"unique_identifier"`
	Amount           float64         `csv:"amount"`
	Type             TransactionType `csv:"type"`
	TransactionTime  time.Time       `csv:"transactionTime"`
	BankName         string          `csv:"bank_name"`
}

type DateTimeBankStatement struct {
	time.Time
}

type DateTimeTransaction struct {
	time.Time
}

func (date *DateTimeBankStatement) UnmarshalCSV(csv string) (err error) {
	date.Time, err = time.Parse("2006-01-02", csv)
	return err
}

func (date *DateTimeTransaction) UnmarshalCSV(csv string) (err error) {
	date.Time, err = time.Parse("2006-01-02T15:04:05", csv)
	return err
}
