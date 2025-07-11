// mapreduce
// Here we implement mapreduce algorithm to match the transaction with the bank statement
// 1. MapTransaction and MapBankStatement is to create group by amount and date (MatchKey)
// 2. Reducer contain reconciliation logic, it will reduce only the provided group
// 3. This means we can run multiple reducer in paralel by providing different group each
package mapreduce

import (
	"fmt"
	"time"

	"github.com/wejick/reconciler/internal/model"
)

type MatchKey struct {
	Amount float64
	Date   time.Time
}

func (m MatchKey) String() string {
	return fmt.Sprintf("%s%.2f", m.Date.Format("2006-01-02"), m.Amount)
}

func MapTransaction(transactions []model.Transaction) map[string][]model.Transaction {
	mapper := make(map[string][]model.Transaction)

	for _, tx := range transactions {
		matchKey := MatchKey{
			Amount: tx.Amount,
			Date:   tx.TransactionTime.Time,
		}
		mapper[matchKey.String()] = append(mapper[matchKey.String()], tx)
	}

	return mapper
}

func MapBankStatement(bankStatements []model.BankStatement) map[string][]model.BankStatement {
	mapper := make(map[string][]model.BankStatement)

	for _, tx := range bankStatements {
		matchKey := MatchKey{
			Amount: tx.Amount,
			Date:   tx.Date.Time,
		}
		mapper[matchKey.String()] = append(mapper[matchKey.String()], tx)
	}

	return mapper
}

func GetAllTransactionGroup(mapper map[string][]model.Transaction) []string {
	groups := []string{}

	for key := range mapper {
		groups = append(groups, key)
	}

	return groups
}

func GetAllBankStatementGroup(mapper map[string][]model.BankStatement) []string {
	groups := []string{}

	for key := range mapper {
		groups = append(groups, key)
	}

	return groups
}

func CombineAndDedupList(txGroups []string, bsGroups []string) []string {
	pool := make(map[string]bool)
	result := []string{}

	for _, group := range txGroups {
		if _, exist := pool[group]; !exist {
			result = append(result, group)
			pool[group] = true
		}
	}

	for _, group := range bsGroups {
		if _, exist := pool[group]; !exist {
			result = append(result, group)
		}
	}

	return result
}

func Reduce(groups []string, mapper map[string][]model.Transaction, bankStatements map[string][]model.BankStatement) (txUnmatch, bankUnmatch []model.UnmatchedResult) {
	for _, group := range groups {
		txs := mapper[group]
		bankStatements := bankStatements[group]

		// the data type looks like this
		// map[groupKey][]model.Transaction{} and map[groupKey][]model.BankStatement{}
		// because the key is the same, and the combination of amount and date, everything within the group should be a match
		// the idea to just put the difference to unmatched list if any
		// with the assumption that the provided by csv is sorted by date, we will always have the latest data in the list

		// if the opposite group empty, automatic unmatch
		if len(txs) == 0 {
			for _, bs := range bankStatements {
				txType := model.TransactionTypeDebit
				if bs.Amount < 0 {
					txType = model.TransactionTypeDebit
				}

				bankUnmatch = append(bankUnmatch, model.UnmatchedResult{
					UniqueIdentifier: bs.UniqueIdentifier,
					Amount:           bs.Amount,
					Type:             txType,
					TransactionTime:  bs.Date.Time,
					BankName:         bs.Bank,
				})
			}
		} else if len(bankStatements) == 0 {
			for _, tx := range txs {
				txUnmatch = append(txUnmatch, model.UnmatchedResult{
					TrxID:           tx.TrxID,
					Amount:          tx.Amount,
					Type:            tx.Type,
					TransactionTime: tx.TransactionTime.Time,
				})
			}
		} else {

			// THE MAIN LOGIC HERE

			// match tx with bank statement
			if len(bankStatements) == len(txs) {
				//all matched do nothing
			} else if len(bankStatements) > len(txs) {
				// add the diff to bankUnmatch
				for _, bs := range bankStatements[len(txs)-1:] {
					txType := model.TransactionTypeDebit
					if bs.Amount < 0 {
						txType = model.TransactionTypeDebit
					}
					bankUnmatch = append(bankUnmatch, model.UnmatchedResult{
						UniqueIdentifier: bs.UniqueIdentifier,
						Amount:           bs.Amount,
						Type:             txType,
						TransactionTime:  bs.Date.Time,
						BankName:         bs.Bank,
					})
				}
			} else {
				// add the diff to txUnmatch
				for _, tx := range txs[len(bankStatements)-1:] {
					txUnmatch = append(txUnmatch, model.UnmatchedResult{
						TrxID:  tx.TrxID,
						Amount: tx.Amount,
						Type:   tx.Type,
					})
				}
			}
		}
	}

	return
}
