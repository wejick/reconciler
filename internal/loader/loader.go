package loader

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/wejick/reconciler/internal/model"
)

// CSV Format is source_partition.csv with path of the date DDMMYY. All sources are available in the same directory.

// Example :
// 01012021/BCA_1.csv
// 01012021/BRI_1.csv
// 01012021/transaction_1.csv

var ErrBankNameNotFound = errors.New("bank name not found")

var bankName []string = []string{"BCA", "BRI"}

func LoadTransactions(beginningDate, endDate time.Time) ([]model.Transaction, error) {
	transactions := []model.Transaction{}

	for date := beginningDate; !date.After(endDate); date = date.AddDate(0, 0, 1) {
		filePath := fmt.Sprintf("testdata/%s/transaction_1.csv", date.Format("01012006"))
		txns, err := LoadSystemTransactions(filePath)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, txns...)
	}

	return transactions, nil
}

func LoadBankStatements(beginningDate, endDate time.Time) ([]model.BankStatement, error) {
	bankStatements := []model.BankStatement{}

	for date := beginningDate; !date.After(endDate); date = date.AddDate(0, 0, 1) {
		//read all files in the directory on correct date
		files, err := os.ReadDir(fmt.Sprintf("testdata/%s", date.Format("01012006")))
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			filePath := fmt.Sprintf("testdata/%s/%s", date.Format("01012006"), file.Name())
			statements, err := LoadBankTransactions(filePath)
			if err != nil {
				if errors.Is(err, ErrBankNameNotFound) {
					continue
				}
				return nil, err
			}
			bankStatements = append(bankStatements, statements...)
		}
	}
	return bankStatements, nil
}

func LoadSystemTransactions(filePath string) ([]model.Transaction, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	transactions := []model.Transaction{}
	if err := gocsv.UnmarshalFile(file, &transactions); err != nil {
		return nil, err
	}
	return transactions, nil
}

func LoadBankTransactions(filePath string) ([]model.BankStatement, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bankName := getBankName(filePath)
	if bankName == "" {
		return nil, ErrBankNameNotFound
	}

	bankStatements := []model.BankStatement{}
	if err := gocsv.UnmarshalFile(file, &bankStatements); err != nil {
		return nil, err
	}

	for i := range bankStatements {
		bankStatements[i].Bank = bankName
	}

	return bankStatements, nil
}

func getBankName(filePath string) string {
	for _, bank := range bankName {
		if strings.Contains(filePath, bank) {
			return bank
		}
	}

	return ""
}

func GetTotalAmount(transactions []model.Transaction) float64 {
	total := 0.0
	for _, tx := range transactions {
		total += tx.Amount
	}
	return total
}

func GetBankTotalAmount(bankStatements []model.BankStatement) float64 {
	total := 0.0
	for _, bs := range bankStatements {
		total += bs.Amount
	}
	return total
}

func GetTotalUnmatchedAmount(unmatchedResults []model.UnmatchedResult) float64 {
	total := 0.0
	for _, result := range unmatchedResults {
		total += result.Amount
	}
	return total
}
