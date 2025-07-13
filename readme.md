# Reconciliator service utilizing map reduce

## Setup and Run
1. Make sure to have go setup >1.24.4 (it will work with older go, but I use this).
2. $make setup
3. $make run

## How to use
1. The bank statement and transaction should be put under `testdata/[date]`, the date format is ddmmyyyy.
2. Submit the job with `/reconciler/addqueue` api.
3. Check the job status with `/reconciler/status`. The report and summery csv will be available when the job is done.

Example request is provided in the test.restbook, use REST Book extension in vs code.

