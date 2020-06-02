package main

import (
	"log"
	"os"
)

// GOAL: read and process the transactions in txnlog.dat binary file
func main() {
	file, err := os.Open("txnlog.dat")
	if err != nil {
		log.Fatal("Error opening txnlog file: ", err)
	}
	defer file.Close()

	// create a new ledger instance by passing txnlog.dat binary file
	ledger, items := newLedger(file)

	// for each item in the channel of items
	for i := range items {
		ledger.process(i)
	}

	ledger.printSummary()
}
