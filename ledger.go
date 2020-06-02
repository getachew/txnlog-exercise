package main

import (
	"bufio"
	"fmt"
	"os"
)

// ledger is the primary structure used to process the file and keep a running record
type ledger struct {

	// the file read into a buffered reader
	rd *bufio.Reader

	// items is a channel where each line item is emitted in
	items chan item

	// debits keeps a running total amount of the debits txns
	debits float64

	// credits keeps a running total amount of the credits txns
	credits float64

	// credits keeps a running total amount of the credits txns
	apStarted int64

	// credits keeps a running total amount of the credits txns
	apEnded int64

	// users keep a total per user
	users map[uint64]float64

	// header info for the current process used for validation
	header Header

	// keeps a count of how many line items were processed used for validation
	count uint32
}

// newLedger create a ledger and returns the ledger
// and a channel of items that the ledger can be notified of an
// item (read: a line from txnslog.dat database)
func newLedger(file *os.File) (*ledger, chan item) {
	reader := bufio.NewReader(file)
	l := &ledger{
		rd:    reader,
		items: make(chan item),
		users: make(map[uint64]float64),
	}

	go l.run()
	return l, l.items
}

// prints the summary of current ledger totals
func (l *ledger) printSummary() {
	summary := `  total credit amount=%.2f
  total debit amount=%.2f
  autopays started=%d
  autopays ended=%d
  balance for user 2456938384156277127=%.2f
`
	fmt.Printf(summary, l.credits, l.debits, l.apStarted, l.apEnded, l.users[2456938384156277127])
}

// run is a function that begins processing the ledger. Using a state function pattern
// starts with the readHeader state function which we know to call because the first "n"
// bytes of a file is the header
// when readHeader returns it passes the next state function thus this function loops until all
// state function are complete
func (l *ledger) run() {
	for state := readHeader; state != nil; {
		state = state(l)
	}
	close(l.items) // we are done
}

// a helper function to emit an item
func (l *ledger) emit(t item) {
	l.items <- t
}

func (l *ledger) validateRun() (success bool, received uint32, declared uint32) {
	success = l.header.length == l.count
	received = l.count
	declared = l.header.length

	return
}

func (l *ledger) process(i item) {
	switch i.rt {
	case Head:
		l.header = parseHeader(i.buf)
		l.count--

	case Credit:
		t := parseDebitCredit(i.buf)
		amt := float64(t.amount)
		l.users[t.userID] += amt
		l.credits += amt
	case Debit:
		t := parseDebitCredit(i.buf)
		amt := float64(t.amount)
		l.users[t.userID] -= amt
		l.debits += amt
	case StartAutopay:
		l.apStarted++

	case EndAutopay:
		l.apEnded++
	}

	l.count++
}
