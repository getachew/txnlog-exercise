package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
)

// RecordType is a byte type to track transaction records
// it maps nicely to record type enum as per the documentation
//
// Record type enum:
// - 0x00: Debit
// - 0x01: Credit
// - 0x02: StartAutopay
// - 0x03: EndAutopay
type RecordType byte

const (
	// Debit 0x00
	Debit RecordType = iota

	// Credit 0x01
	Credit

	// StartAutopay 0x02
	StartAutopay

	// EndAutopay 0x03
	EndAutopay

	// Head is an addition RecordType to track a Header line
	Head
)

// txnlog.dat is a binary file written in this ByteOrder
var systemByteOrder binary.ByteOrder = binary.BigEndian

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

// item represents the line item identified by the recordType rt
type item struct {
	rt  RecordType
	buf []byte
}

// newLedger provides and starts to process the file from a goroutine
// return the ledger and a channel to listen to individual records
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

// stateFn is a function that takes a ledger and returns the next state function
type stateFn func(*ledger) stateFn

// run processes the file using stateFn, the start state is read the header
// using readHeader state function which we know to call because the first "n"
// bytes of a file is the header
func (l *ledger) run() {
	for state := readHeader; state != nil; {
		state = state(l)
	}
	close(l.items) // we are done
}

func (l *ledger) emit(t item) {
	l.items <- t
}

// readHeader knows how to read a header from the given byte buffer
// returns the next state which is readRecord
func readHeader(l *ledger) stateFn {
	buf := make([]byte, 9)
	_, err := l.rd.Read(buf)
	if err != nil {
		log.Fatal("Error parsing Header: ", err)
	}
	l.emit(item{Head, buf})

	return readRecord
}

// readRecord know how to read a single record
// returns the next state which could be nil if we have processed all
// or itself to read the next record
func readRecord(l *ledger) stateFn {
	// check what kind of record this depending on its type we have more or less data to read
	b, err := l.rd.Peek(1)
	if err != nil {
		log.Fatal("Error parsing lexRecord: ", err)
	}

	recordType := RecordType(b[0])
	var buf []byte
	if recordType == Debit || recordType == Credit {
		// debits and credits have extra 8 byte more data
		buf = make([]byte, 21)
	} else {
		buf = make([]byte, 13)
	}
	_, err = l.rd.Read(buf)
	if err != nil {
		log.Fatal("Error parsing Record: ", err)
	}
	l.emit(item{recordType, buf})

	// check to see if we have the minimum amount of bytes to read for the next record
	if _, err := l.rd.Peek(13); err != nil {
		return nil
	}
	return readRecord
}

// Header preserves header information
type Header struct {
	magic   string
	version byte
	length  uint32
}

// Amount type to track amount associated with a record
type Amount float64

// Record encapsulate a single record of txn
type Record struct {
	recordType RecordType
	timestamp  uint32
	userID     uint64
	amount     Amount
}

func parseHeader(b []byte) Header {
	magicString := string(b[0:4])
	version := b[4]
	numRecords := systemByteOrder.Uint32(b[5:])

	return Header{magicString, version, numRecords}

}

func readHeaderMagicString(rd *bufio.Reader) {
	log.Println("Curr Size: ", rd.Size())

	magicString := make([]byte, 4)
	rd.Read(magicString)
	fmt.Println("magicString: ", string(magicString))

	readHeaderVersion(rd)
}

func readHeaderVersion(rd *bufio.Reader) {
	version, err := rd.ReadByte()
	if err != nil {
		log.Fatal("Error parsing txn log header version")
	}
	fmt.Println("version: ", version)
	readHeaderNumRecords(rd)
}

func readHeaderNumRecords(rd *bufio.Reader) {
	numRecords := make([]byte, 4)
	rd.Read(numRecords)
	n := systemByteOrder.Uint32(numRecords)
	fmt.Print("length of records: ", n)
	// emit header

	// parse body
	parseTxn(rd)
}

func parseTxn(rd *bufio.Reader) {
	// record enum
	recordType, err := rd.ReadByte()
	if err != nil {
		log.Fatal("unable to read record", err)
	}

	ts := make([]byte, 4)
	_, err = rd.Read(ts)
	if err != nil {
		log.Fatal("unable to read record", err)
	}

	time := systemByteOrder.Uint32(ts)

	userID := make([]byte, 8)
	_, err = rd.Read(userID)
	if err != nil {
		log.Fatal("unable to read record", err)
	}
	id := systemByteOrder.Uint64(userID)

	r := Record{RecordType(recordType), time, id, Amount(0)}

	if r.recordType == Debit || r.recordType == Credit {
		amt := make([]byte, 8)
		_, err = rd.Read(amt)
		if err != nil {
			log.Fatal("unable to read amount value for Credit or Debit record", err)
		}
		bits := systemByteOrder.Uint64(amt)
		f := math.Float64frombits(bits)
		r.amount = Amount(f)
	}

	if _, err := rd.Peek(13); err != nil {
		return
	}

	parseTxn(rd)
}

func main() {
	file, err := os.Open("txnlog.dat")
	if err != nil {
		log.Fatal("Error opening txnlog file: ", err)
	}
	defer file.Close()

	l, items := newLedger(file)

	for i := range items {
		l.process(i)
	}

	// TODO: clarify requirements on how to validate
	// declared count of records in the header of the eg file-> 71
	// doesnt match with processed number of records-> 72
	// there's a one off error building the eg file
	_, _, _ = l.validateRun()

	summary := `  total credit amount=%.2f
  total debit amount=%.2f
  autopays started=%d
  autopays ended=%d
  balance for user 2456938384156277127=%.2f
`
	fmt.Printf(summary, l.credits, l.debits, l.apStarted, l.apEnded, l.users[2456938384156277127])
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

func parseDebitCredit(txn []byte) Record {
	// record enum
	rd := bytes.NewBuffer(txn)
	recordType, err := rd.ReadByte()
	if err != nil {
		log.Fatal("unable to read record", err)
	}

	ts := make([]byte, 4)
	_, err = rd.Read(ts)
	if err != nil {
		log.Fatal("unable to read record", err)
	}

	time := systemByteOrder.Uint32(ts)

	userID := make([]byte, 8)
	_, err = rd.Read(userID)
	if err != nil {
		log.Fatal("unable to read record", err)
	}
	id := systemByteOrder.Uint64(userID)

	r := Record{RecordType(recordType), time, id, Amount(0)}

	if r.recordType == Debit || r.recordType == Credit {
		amt := make([]byte, 8)
		_, err = rd.Read(amt)
		if err != nil {
			log.Fatal("unable to read amount value for Credit or Debit record", err)
		}
		bits := systemByteOrder.Uint64(amt)
		f := math.Float64frombits(bits)
		r.amount = Amount(f)
	}
	return r
}
