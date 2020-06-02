package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
)

// RecordType is a byte type to track transaction records
type RecordType byte

// Record type enumeration
// - 0x00: Debit
// - 0x01: Credit
// - 0x02: StartAutopay
// - 0x03: EndAutopay
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

// item represents the line item identified by the recordType rt
type item struct {
	rt  RecordType
	buf []byte
}

// stateFn is a function that takes a ledger and returns the next state function
type stateFn func(*ledger) stateFn

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
