# TxnLog

## How to run

`go run .`

## Design

The solution is a modified version of a "state function" pattern that Rob Pike explained in this talk: <https://talks.golang.org/2011/lex.slide#1>
`type stateFn func(*ledger) stateFn`

There are two types of state functions:

`func readHeader(l *ledger) stateFn` - A read header reads the header and passes the `readRecord` as the next state function
`func readRecord(l *ledger) stateFn` - A readRecord recursively calls itself until it exhausts reading the file then is done

I am using a go routine to read the `txnlog.dat` while the main go routines does the execution of the state functions as data
arrives in a channel.

## Tests

Manual tests
