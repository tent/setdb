package main

import (
	"fmt"
	"os"

	"github.com/jmhodges/levigo"
)

var DB *levigo.DB
var DefaultReadOptions = levigo.NewReadOptions()
var DefaultWriteOptions = levigo.NewWriteOptions()

// Key/Value type identifiers, only append to this list
const (
	MetaKey byte = iota
	StringKey
	HashKey
	ListKey
	SetKey
	ZSetKey
	ZScoreKey
	StringLengthValue
	HashLengthValue
	ListLengthValue
	SetCardValue
	ZCardValue
)

func openDB() {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)

	var err error
	DB, err = levigo.Open("db", opts)
	maybeFatal(err)
}

func maybeFatal(err error) {
	if err != nil {
		fmt.Printf("Fatal error: %s\n", err)
		os.Exit(1)
	}
}

func main() {
	openDB()
	listen()
}
