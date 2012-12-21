package main

import (
	"fmt"

	"github.com/jmhodges/levigo"
)

var DB *levigo.DB
var DefaultReadOptions = levigo.NewReadOptions()
var DefaultWriteOptions = levigo.NewWriteOptions()
var InvalidDataError = fmt.Errorf("Invalid data")

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
	if err != nil {
		panic(err)
	}
}

func main() {
	openDB()
}
