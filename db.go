package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/jmhodges/levigo"
)

var DB *levigo.DB
var DefaultReadOptions = levigo.NewReadOptions()
var DefaultWriteOptions = levigo.NewWriteOptions()
var ReadWithoutCacheFill = levigo.NewReadOptions()

func openDB() {
	opts := levigo.NewOptions()
	cache := levigo.NewLRUCache(128 * 1024 * 1024) // 128MB cache
	opts.SetCache(cache)
	filter := levigo.NewBloomFilter(10)
	opts.SetFilterPolicy(filter)
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
	runtime.GOMAXPROCS(runtime.NumCPU())
	openDB()
	listen()
}

func init() {
	ReadWithoutCacheFill.SetFillCache(false)
}
