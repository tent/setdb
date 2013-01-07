package main

import (
	"reflect"
	"unsafe"
)

// DANGEROUS! Only use when you know that b will never be modified.
func UnsafeBytesToString(b []byte) string {
	bytesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	strHeader := &reflect.StringHeader{bytesHeader.Data, bytesHeader.Len}
	return *(*string)(unsafe.Pointer(strHeader))
}
