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

func EqualIgnoreCase(b1, b2 []byte) bool {
	if len(b1) != len(b2) {
		return false
	}
	for i := 0; i < len(b1); i++ {
		c1 := b1[i]
		if 'A' <= c1 && c1 <= 'Z' {
			c1 += 'a' - 'A'
		}
		c2 := b2[i]
		if 'A' <= c2 && c2 <= 'Z' {
			c2 += 'a' - 'A'
		}
		if c1 != c2 {
			return false
		}
	}
	return true
}
