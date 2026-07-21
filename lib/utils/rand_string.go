package utils

import (
	"math/rand/v2"
	"time"
)

var r = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString create a random string no longer than n
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[r.IntN(len(letters))]
	}
	return string(b)
}

var hexLetters = []rune("0123456789abcdef")

func RandHexString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = hexLetters[r.IntN(len(hexLetters))]
	}
	return string(b)
}

// RandIndex returns random indexes to random pick elements from slice
func RandIndex(size int) []int {
	result := make([]int, size)
	for i := range result {
		result[i] = i
	}
	rand.Shuffle(size, func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return result
}
