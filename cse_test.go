package cse

import (
	"testing"
)

func TestRepeatCloseStore(t *testing.T) {
	s := &store{
		breaker: newAsyncBreaker(settings{}),
	}
	s.breaker.Close()
	s.breaker.Close()
}
