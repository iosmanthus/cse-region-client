package cse

import (
	"testing"
)

func TestRepeatCloseStore(t *testing.T) {
	s := &store{
		breaker: newAsyncBreaker(settings{}),
	}
	s.Close()
	s.Close()
}
