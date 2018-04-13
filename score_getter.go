package qio

import (
	"time"
)

type QueueScoreGetter interface {
	CurrentScore() (uint64, error)
}

type TimeScoreGetter struct{}

func (t TimeScoreGetter) CurrentScore() (uint64, error) {
	return uint64(time.Now().Unix()), nil
}

type ZeroScoreGetter struct{}

func (t ZeroScoreGetter) CurrentScore() (uint64, error) {
	return 0, nil
}
