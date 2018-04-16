package qio

import (
	"time"
)

type QueueScoreGetter interface {
	// current score, used by simplequeue for element query
	CurrentScore() (uint64, error)
	// set clock score,used by user for add clock element
	ToScore() uint64
}

type TimeScoreGetter time.Time

func NewTimeScoreGetter(tm time.Time) TimeScoreGetter {
	return TimeScoreGetter(tm)
}

func (t TimeScoreGetter) CurrentScore() (uint64, error) {
	return uint64(time.Now().Unix()), nil
}

func (t TimeScoreGetter) ToScore() uint64 {
	return uint64(time.Time(t).Unix())
}
