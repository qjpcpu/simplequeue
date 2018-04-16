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

type TimeScore time.Time

func NewTimeScore(tm time.Time) TimeScore {
	return TimeScore(tm)
}

func (t TimeScore) CurrentScore() (uint64, error) {
	return uint64(time.Now().Unix()), nil
}

func (t TimeScore) ToScore() uint64 {
	return uint64(time.Time(t).Unix())
}
