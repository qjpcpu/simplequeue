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

// timed  queue shortcut
func (q QueueIO) ReadTimedMsg(topic string, dataCh chan<- string, errCh chan<- error, readIntervals ...time.Duration) *Mux {
	return q.ReadDelayMsg(topic, TimeScore{}, dataCh, errCh, readIntervals...)
}

func (s *QueueSession) SendTimedString(topic string, payload string, tm time.Time) error {
	return AddDelayString(s.Conn, queueKey(ns_clock, topic), payload, NewTimeScore(tm).ToScore())
}

func (s *QueueSession) SendTimedJSON(topic string, payload interface{}, tm time.Time) error {
	return AddDelayJSON(s.Conn, queueKey(ns_clock, topic), payload, NewTimeScore(tm).ToScore())
}
