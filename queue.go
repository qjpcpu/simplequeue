package qio

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	ns_clock = "c"
	ns_norm  = "n"
)

type QueueIO struct {
	redis_pool *redis.Pool
	topics     map[string]struct{}
}

type QueueSession struct {
	Conn redis.Conn
}

type Mux struct {
	done chan struct{}
}

func NewQueueIOByPool(pool *redis.Pool) QueueIO {
	return QueueIO{redis_pool: pool, topics: make(map[string]struct{})}
}

func NewQueueIO(conn, redis_db, passwd string) QueueIO {
	pool := &redis.Pool{
		MaxIdle:     200,
		MaxActive:   200,
		IdleTimeout: 2 * time.Second,
		Dial: func() (redis.Conn, error) {
			connect_timeout := 2 * time.Second
			read_timeout := 2 * time.Second
			write_timeout := 2 * time.Second
			c, err := redis.DialTimeout("tcp", conn, connect_timeout,
				read_timeout, write_timeout)
			if err != nil {
				return nil, err
			}

			if passwd != "" {
				if _, err := c.Do("AUTH", passwd); err != nil {
					c.Close()
					return nil, err
				}
			}

			if redis_db != "" {
				if _, err = c.Do("SELECT", redis_db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return NewQueueIOByPool(pool)
}

func (q QueueIO) GetSession() *QueueSession {
	return &QueueSession{Conn: q.redis_pool.Get()}
}

func (s *QueueSession) Close() {
	s.Conn.Close()
}

func (s *QueueSession) PopClockString(topic string, score uint64) (string, error) {
	return PopClockString(s.Conn, queueKey(ns_clock, topic), score)
}

func (s *QueueSession) SendClockString(topic string, payload string, getter QueueScoreGetter) error {
	return AddClockString(s.Conn, queueKey(ns_clock, topic), payload, getter.ToScore())
}

func (s *QueueSession) SendClockJSON(topic string, payload interface{}, getter QueueScoreGetter) error {
	return AddClockJSON(s.Conn, queueKey(ns_clock, topic), payload, getter.ToScore())
}

func (s *QueueSession) PopString(topic string) (string, error) {
	return PopString(s.Conn, queueKey(ns_norm, topic))
}

func (s *QueueSession) SendString(topic string, payload string) error {
	return AddString(s.Conn, queueKey(ns_norm, topic), payload)
}

func (s *QueueSession) SendJSON(topic string, payload interface{}) error {
	return AddJSON(s.Conn, queueKey(ns_norm, topic), payload)
}

func (m *Mux) CloseRead() {
	m.done <- struct{}{}
}

func (q QueueIO) ReadClockMsg(topic string, getter QueueScoreGetter, dataCh chan<- string, errCh chan<- error, readIntervals ...time.Duration) *Mux {
	onceFunc := func() (string, error) {
		if r := recover(); r != nil {
			if errCh != nil {
				errCh <- fmt.Errorf("panic occur when read msg:%v", r)
			}
		}
		score, err := getter.CurrentScore()
		if err != nil {
			return "", err
		}
		session := q.GetSession()
		defer session.Close()
		return session.PopClockString(topic, score)
	}
	q.markTopicReadable(queueKey(ns_clock, topic))
	return q.readMsgLoop(onceFunc, dataCh, errCh, readIntervals...)
}

func (q QueueIO) ReadMsg(topic string, dataCh chan<- string, errCh chan<- error, readIntervals ...time.Duration) *Mux {
	onceFunc := func() (string, error) {
		session := q.GetSession()
		str, err := session.PopString(topic)
		session.Close()
		return str, err
	}
	q.markTopicReadable(queueKey(ns_norm, topic))
	return q.readMsgLoop(onceFunc, dataCh, errCh, readIntervals...)
}

func (q QueueIO) markTopicReadable(topic string) {
	if _, ok := q.topics[topic]; ok {
		panic("duplicate read " + topic)
	}
	q.topics[topic] = struct{}{}
}

func (q QueueIO) readMsgLoop(onceFunc func() (string, error), dataCh chan<- string, errCh chan<- error, readIntervals ...time.Duration) *Mux {
	if onceFunc == nil {
		panic("null read func")
	}
	if dataCh == nil {
		panic("dataCh should not be nil")
	}
	done := make(chan struct{}, 1)
	loop := func() {
		// loop interval duration
		defaultInterval := 1 * time.Second
		if len(readIntervals) > 0 {
			defaultInterval = readIntervals[0]
		}

		for {
			var period time.Duration
			str, err := onceFunc()
			if err != nil {
				if errCh != nil && err != redis.ErrNil {
					errCh <- err
				}
				period = defaultInterval
			} else {
				dataCh <- str
				period = 0 * time.Second
			}
			select {
			case <-time.After(period):
			case <-done:
				close(done)
				return
			}
		}
	}
	go loop()
	return &Mux{done: done}
}

func queueKey(namespace, topic string) string {
	return fmt.Sprintf("%s:%s", topic, namespace)
}
