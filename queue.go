package qio

import (
	"github.com/garyburd/redigo/redis"
	"time"
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

func (s *QueueSession) PopString(topic string, score uint64) (string, error) {
	return PopString(s.Conn, topic, score)
}

func (s *QueueSession) PopJSON(topic string, score uint64, res interface{}) error {
	return PopJSON(s.Conn, topic, score, res)
}

func (s *QueueSession) SendString(topic string, payload string, score uint64) error {
	return AddString(s.Conn, topic, payload, score)
}

func (s *QueueSession) SendJSON(topic string, payload interface{}, score uint64) error {
	return AddJSON(s.Conn, topic, payload, score)
}

func (m *Mux) CloseRead() {
	m.done <- struct{}{}
}

func (q QueueIO) StartRead(topic string, getter QueueScoreGetter, dataCh chan<- string, errCh chan<- error, readIntervals ...time.Duration) *Mux {
	if _, ok := q.topics[topic]; ok {
		panic("duplicate read " + topic)
	}
	q.topics[topic] = struct{}{}
	if dataCh == nil {
		panic("dataCh should not be nil")
	}
	done := make(chan struct{}, 1)
	loop := func() {
		onceFunc := func() (string, error) {
			score, err := getter.CurrentScore()
			if err != nil {
				return "", err
			}
			session := q.GetSession()
			str, err := session.PopString(topic, score)
			session.Close()
			return str, err
		}
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
