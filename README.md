simple queue based on redis
=============================


## order queue

```
package main

import (
	"fmt"
	"github.com/qjpcpu/simplequeue"
)

func main() {
	q := qio.NewQueueIO("127.0.0.1:6379", "", "")
	dataCh := make(chan string)
	errCh := make(chan error)
	name := "testq"
	mux := q.ReadMsg(name, dataCh, errCh)
	defer mux.CloseRead()
	session := q.GetSession()
	session.SendString(name, "plain text")
	session.SendJSON(name, map[string]string{"a": "b", "c": "d"})
	session.Close()

	for {
		select {
		case d := <-dataCh:
			fmt.Println("get data:", d)
		case err := <-errCh:
			fmt.Println("get error:", err)
		}
	}
}
```

## clock queue

```
package main

import (
	"fmt"
	"github.com/qjpcpu/simplequeue"
	"time"
)

func main() {
	q := qio.NewQueueIO("127.0.0.1:6379", "", "")
	dataCh := make(chan string)
	errCh := make(chan error)
	name := "testq"
	mux := q.ReadClockMsg(name, qio.TimeScoreGetter{}, dataCh, errCh)
	defer mux.CloseRead()
	session := q.GetSession()
	session.SendClockJSON(name, map[string]string{"a": "b", "c": "d"}, qio.NewTimeScoreGetter(time.Now()))
	session.SendClockString(name, "plain text", qio.NewTimeScoreGetter(time.Now().Add(5*time.Second)))
	session.Close()

	for {
		select {
		case d := <-dataCh:
			fmt.Println("get data:", d)
		case err := <-errCh:
			fmt.Println("get error:", err)
		}
	}
}
```
