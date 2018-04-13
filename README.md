simple queue based on redis
=============================


## unorder queue

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
	mux := q.StartRead(name, qio.ZeroScoreGetter{}, dataCh, errCh)
	defer mux.CloseRead()
	session := q.GetSession()
	session.SendString(name, "plain text", 0)
	session.SendJSON(name, map[string]string{"a": "b", "c": "d"}, 0)
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
	mux := q.StartRead(name, qio.TimeScoreGetter{}, dataCh, errCh)
	defer mux.CloseRead()
	session := q.GetSession()
	session.SendJSON(name, map[string]string{"a": "b", "c": "d"}, uint64(time.Now().Add(5*time.Second).Unix()))
	session.SendString(name, "plain text", uint64(time.Now().Unix()))
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
