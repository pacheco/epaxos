package main

import (
	"bufio"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"state"
	"time"
)

var serverAddr *string = flag.String("saddr", "127.0.0.1", "Address of the server to connect to")
var serverPort *int = flag.Int("sport", 7070, "Port of the server to connect to")

var keys *int = flag.Int("keys", 1000000, "Total number of keys")
var nConns *int = flag.Int("c", 1, "Number of client connections to create")
var outstanding *int = flag.Int("o", 1, "Number of outstanding requests")

var N int

var rsp []bool

func main() {
	flag.Parse()

	// create nConns client connections. Each will have a number of outstanding operations
	for i := 0; i < *nConns; i++ {
		go clientConnection(fmt.Sprintf("%s:%d", *serverAddr, *serverPort))
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}

func clientConnection(rAddr string) {
	var err error
	log.Printf("Connecting to replica at %s\n", rAddr)
	server, err := net.Dial("tcp", rAddr)
	if err != nil {
		log.Printf("Error connecting to replica at %s\n", rAddr)
	}
	reader := bufio.NewReader(server)
	writer := bufio.NewWriter(server)

	pending := make(map[int32]time.Time)

	// send outstanding msgs
	for count := 0; count < *outstanding; count++ {
		propose(pending, writer)
	}

	// wait for replies and propose again
	for {
		reply := new(genericsmrproto.ProposeReplyTS)
		if err := reply.Unmarshal(reader); err != nil {
			log.Println("Error reading: ", err)
			continue
		}
		if reply.OK != 0 {
			start := pending[reply.CommandId]
			delete(pending, reply.CommandId)
			dur_us := time.Since(start) / time.Microsecond
			fmt.Printf("%d %d l 0\n", start.UnixNano()/1000, dur_us)
		} else {
			log.Println("Reply error: ", reply.OK)
		}
		// send another msg
		propose(pending, writer)
	}
}

func propose(pending map[int32]time.Time, w *bufio.Writer) {
	id := rand.Int31()
	key := rand.Int63n(int64(*keys))
	val := key
	args := genericsmrproto.Propose{id, state.Command{state.PUT, state.Key(key), state.Value(val)}, 0}
	pending[id] = time.Now()
	w.WriteByte(genericsmrproto.PROPOSE)
	args.Marshal(w)
	w.Flush()
}
