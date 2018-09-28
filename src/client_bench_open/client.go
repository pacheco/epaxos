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
	"sync"
	"time"
)

var serverAddr *string = flag.String("saddr", "127.0.0.1", "Address of the server to connect to")
var serverPort *int = flag.Int("sport", 7070, "Port of the server to connect to")

var keys *int = flag.Int("keys", 1000000, "Total number of keys")
var nConns *int = flag.Int("c", 1, "Number of client connections to create")
var opsSec *int = flag.Int("o", 1, "Target operations per second per client connection")

var N int

var rsp []bool

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	// create nConns client connections. Each will try to reach its opsSec target.
	for i := 0; i < *nConns; i++ {
		startClientConnection(fmt.Sprintf("%s:%d", *serverAddr, *serverPort))
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}

func startClientConnection(rAddr string) {
	var err error
	log.Printf("Connecting to replica at %s\n", rAddr)
	server, err := net.Dial("tcp", rAddr)
	if err != nil {
		log.Printf("Error connecting to replica at %s\n", rAddr)
	}
	reader := bufio.NewReader(server)
	writer := bufio.NewWriter(server)

	pending := make(map[int32]time.Time)
	pending_lock := new(sync.Mutex)

	go clientWriter(writer, pending, pending_lock)
	go clientReader(reader, pending, pending_lock)
}

func clientWriter(w *bufio.Writer, pending map[int32]time.Time, m *sync.Mutex) {
	count := int64(0)
	start := time.Now()

	for {
		time.Sleep(100 * time.Millisecond)
		elapsed_ms := time.Since(start) / time.Millisecond
		target := (elapsed_ms * (time.Duration(*opsSec))) / 1000
		for ; count < int64(target); count++ {
			now := time.Now()
			id := rand.Int31()
			key := rand.Int63n(int64(*keys))
			val := key
			args := genericsmrproto.Propose{id, state.Command{state.PUT, state.Key(key), state.Value(val)}, 0}
			m.Lock()
			pending[id] = now
			m.Unlock()
			w.WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(w)
			w.Flush()
		}
	}
}

func clientReader(r *bufio.Reader, pending map[int32]time.Time, m *sync.Mutex) {
	for {
		reply := new(genericsmrproto.ProposeReplyTS)
		if err := reply.Unmarshal(r); err != nil {
			log.Println("Error reading: ", err)
			continue
		}
		if reply.OK != 0 {
			m.Lock()
			start := pending[reply.CommandId]
			delete(pending, reply.CommandId)
			dur_us := time.Since(start) / time.Microsecond
			fmt.Printf("%d %d l 0\n", start.UnixNano()/1000, dur_us)
			m.Unlock()
		} else {
			log.Println("Reply error: ", reply.OK)
		}
	}
}
