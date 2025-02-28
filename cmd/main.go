package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/cockroachdb/pebble"
)

const (
	redisOK     = "+OK\r\n"
	redisNil    = "$-1\r\n"
	redisPrefix = "*"
)

var (
	db   *pebble.DB
	lock sync.RWMutex
)

func main() {
	var err error
	db, err = pebble.Open("pebble_data", &pebble.Options{})
	if err != nil {
		log.Fatalf("Failed to open Pebble DB: %v", err)
	}
	defer db.Close()

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	log.Println("Redis-compatible server running on :6379")
	// Handle SIGTERM for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	quitCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	var wg sync.WaitGroup
	go func() {
		<-sigCh
		log.Println("Received shutdown signal, closing server...")
		close(quitCh)    // Notify all goroutines to stop
		listener.Close() // Stop accepting new connections
		wg.Wait()        // Wait for all connections to close
		db.Flush()
		log.Println("Server shutdown complete")
		os.Exit(0)
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		wg.Add(1)
		go handleConnection(conn, &wg)
	}

}

func handleConnection(conn net.Conn, wg *sync.WaitGroup) {
	defer func() {
		log.Printf("Client disconnected: %s", conn.RemoteAddr().String())
		conn.Close()
		wg.Done()
	}()

	reader := bufio.NewReader(conn)
	// conn.Write([]byte("+Hello! Welcome to Pebble-Redis.\r\n"))

	for {
		cmd, args, err := parseRESP(reader)
		if err != nil {
			conn.Write([]byte("-ERR Parse error\r\n"))
			return
		}
		response := handleCommand(cmd, args)
		conn.Write([]byte(response))
	}
}

func parseRESP(reader *bufio.Reader) (string, []string, error) {
	// Read the first line to determine the command type
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", []string{}, err
	}

	log.Printf("Command: %q", line)
	line = strings.TrimSpace(line)
	log.Printf("Line: %q", line)

	// Handle simple strings (single-line commands like PING)
	if !strings.HasPrefix(line, "*") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			return "", nil, fmt.Errorf("empty command")
		}
		return parts[0], parts[1:], nil
	}

	// Handle RESP arrays (multi-line commands like SET key value)
	numArgs := 0
	fmt.Sscanf(line, "*%d", &numArgs)

	args := make([]string, 0, numArgs)
	for i := 0; i < numArgs; i++ {
		_, err := reader.ReadString('\n') // Read length (skip it)
		if err != nil {
			return "", nil, err
		}
		arg, err := reader.ReadString('\n') // Read actual argument
		if err != nil {
			return "", nil, err
		}
		args = append(args, strings.TrimSpace(arg))
	}

	if len(args) == 0 {
		return "", nil, fmt.Errorf("invalid command format")
	}

	return strings.ToLower(args[0]), args[1:], nil
}

func handleCommand(cmd string, args []string) string {
	log.Printf("Executing command: %s, Args: %v", cmd, args)

	switch cmd {
	case "ping":
		return "+PONG\r\n"
	case "set":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'set' command\r\n"
		}
		key := args[0]
		value := args[1]
		err := db.Set([]byte(key), []byte(value), &pebble.WriteOptions{
			Sync: false,
		})
		if err != nil {
			return "-ERR Failed to set key: " + err.Error() + "\r\n"
		}
		return "+OK\r\n"
	case "get":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'get' command\r\n"
		}
		res, closer, err := db.Get([]byte(args[0]))
		if err != nil {
			if err == pebble.ErrNotFound {
				return "$-1\r\n" // RESP representation for nil
			}
			return "-ERR Failed to get key: " + err.Error() + "\r\n"
		}
		defer closer.Close()
		return fmt.Sprintf("$%d\r\n%s\r\n", len(res), res)
	default:
		return "-ERR unknown command\r\n"
	}
}
