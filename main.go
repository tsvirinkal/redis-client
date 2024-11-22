package main

// #cgo CFLAGS: -I .
// #include <stdlib.h>
// #include <char.h>

import "C"

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
)

//var ctx = context.Background()
var ctx context.Context
var cancelFunc context.CancelFunc
var client *redis.Client
var messageQueue []string // List to store messages
var mutex sync.Mutex      // Mutex for concurrent access to messageQueue
var logFile *os.File

// Initialize logging to a file
func init() {
	var err error
	logFile, err = os.OpenFile("C:\\temp\\redis_client_debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening log file:", err)
	}
	log.SetOutput(logFile)
	log.Println("Debug log initialized")
}

//export SetRedisConfig
func SetRedisConfig(host *C.char, port int) {
	goHost := C.GoString(host)
	initClient(goHost, port)
}

// Initialize the client with a default configuration
func initClient(host string, port int) {
	addr := fmt.Sprintf("%s:%d", host, port)
	client = redis.NewClient(&redis.Options{
		Addr: addr,
	})
	ctx, cancelFunc = context.WithCancel(context.Background())
	log.Printf("Initialized Redis client at %s ctx=%p cancelFunc=%p\n", addr, ctx, cancelFunc)
}

// Subscribe to a Redis channel and store messages in the queue
//export Subscribe
func Subscribe(channel *C.char) {
	ch := C.GoString(channel)
	log.Printf("Subscribing to channel: %s\n", ch)
	sub := client.Subscribe(ctx, ch)
	if sub == nil {
		log.Printf("Failed to subscribe to channel %s: ", ch)
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in subscription goroutine: %v", r)
			}
		}()
		for {
			select {
			case msg, ok := <-sub.Channel():
				if !ok {
					log.Println("Channel closed for subscription: %s", ch)
					return
				}
				addMessageToQueue(msg.Payload)
			case <-ctx.Done():
				_ = sub.Unsubscribe(ctx, ch)
				log.Println("Subscription loop stopped")
				return
			}
		}
	}()
}

// Exported function to retrieve and remove messages from the queue
//export GetNextMessage
func GetNextMessage() *C.char {
	mutex.Lock()
	defer mutex.Unlock()

	if len(messageQueue) == 0 {
		log.Println("No messages in the queue")
		return nil
	}

	// Retrieve the first message and remove it from the queue
	msg := messageQueue[0]
	messageQueue = messageQueue[1:]
	log.Printf("Message retrieved from queue: %s\n", msg)
	return C.CString(msg)
}

// Add a message to the queue
func addMessageToQueue(message string) {
	mutex.Lock()
	defer mutex.Unlock()
	messageQueue = append(messageQueue, message)
	log.Printf("Message added to queue: %s\n", message)
}

//export Cleanup
func Cleanup() {
	log.Println("Starting cleanup")

	// Close the Redis client connection if it's open
	if client != nil {
		log.Println("Closing Redis client connection")
		client.Close()
		client = nil
	}

	// Clear the message queue
	mutex.Lock()
	messageQueue = nil
	mutex.Unlock()
	log.Println("Cleared message queue")

	if cancelFunc != nil {
		cancelFunc() // Cancel any ongoing context operations
		log.Println("Context operations cancelled")
	}

	log.Println("Cleanup completed")

	// Close the log file
	if logFile != nil {
		log.Println("Closing log file")
		logFile.Close()
		logFile = nil
	}
}

func main() {
}
