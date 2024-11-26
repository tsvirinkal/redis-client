package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
	"os"
	"strings"
	"unsafe"

	"github.com/go-redis/redis/v8"
)

var client *redis.Client
var logFile *os.File
var dynamicString *C.char

// Initialize the client with a default configuration
//export InitClient
func InitClient(host *C.char, port int) bool {
	addr := fmt.Sprintf("%s:%d", C.GoString(host), port)
	client = redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return client != nil
}

//export ReadMessage
func ReadMessage(stream *C.char, startId *C.char) unsafe.Pointer {
	var ctx = context.Background()
	goStream := C.GoString(stream)
	goStartId := C.GoString(startId)
	streams, err := client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{goStream, goStartId},
		Block:   -1, // do not wait
		Count:   1,  // Number of messages to fetch
	}).Result()

	if err == nil {
		// Process the messages
		for _, stream := range streams {
			for _, message := range stream.Messages {
				for key, value := range message.Values {
					combinedResult := fmt.Sprintf("%s|%s=%s", message.ID, key, value)
					dynamicString = C.CString(toWideChars(combinedResult))
					return unsafe.Pointer(dynamicString)
				}
			}
		}
	}

	// If no messages are found, return an empty string
	return unsafe.Pointer(C.CString(toWideChars("")))
}

//export SendMessage
func SendMessage(stream *C.char, key *C.char, value *C.char) bool {
	var ctx = context.Background()
	goStream := C.GoString(stream)
	goKey := C.GoString(key)
	goValue := C.GoString(value)
	values := map[string]interface{}{
		goKey: goValue,
	}

	_, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: goStream,
		Values: values,
	}).Result()

	return err == nil
}

//export FreeString
func FreeString() {
	if dynamicString != nil {
		C.free(unsafe.Pointer(dynamicString))
		dynamicString = nil
	}
}

//export Cleanup
func Cleanup() {
	// Close the Redis client connection if it's open
	if client != nil {
		client.Close()
		client = nil
	}
}

func toWideChars(input string) string {
	chars := strings.Split(input, "")
	return strings.Join(chars, " ") + string('\x00') + string('\x00')
}

func main() {
}
