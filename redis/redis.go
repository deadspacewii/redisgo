// Copyright 2021 DIO

package redis

import "time"

//represent an error return
type Error string

func (e Error) Error() string { return string(e)}

// Connection represents an conn to redis server
type Connection interface {
	// Close close the connection
	Close() error

	// Err return a non-nil error
	Err() error

	// Exec exec command and fetch the reply
	Exec(commandName string, args ...interface{}) (reply interface{}, err error)

	// Send send command to the buffer
	Send(commandName string, args ...interface{}) error

	// Flush flushes the output buffer
	Flush() error

	Receive() (reply interface{}, err error)
}

type ConnectionWithTimeout interface {
	Connection

	// ExecWithTimeout sends a command to the server and returns the received reply.
	// The timeout overrides the read timeout set when dialing the
	// connection.
	ExecWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (interface{}, error)

	// ReceiveWithTimeout receives a single reply from the Redis server. The timeout
	// overrides the read timeout set when dialing the connection.
	ReceiveWithTimeout(timeout time.Duration) (interface{}, error)
}