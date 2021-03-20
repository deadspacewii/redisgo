package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	_ ConnectionWithTimeout = (*connection)(nil)
)

type connection struct {
	// common
	mu           sync.Mutex
	pending      int
	err          error
	conn         net.Conn

	// rend
	readTimeout  time.Duration
	br           *bufio.Reader

	// write
	writeTimeout time.Duration
	bw           *bufio.Writer

	// Scratch space for formatting argument length.
	// '*' or '$', length, "\r\n"
	lenScratch [32]byte
}

// DialOption specifies an option for dialing a Redis server.
type DialOption struct {
	f  func(*dialOptions)
}

type dialOptions struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialer       *net.Dialer
	dial         func(network, addr string) (net.Conn, error)
	db           int
	passWord     string
    clientName   string
}

// DialReadTimeout set a timeout of a command reply
func DialReadTimeout(d time.Duration) DialOption {
	return DialOption{f: func(do *dialOptions) {
		do.readTimeout = d
	}}
}

// DialWriteTimeout set a timeout of a write command
func DialWriteTimeout(w time.Duration) DialOption {
	return DialOption{f: func(do *dialOptions) {
		do.writeTimeout = w
	}}
}

// DialConnectTimeout
func DialConnectTimeout(d time.Duration) DialOption {
	return DialOption{f: func(do *dialOptions) {
		do.dialer.Timeout = d
	}}
}

// DialKeepAlive
func DialKeepAlive(d time.Duration) DialOption {
	return DialOption{f: func(do *dialOptions) {
		do.dialer.KeepAlive = d
	}}
}

// DialNetDial set a function of net.Conn
func DialNetDial(dial func(network, addr string) (net.Conn, error)) DialOption {
	return DialOption{f: func(do *dialOptions) {
		do.dial = dial
	}}
}

// DialDataBase set redis db select
func DialDataBase(db int) DialOption {
	return DialOption{f: func(do *dialOptions) {
		do.db = db
	}}
}

// DialPassWord redis auth password
func DialPassWord(passWord string) DialOption {
	return DialOption{f: func(do *dialOptions) {
		do.passWord = passWord
	}}
}

// DialClientName set the tcp connection name
func DialClientName(clientName string) DialOption {
	return DialOption{f: func(do *dialOptions) {
		do.clientName = clientName
	}}
}

// Dial return a TCP conn to the redis
// use option model to fix the param
func Dial(netWork, addr string, options ...DialOption) (Connection, error) {
	do := dialOptions{
		dialer: &net.Dialer{
			KeepAlive: 5 * time.Minute,
		},
	}
	for _, option := range options {
		option.f(&do)
	}
	if do.dial == nil {
		do.dial = do.dialer.Dial
	}

	netConn, err := do.dial(netWork, addr)
	if err != nil {
		return nil, err
	}

	conn := &connection{
		conn:         netConn,
		br:           bufio.NewReader(netConn),
		bw:           bufio.NewWriter(netConn),
		readTimeout:  do.readTimeout,
		writeTimeout: do.writeTimeout,
	}

	return conn, nil
}

// NewConn get a new conn to redis
func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration) Connection {
	return &connection{
		conn:         netConn,
		br:           bufio.NewReader(netConn),
		bw:           bufio.NewWriter(netConn),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

// Close close the TCP connection
func (c *connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("redisgo: closed")
		err = c.conn.Close()
	}
	return err
}

// Err
func (c *connection) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.err
	return err
}

// writeLen write the param length to the buffer
func (c *connection) writeLen(prefix byte, n int) error {
	pre := 0
	c.lenScratch[len(c.lenScratch) - 1] = '\n'
	c.lenScratch[len(c.lenScratch) - 2] = '\r'
	for i := len(c.lenScratch) - 3; i >= 0; i -- {
		c.lenScratch[i] = byte('0' + n % 10)
		n = n / 10
		if n == 0 {
			pre = i - 1
			break
		}
	}
	c.lenScratch[pre] = prefix
	_, err := c.bw.Write(c.lenScratch[pre:])
	if err != nil {
		return err
	}
	return nil
}

// writeString
func (c *connection) writeString(s string) error {
	if err := c.writeLen('$', len(s)); err != nil {
		return err
	}
	c.bw.WriteString(s)
	_, err := c.bw.WriteString("\r\n")
	if err != nil {
		return err
	}
	return nil
}

// writeBytes
func (c *connection) writeBytes(b []byte) error {
	if err := c.writeLen('$', len(b)); err != nil {
		return err
	}
	c.bw.Write(b)
	_, err := c.bw.WriteString("\r\n")
	if err != nil {
		return err
	}
	return nil
}

// writeInt64
func (c *connection) writeInt64(n int64) error {
	num := make([]byte, 40)
	return c.writeBytes(strconv.AppendInt(num, n, 10))
}

// writeFloat64
func (c *connection) writeFloat64(n float64) error {
	num := make([]byte, 40)
	return c.writeBytes(strconv.AppendFloat(num, n, 'g', -1, 64))
}

// writeArg
func (c *connection) writeArg(arg interface{}) (err error) {
	switch arg := arg.(type) {
	case string:
		return c.writeString(arg)
	case []byte:
		return c.writeBytes(arg)
	case int:
		return c.writeInt64(int64(arg))
	case int64:
		return c.writeInt64(arg)
	case float64:
		return c.writeFloat64(arg)
	case bool:
		if arg {
			return c.writeString("1")
		} else {
			return c.writeString("0")
		}
	case nil:
		return c.writeString("")
	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return c.writeBytes(buf.Bytes())
	}
}

// writeCommand
func (c *connection) writeCommand(cmd string, args []interface{}) error {
	if err := c.writeLen('*', len(args) + 1); err != nil {
		return err
	}
	if err := c.writeString(cmd); err != nil {
		return err
	}
	for _, arg := range args {
		if err := c.writeArg(arg); err != nil {
			return err
		}
	}
	return nil
}

type redisError string

func (re redisError) Error() string {
	return fmt.Sprintf("redisgo: %s", string(re))
}

// readLine reads a line of input from the RESP stream.
func (c *connection) readLine() ([]byte, error) {
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		buf := append([]byte{}, p...)
		for err == bufio.ErrBufferFull {
			p, err = c.br.ReadSlice('\n')
			buf = append(buf, p...)
		}
		p = buf
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, redisError("bad respone of terminator")
	}
	return p[:i], nil
}

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, redisError("malformed length")
	}

	if len(p) == 2 && p[0] == '-' && p[1] == '1' {
		return -1, nil
	}
	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, redisError("illegal bytes in length")
		}
		n += int(b - '0')
	}
	return n, nil
}

// parseInt
func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, redisError("malformed integer")
	}
	var negative bool
	if p[0] == '-' {
		negative = true
		p = p[1:]
		if len(p) == 0 {
			return 0, redisError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, redisError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negative {
		n = -n
	}
	return n, nil
}

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)

// read the result
func (c *connection) readReply() (interface{}, error) {
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, redisError("reply too short")
	}
	switch line[0] {
	case '+':
		if len(line) == 3 && line[1] == 'O' && line[2] == 'K' {
			return okReply, nil
		}
		if len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G' {
			return pongReply, nil
		}
		return string(line[1:]), nil
	case '-':
		return Error(line[1:]), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, redisError("short response line")
		}
		p := make([]byte, n)
		_, err = io.ReadFull(c.br, p)
		if err != nil {
			return nil, err
		}
		if line, err = c.readLine(); err != nil {
			return nil, err
	} else if len(line) != 0 {
			return nil, redisError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, redisError("short response line")
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = c.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, redisError("unexpected response line")
}

// Send send the command to the buffer
func (c *connection) Send(commandName string, args ...interface{}) error {
	c.mu.Lock()
	c.pending += 1
	c.mu.Unlock()
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.writeCommand(commandName, args); err != nil {
		return err
	}
	return nil
}

// Flush
func (c *connection) Flush() error {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.bw.Flush(); err != nil {
		return err
	}
	return nil
}

// receiveWithTimeout
func (c *connection) ReceiveWithTimeout(d time.Duration) (interface{}, error) {
	var deadLine time.Time
	if d > 0 {
		deadLine = time.Now().Add(d)
	}
	c.conn.SetReadDeadline(deadLine)

	reply, err := c.readReply()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.pending > 0 {
		c.pending -= 1
	}
	c.mu.Unlock()
	if err, ok := reply.(Error); ok {
		return nil, err
	}
	return reply, nil
}

// Receive
func (c *connection) Receive() (interface{}, error) {
	return c.ReceiveWithTimeout(c.readTimeout)
}


// execWithTimeout
func (c *connection) ExecWithTimeout(readTimeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending := c.pending
	c.pending = 0

	if cmd == "" && pending == 0 {
		return nil, redisError("no command send")
	}

	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	if cmd != "" {
		if err := c.writeCommand(cmd, args); err != nil {
			return nil, err
		}
	}

	if err := c.bw.Flush(); err != nil {
		return nil, err
	}

	var deadline time.Time
	if readTimeout != 0 {
		deadline = time.Now().Add(readTimeout)
	}
	c.conn.SetReadDeadline(deadline)

	if cmd == "" && pending != 0 {
		reply := make([]interface{}, pending)
		for i := range reply {
			r, err := c.readReply()
			if err != nil {
				return nil, err
			}
			reply[i] = r
		}
		return reply, nil
	}

	var err error
	var reply interface{}
	for i := 0; i <= pending; i++ {
		var e error
		if reply, e = c.readReply(); e != nil {
			return nil, e
		}
		if e, ok := reply.(Error); ok && err == nil {
			err = e
		}
	}
	return reply, err
}

// Exec
func (c *connection) Exec(cmd string, args ...interface{}) (interface{}, error) {
	return c.ExecWithTimeout(c.readTimeout, cmd, args...)
}