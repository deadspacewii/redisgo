package redis

import (
	"errors"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

var nowFunc = time.Now // for check poolCoon is timeout

var ErrPoolExhausted = errors.New("redigo: connection pool exhausted")

var (
	errPoolClose = errors.New("redisgo: connection pool is closed")
	errConnClose = errors.New("redisgo: connection is closed")
	errTimeoutNotSupported = errors.New("redisgo: connection does not support ConnWithTimeout")
)

type Pool struct {
	// Dial return the connection which not in
	// special states
	Dial func() (Connection, error)

	// DialContext return the connection with
	// context
	DialContext func(ctx context.Context) (Connection, error)

	MaxIdle int

	MaxActive int

	Wait bool

	chInitlized uint32

	List idleList

	mu sync.Mutex
	active int
	closed bool
	MaxConnLifetime time.Duration
	ch chan struct{}
	waitCount int64
	waitDuratime time.Duration
}

type PoolOpetion struct {
	f func(*Pool)
}

// NewPool initlized and use options function to fix pool
func NewPool(newFunc func() (Connection, error), maxIdle int, options ...PoolOpetion) *Pool {
	p := &Pool{Dial: newFunc, MaxIdle: maxIdle}
	for _, op := range options {
		op.f(p)
	}
	return p
}

// PoolInitlizeWait is the option of initlize ch
// used in waiting goroutine beyoung pool's MaxActive
func PoolInitlizeWait(maxActive int, wait bool) PoolOpetion {
	return PoolOpetion{ f: func(p *Pool) {
		p.MaxActive = maxActive
		p.Wait = wait
		if p.MaxActive > 0 && p.Wait {
			p.initlized()
		}
	}}
}

func (p *Pool) initlized() {
	if atomic.LoadUint32(&p.chInitlized) == 1 {
		return
	}
	p.mu.Lock()
	if p.chInitlized == 0 {
		p.ch = make(chan struct{}, p.MaxActive)
		if p.closed {
			close(p.ch)
		} else {
			for i := 0; i < p.MaxActive; i ++ {
				p.ch <- struct{}{}
			}
		}
		atomic.StoreUint32(&p.chInitlized, 1)
	}
	p.mu.Unlock()
}

func (p *Pool) GetActive() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

func (p *Pool) GetIdleCount() int {
	p.mu.Lock()
	idleCount := p.MaxIdle
	p.mu.Unlock()
	return idleCount
}

func (p *Pool) Get() Connection {
	pc, err := p.get(nil)
	if err != nil {
		return errorConn{err: err}
	}
	return &activeCoon{pc: p, p: pc}
}

func (p *Pool) GetContext(ctx context.Context) (Connection, error) {
	pc, err := p.get(ctx)
	if err != nil {
		return errorConn{err: err}, err
	}
	return &activeCoon{pc: p, p: pc}, nil
}

func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.active -= p.List.count
	pc := p.List.front
	p.List.count = 0
	p.List.front, p.List.back = nil, nil
	if p.ch != nil {
		close(p.ch)
	}
	p.mu.Unlock()
	for ; pc != nil; pc = pc.next {
		pc.c.Close()
	}
	return nil
}

func (p *Pool) get(ctx context.Context) (*poolCoon, error) {
	var waited time.Duration
	if p.Wait && p.MaxActive > 0 {
		wait := len(p.ch) == 0
		var start time.Time
		if wait {
			start = time.Now()
		}
		if ctx == nil {
			<- p.ch
		} else {
			select {
			case <- p.ch:
			case <- ctx.Done():
				return nil, ctx.Err()
			}
		}
		if wait {
			waited = time.Since(start)
		}
	}
	p.mu.Lock()
	if waited > 0 {
		p.waitCount += 1
		p.waitDuratime += waited
	}

	for p.List.front != nil {
		pc := p.List.front
		p.List.popFront()
		p.mu.Unlock()
		if p.MaxConnLifetime == 0 || nowFunc().Sub(pc.created) < p.MaxConnLifetime {
			return pc, nil
		}
		pc.c.Close()
		p.mu.Lock()
		p.active--
	}

	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("redisgo: get on closed pool")
	}

	if !p.Wait && p.MaxActive > 0 && p.active > p.MaxActive {
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	}

	p.active++
	p.mu.Unlock()
	conn, err := p.dial(ctx)
	if err != nil {
		conn = nil
		p.mu.Lock()
		p.active--
		if p.ch != nil && !p.closed {
			p.ch <- struct{}{}
		}
		p.mu.Unlock()
	}
	return &poolCoon{c: conn, created: nowFunc()}, err
}

func (p *Pool) dial(ctx context.Context) (Connection, error) {
	if p.DialContext != nil {
		return p.DialContext(ctx)
	}
	if p.Dial != nil {
		return p.Dial()
	}
	return nil, errors.New("redisgo: must pass Dial or DialContext to pool")
}

func (p *Pool) put(pc *poolCoon, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && !forceClose {
		pc.t = nowFunc()
		p.List.pushFront(pc)
		if p.List.count > p.MaxActive {
			pc = p.List.back
			p.List.popBack()
		} else {
			pc = nil
		}
	}

	if pc != nil {
		p.mu.Unlock()
		pc.c.Close()
		p.mu.Lock()
		p.active--
	}

	if p.ch != nil && !p.closed {
		p.ch <- struct{}{}
	}
	p.mu.Unlock()
	return nil
}

type activeCoon struct {
	pc    *Pool
	p     *poolCoon
	state int
}

func (ac *activeCoon) Close() error {
	pc := ac.p
	if pc == nil {
		return nil
	}
	ac.p = nil

	if ac.state&connectionMultiState != 0 {
		pc.c.Send("DISCARD")
		ac.state &^= lookupCommandInfo("DISCARD").clear
	} else if ac.state&connectionWatchState != 0 {
		pc.c.Send("UNWATCH")
		ac.state &^= lookupCommandInfo("UNWATCH").clear
	}

	pc.c.Exec("")
	ac.pc.put(pc, ac.state != 0 || pc.c.Err() != nil)
	ac.pc = nil
	return nil
}

func (ac *activeCoon) Err() error {
	pc := ac.p
	if pc == nil {
		return errConnClose
	}
	return pc.c.Err()
}

func (ac *activeCoon) Exec(commandName string, args ...interface{}) (reply interface{}, err error) {
	pc := ac.p
	if pc == nil {
		return nil, errConnClose
	}
	ci := lookupCommandInfo(commandName)
	ac.state &^= ci.clear
	return pc.c.Exec(commandName, args...)
}

func (ac *activeCoon) Send(commandName string, args ...interface{}) error {
	pc := ac.p
	if pc == nil {
		return errConnClose
	}
	ci := lookupCommandInfo(commandName)
	ac.state &^= ci.clear
	return pc.c.Send(commandName, args...)
}

func (ac *activeCoon) Flush() error {
	pc := ac.p
	if pc == nil {
		return errConnClose
	}
	return pc.c.Flush()
}

func (ac *activeCoon) Receive() (reply interface{}, err error) {
	pc := ac.p
	if pc == nil {
		return nil, errConnClose
	}
	return pc.c.Receive()
}

func (ac *activeCoon) ExecWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (interface{}, error) {
	pc := ac.p
	if pc == nil {
		return nil, errConnClose
	}
	conwt, ok := pc.c.(ConnectionWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}
	ci := lookupCommandInfo(commandName)
	ac.state &^= ci.clear
	return conwt.ExecWithTimeout(timeout, commandName, args...)
}

func (ac *activeCoon) ReceiveWithTimeout(timeout time.Duration) (interface{}, error) {
	pc := ac.p
	if pc == nil {
		return nil, errConnClose
	}
	conwt, ok := pc.c.(ConnectionWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}
	return conwt.ReceiveWithTimeout(timeout)
}


type errorConn struct{ err error }

func (ec errorConn) Exec(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConn) DoWithTimeout(time.Duration, string, ...interface{}) (interface{}, error) {
	return nil, ec.err
}
func (ec errorConn) Send(string, ...interface{}) error                     { return ec.err }
func (ec errorConn) Err() error                                            { return ec.err }
func (ec errorConn) Close() error                                          { return nil }
func (ec errorConn) Flush() error                                          { return ec.err }
func (ec errorConn) Receive() (interface{}, error)                         { return nil, ec.err }
func (ec errorConn) ReceiveWithTimeout(time.Duration) (interface{}, error) { return nil, ec.err }


type idleList struct {
	count int
	front, back *poolCoon
}

type poolCoon struct {
	c Connection
	t time.Time
	created time.Time
	prev, next *poolCoon
}

func (l *idleList) pushFront(pc *poolCoon) {
	pc.next = l.front
	pc.prev = nil
	if l.count == 0 {
		l.back = pc
	} else {
		l.front.prev = pc
	}
	l.front = pc
	l.count ++
	return
}

func (l *idleList) popFront() {
	pc := l.front
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.next.prev = nil
		l.front = pc.next
	}
	pc.prev, pc.next = nil, nil
}

func (l *idleList) popBack() {
	pc := l.back
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.prev.next = nil
		l.back = pc.prev
	}
	pc.prev, pc.next = nil, nil
}