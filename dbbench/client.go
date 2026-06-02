package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ── Protocol constants ────────────────────────────────────────────────────────

const (
	cmdPut  byte = 0x01
	cmdGet  byte = 0x02
	cmdDel  byte = 0x03
	cmdPing byte = 0x04
	cmdInfo byte = 0x05
	cmdMPut byte = 0x06
	cmdMGet byte = 0x07
	cmdAuth byte = 0x09

	statusOK       byte = 0x00
	statusErr      byte = 0x01
	statusNotFound byte = 0x02
)

// ── Single connection ─────────────────────────────────────────────────────────

type conn struct {
	nc net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
}

func dial(host string, port int, timeout time.Duration) (*conn, error) {
	nc, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), timeout)
	if err != nil {
		return nil, err
	}
	nc.(*net.TCPConn).SetNoDelay(true)
	return &conn{
		nc: nc,
		r:  bufio.NewReaderSize(nc, 32768),
		w:  bufio.NewWriterSize(nc, 32768),
	}, nil
}

func (c *conn) close() { c.nc.Close() }

func (c *conn) auth(user, pass string) error {
	if err := c.sendCmd(cmdAuth, []byte(user), []byte(pass)); err != nil {
		return err
	}
	_, _, err := c.readResp()
	return err
}

func (c *conn) ping() error {
	if err := c.sendCmd(cmdPing, nil, nil); err != nil {
		return err
	}
	_, _, err := c.readResp()
	return err
}

func (c *conn) info() (string, error) {
	if err := c.sendCmd(cmdInfo, nil, nil); err != nil {
		return "", err
	}
	payload, _, err := c.readResp()
	return string(payload), err
}

func (c *conn) put(key, value []byte) error {
	if err := c.sendCmd(cmdPut, key, value); err != nil {
		return err
	}
	_, _, err := c.readResp()
	return err
}

// get returns (value, found, err). err is non-nil only for I/O failures;
// a missing key returns (nil, false, nil).
func (c *conn) get(key []byte) ([]byte, bool, error) {
	if err := c.sendCmd(cmdGet, key, nil); err != nil {
		return nil, false, err
	}
	payload, found, err := c.readResp()
	return payload, found, err
}

func (c *conn) del(key []byte) error {
	if err := c.sendCmd(cmdDel, key, nil); err != nil {
		return err
	}
	_, _, err := c.readResp()
	return err
}

// multiPut sends a single MPUT frame with all key-value pairs.
// Returns the number of entries that succeeded.
func (c *conn) multiPut(keys, values [][]byte) (int, error) {
	n := len(keys)
	if n == 0 {
		return 0, nil
	}

	// Outer header: [1B cmd][2B 0 LE][4B count LE]
	var hdr [7]byte
	hdr[0] = cmdMPut
	binary.LittleEndian.PutUint16(hdr[1:], 0)
	binary.LittleEndian.PutUint32(hdr[3:], uint32(n))
	c.w.Write(hdr[:])

	// Per-entry: [2B kl LE][4B vl LE][4B ttl=-1 LE][key][value]
	var entHdr [10]byte
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint16(entHdr[0:], uint16(len(keys[i])))
		binary.LittleEndian.PutUint32(entHdr[2:], uint32(len(values[i])))
		// TTL = -1 (no expiry) encoded as int32 little-endian
		binary.LittleEndian.PutUint32(entHdr[6:], 0xFFFFFFFF)
		c.w.Write(entHdr[:])
		c.w.Write(keys[i])
		c.w.Write(values[i])
	}
	if err := c.w.Flush(); err != nil {
		return 0, err
	}

	// Response: [1B status][4B count LE][count × 1B per-entry status]
	var respHdr [5]byte
	if _, err := io.ReadFull(c.r, respHdr[:]); err != nil {
		return 0, err
	}
	if respHdr[0] != statusOK {
		return 0, fmt.Errorf("mput batch error (status=0x%02x)", respHdr[0])
	}
	retCount := int(binary.LittleEndian.Uint32(respHdr[1:]))
	statuses := make([]byte, retCount)
	if _, err := io.ReadFull(c.r, statuses); err != nil {
		return 0, err
	}
	ok := 0
	for _, s := range statuses {
		if s == statusOK {
			ok++
		}
	}
	return ok, nil
}

// multiGet sends a single MGET frame.
// Returns parallel slices: values[i] is nil when found[i] is false.
func (c *conn) multiGet(keys [][]byte) (values [][]byte, found []bool, err error) {
	n := len(keys)
	if n == 0 {
		return nil, nil, nil
	}

	// Outer header: [1B cmd][2B 0 LE][4B count LE]
	var hdr [7]byte
	hdr[0] = cmdMGet
	binary.LittleEndian.PutUint32(hdr[3:], uint32(n))
	c.w.Write(hdr[:])

	// Per-key: [2B kl LE][key]
	var klBuf [2]byte
	for _, k := range keys {
		binary.LittleEndian.PutUint16(klBuf[:], uint16(len(k)))
		c.w.Write(klBuf[:])
		c.w.Write(k)
	}
	if err = c.w.Flush(); err != nil {
		return
	}

	// Response: [1B status][4B count LE] + count × [1B status][4B vl LE][value]
	var respHdr [5]byte
	if _, err = io.ReadFull(c.r, respHdr[:]); err != nil {
		return
	}
	if respHdr[0] != statusOK {
		err = fmt.Errorf("mget batch error (status=0x%02x)", respHdr[0])
		return
	}
	retCount := int(binary.LittleEndian.Uint32(respHdr[1:]))
	values = make([][]byte, retCount)
	found = make([]bool, retCount)

	var entHdr [5]byte
	for i := 0; i < retCount; i++ {
		if _, err = io.ReadFull(c.r, entHdr[:]); err != nil {
			return
		}
		entStatus := entHdr[0]
		valLen := int(binary.LittleEndian.Uint32(entHdr[1:]))
		if entStatus == statusOK && valLen > 0 {
			v := make([]byte, valLen)
			if _, err = io.ReadFull(c.r, v); err != nil {
				return
			}
			values[i] = v
			found[i] = true
		}
	}
	return
}

// ── Private wire helpers ──────────────────────────────────────────────────────

// sendCmd writes [1B cmd][2B kl LE][4B vl LE][key][value] and flushes.
func (c *conn) sendCmd(cmd byte, key, value []byte) error {
	var hdr [7]byte
	hdr[0] = cmd
	binary.LittleEndian.PutUint16(hdr[1:], uint16(len(key)))
	binary.LittleEndian.PutUint32(hdr[3:], uint32(len(value)))
	c.w.Write(hdr[:])
	if len(key) > 0 {
		c.w.Write(key)
	}
	if len(value) > 0 {
		c.w.Write(value)
	}
	return c.w.Flush()
}

// readResp reads [1B status][4B payloadLen LE][payload].
// Returns (payload, found, err). err is only set for I/O failures.
// A NOT_FOUND response returns (nil, false, nil) — not an error.
func (c *conn) readResp() ([]byte, bool, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return nil, false, err
	}
	status := hdr[0]
	payloadLen := int(binary.LittleEndian.Uint32(hdr[1:]))

	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return nil, false, err
		}
	}

	switch status {
	case statusOK:
		return payload, true, nil
	case statusNotFound:
		return nil, false, nil
	default:
		msg := fmt.Sprintf("server error (status=0x%02x)", status)
		if len(payload) > 0 {
			msg = string(payload)
		}
		return nil, false, fmt.Errorf("veltrixdb: %s", msg)
	}
}

// ── Connection pool ───────────────────────────────────────────────────────────

// Pool is a blocking, fixed-size connection pool.
// Each worker goroutine calls Acquire() to get a dedicated connection for the
// duration of the benchmark; it calls Release() when done.
type Pool struct {
	ch chan *conn
	mu sync.Mutex // guards replenish

	host     string
	port     int
	timeout  time.Duration
	username string
	password string
}

func newPool(host string, port, size int, timeout time.Duration, user, pass string) (*Pool, error) {
	p := &Pool{
		ch:       make(chan *conn, size),
		host:     host,
		port:     port,
		timeout:  timeout,
		username: user,
		password: pass,
	}
	for i := 0; i < size; i++ {
		c, err := p.newConn()
		if err != nil {
			p.close()
			return nil, err
		}
		p.ch <- c
	}
	return p, nil
}

func (p *Pool) acquire() *conn {
	return <-p.ch
}

func (p *Pool) release(c *conn) {
	p.ch <- c
}

func (p *Pool) discardAndReplenish(c *conn) {
	c.close()
	if fresh, err := p.newConn(); err == nil {
		p.ch <- fresh
	}
}

func (p *Pool) close() {
	for {
		select {
		case c := <-p.ch:
			c.close()
		default:
			return
		}
	}
}

func (p *Pool) newConn() (*conn, error) {
	c, err := dial(p.host, p.port, p.timeout)
	if err != nil {
		return nil, err
	}
	if p.username != "" {
		if err := c.auth(p.username, p.password); err != nil {
			c.close()
			return nil, fmt.Errorf("AUTH failed: %w", err)
		}
	}
	return c, nil
}
