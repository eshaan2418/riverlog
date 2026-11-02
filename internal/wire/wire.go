// Package wire is riverlog's binary protocol.
//
// Every message is a frame: u32 length (of what follows), then
//
//	request:  u16 api, u32 correlation id, payload
//	response: u32 correlation id, u16 error code, payload
//
// Requests on one connection may be pipelined. Responses carry the
// correlation id and can arrive out of order (a long-poll fetch does not
// hold up a produce). Integers are little endian. Strings are u16 length +
// bytes, byte blobs are u32 length + bytes.
package wire

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type API uint16

const (
	CreateTopic API = iota + 1
	Metadata
	Produce
	Fetch
	ListOffsets
	JoinGroup
	Heartbeat
	LeaveGroup
	CommitOffset
	FetchOffset
)

type Code uint16

const (
	OK Code = iota
	UnknownTopic
	OffsetOutOfRange
	CorruptBatch
	RebalanceInProgress
	IllegalGeneration
	UnknownMember
	Internal
	BadRequest
	TopicExists
)

var codeNames = map[Code]string{
	OK: "ok", UnknownTopic: "unknown topic or partition", OffsetOutOfRange: "offset out of range",
	CorruptBatch: "corrupt batch", RebalanceInProgress: "rebalance in progress",
	IllegalGeneration: "illegal generation", UnknownMember: "unknown member", Internal: "internal error",
	BadRequest: "bad request", TopicExists: "topic exists",
}

// Error is a protocol error returned by the broker.
type Error struct{ Code Code }

func (e *Error) Error() string { return "riverlog: " + codeNames[e.Code] }

// IsCode reports whether err is a broker error with the given code.
func IsCode(err error, c Code) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == c
}

const MaxFrame = 64 << 20

// ReadFrame reads one frame body into buf (grown as needed).
func ReadFrame(r *bufio.Reader, buf []byte) ([]byte, error) {
	var lb [4]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lb[:])
	if n > MaxFrame {
		return nil, fmt.Errorf("frame of %d bytes exceeds limit", n)
	}
	if cap(buf) < int(n) {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// Enc appends values to a byte slice.
type Enc struct{ B []byte }

func (e *Enc) U16(v uint16)  { e.B = binary.LittleEndian.AppendUint16(e.B, v) }
func (e *Enc) U32(v uint32)  { e.B = binary.LittleEndian.AppendUint32(e.B, v) }
func (e *Enc) U64(v uint64)  { e.B = binary.LittleEndian.AppendUint64(e.B, v) }
func (e *Enc) Str(s string)  { e.U16(uint16(len(s))); e.B = append(e.B, s...) }
func (e *Enc) Blob(b []byte) { e.U32(uint32(len(b))); e.B = append(e.B, b...) }

// Request starts a request frame; call Finish when the payload is written.
func Request(api API, corr uint32) *Enc {
	e := &Enc{B: make([]byte, 4, 64)}
	e.U16(uint16(api))
	e.U32(corr)
	return e
}

// Response starts a response frame.
func Response(corr uint32, code Code) *Enc {
	e := &Enc{B: make([]byte, 4, 64)}
	e.U32(corr)
	e.U16(uint16(code))
	return e
}

// Finish fills in the frame length and returns the frame.
func (e *Enc) Finish() []byte {
	binary.LittleEndian.PutUint32(e.B[0:], uint32(len(e.B)-4))
	return e.B
}

// Dec reads values; the first error sticks and later reads return zeros.
type Dec struct {
	B   []byte
	Err error
}

var errShort = errors.New("short message")

func (d *Dec) take(n int) []byte {
	if d.Err != nil || len(d.B) < n {
		if d.Err == nil {
			d.Err = errShort
		}
		return make([]byte, n)
	}
	v := d.B[:n]
	d.B = d.B[n:]
	return v
}

func (d *Dec) U16() uint16 { return binary.LittleEndian.Uint16(d.take(2)) }
func (d *Dec) U32() uint32 { return binary.LittleEndian.Uint32(d.take(4)) }
func (d *Dec) U64() uint64 { return binary.LittleEndian.Uint64(d.take(8)) }
func (d *Dec) Str() string { return string(d.take(int(d.U16()))) }
func (d *Dec) Blob() []byte {
	n := d.U32()
	if n > MaxFrame {
		d.Err = errShort
		return nil
	}
	return d.take(int(n))
}

// TopicPartition names one partition.
type TopicPartition struct {
	Topic     string
	Partition uint32
}
