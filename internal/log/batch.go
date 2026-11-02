// Package log implements a partition: an append-only sequence of record
// batches stored in segment files.
package log

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Batch layout (little endian). The same bytes are stored on disk and sent
// over the wire.
//
//	0  baseOffset u64   assigned by the log on append
//	8  length     u32   bytes after this field
//	12 crc        u32   CRC-32C of bytes [16, end)
//	16 count      u32   records in the batch
//	20 firstTs    i64   unix milliseconds
//	28 maxTs      i64
//	36 records    count x { keyLen varint (-1 = nil), key, valueLen uvarint, value }
const (
	HeaderSize   = 36
	lengthOffset = 12 // bytes covered by the length field start here
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)

	ErrCorrupt = errors.New("corrupt batch")
	ErrShort   = errors.New("truncated batch")
)

// Record is one key/value pair. Key may be nil.
type Record struct {
	Key   []byte
	Value []byte
}

// Builder accumulates records into a batch.
type Builder struct {
	buf     []byte
	count   uint32
	firstTs int64
	maxTs   int64
}

func NewBuilder() *Builder {
	b := &Builder{}
	b.Reset()
	return b
}

func (b *Builder) Reset() {
	b.buf = append(b.buf[:0], make([]byte, HeaderSize)...)
	b.count = 0
	b.firstTs, b.maxTs = 0, 0
}

func (b *Builder) Add(r Record, tsMillis int64) {
	if b.count == 0 {
		b.firstTs = tsMillis
	}
	if tsMillis > b.maxTs {
		b.maxTs = tsMillis
	}
	if r.Key == nil {
		b.buf = binary.AppendVarint(b.buf, -1)
	} else {
		b.buf = binary.AppendVarint(b.buf, int64(len(r.Key)))
		b.buf = append(b.buf, r.Key...)
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(len(r.Value)))
	b.buf = append(b.buf, r.Value...)
	b.count++
}

func (b *Builder) Count() int { return int(b.count) }
func (b *Builder) Size() int  { return len(b.buf) }

// Bytes finalizes the header (base offset 0) and returns the batch. The
// slice is valid until the next Reset.
func (b *Builder) Bytes() []byte {
	h := b.buf
	binary.LittleEndian.PutUint64(h[0:], 0)
	binary.LittleEndian.PutUint32(h[8:], uint32(len(h)-lengthOffset))
	binary.LittleEndian.PutUint32(h[16:], b.count)
	binary.LittleEndian.PutUint64(h[20:], uint64(b.firstTs))
	binary.LittleEndian.PutUint64(h[28:], uint64(b.maxTs))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[16:], castagnoli))
	return h
}

// Header is a parsed batch header.
type Header struct {
	BaseOffset uint64
	Length     uint32 // total batch size is Length + 12
	CRC        uint32
	Count      uint32
	FirstTs    int64
	MaxTs      int64
}

func (h Header) Size() int          { return int(h.Length) + lengthOffset }
func (h Header) NextOffset() uint64 { return h.BaseOffset + uint64(h.Count) }

func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrShort
	}
	h := Header{
		BaseOffset: binary.LittleEndian.Uint64(b[0:]),
		Length:     binary.LittleEndian.Uint32(b[8:]),
		CRC:        binary.LittleEndian.Uint32(b[12:]),
		Count:      binary.LittleEndian.Uint32(b[16:]),
		FirstTs:    int64(binary.LittleEndian.Uint64(b[20:])),
		MaxTs:      int64(binary.LittleEndian.Uint64(b[28:])),
	}
	if h.Size() < HeaderSize || h.Count == 0 {
		return h, ErrCorrupt
	}
	return h, nil
}

// Validate checks that b is exactly one well-formed batch: length, CRC, and
// that the records parse to the declared count.
func Validate(b []byte) (Header, error) {
	h, err := ParseHeader(b)
	if err != nil {
		return h, err
	}
	if len(b) < h.Size() {
		return h, ErrShort
	}
	if len(b) != h.Size() {
		return h, ErrCorrupt
	}
	if crc32.Checksum(b[16:], castagnoli) != h.CRC {
		return h, ErrCorrupt
	}
	n := 0
	err = EachRecord(b, func(uint64, Record) bool { n++; return true })
	if err != nil || n != int(h.Count) {
		return h, ErrCorrupt
	}
	return h, nil
}

// SetBaseOffset rewrites the base offset (not covered by the CRC).
func SetBaseOffset(b []byte, off uint64) { binary.LittleEndian.PutUint64(b[0:], off) }

// EachRecord calls fn for every record of one batch with its offset. It
// stops early if fn returns false.
func EachRecord(b []byte, fn func(offset uint64, r Record) bool) error {
	h, err := ParseHeader(b)
	if err != nil {
		return err
	}
	if len(b) < h.Size() {
		return ErrShort
	}
	p := b[HeaderSize:h.Size()]
	for i := uint32(0); i < h.Count; i++ {
		kl, n := binary.Varint(p)
		if n <= 0 || kl < -1 {
			return ErrCorrupt
		}
		p = p[n:]
		var key []byte
		if kl >= 0 {
			if int64(len(p)) < kl {
				return ErrCorrupt
			}
			key, p = p[:kl:kl], p[kl:]
		}
		vl, n := binary.Uvarint(p)
		if n <= 0 || uint64(len(p)-n) < vl {
			return ErrCorrupt
		}
		p = p[n:]
		val := p[:vl:vl]
		p = p[vl:]
		if !fn(h.BaseOffset+uint64(i), Record{Key: key, Value: val}) {
			return nil
		}
	}
	if len(p) != 0 {
		return ErrCorrupt
	}
	return nil
}

// EachBatch walks consecutive batches in b, stopping at a partial batch at
// the end (as returned by fetches cut at maxBytes).
func EachBatch(b []byte, fn func(batch []byte) error) error {
	for len(b) >= HeaderSize {
		h, err := ParseHeader(b)
		if err != nil {
			return err
		}
		if len(b) < h.Size() {
			return nil
		}
		if err := fn(b[:h.Size()]); err != nil {
			return err
		}
		b = b[h.Size():]
	}
	return nil
}
