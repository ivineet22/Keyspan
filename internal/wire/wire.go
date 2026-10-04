// Package wire is the small conversation between the Go router and a C++ shard.
//
// One call uses one TCP connection. A frame is a 4-byte length, then that many
// bytes. Integers are little-endian: the least significant byte comes first.
// A string or a byte blob is a 4-byte length followed by the bytes.
package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	methodGet     = 1
	methodPut     = 2
	methodScan    = 3
	methodSync    = 4
	methodFence   = 5
	methodDrop    = 6
	methodPull    = 7
	methodInstall = 8
	methodObserve = 9
)

const (
	statusOK          = 0
	statusNotFound    = 1
	statusWrongShard  = 2
	statusStaleEpoch  = 3
	statusUnavailable = 4
	statusInvalid     = 5
	statusRetry       = 6
)

const maxFrame = 4 << 20

var (
	ErrNotFound    = errors.New("not found")
	ErrWrongShard  = errors.New("wrong shard")
	ErrStaleEpoch  = errors.New("stale epoch")
	ErrUnavailable = errors.New("range unavailable")
	// ErrRetry means the owner refused the put because the range is fenced.
	// The value was not stored. Call again after the move finishes.
	ErrRetry = errors.New("retry")
)

// Record is one log entry returned by Scan.
type Record struct {
	Position uint64
	Key      string
	Value    []byte
	Deleted  bool
}

func Put(ctx context.Context, addr, tenant, key string, value []byte, epoch int64) error {
	var body []byte
	body = append(body, methodPut)
	body = appendI64(body, epoch)
	body = appendStr(body, tenant)
	body = appendStr(body, key)
	body = appendBytes(body, value)
	conn, _, err := roundTrip(ctx, addr, body)
	if conn != nil {
		conn.Close()
	}
	return err
}

func Get(ctx context.Context, addr, tenant, key string, epoch int64) ([]byte, error) {
	var body []byte
	body = append(body, methodGet)
	body = appendI64(body, epoch)
	body = appendStr(body, tenant)
	body = appendStr(body, key)
	conn, payload, err := roundTrip(ctx, addr, body)
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		return nil, err
	}
	value, _, err := readBytes(payload, 0)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func Sync(ctx context.Context, addr string) (uint64, error) {
	conn, payload, err := roundTrip(ctx, addr, []byte{methodSync})
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		return 0, err
	}
	if len(payload) < 8 {
		return 0, fmt.Errorf("short sync reply")
	}
	return binary.LittleEndian.Uint64(payload), nil
}

func Fence(ctx context.Context, addr, tenant string, hashStart, hashEnd, epoch int64) error {
	body := []byte{methodFence}
	body = appendI64(body, epoch)
	body = appendI64(body, hashStart)
	body = appendI64(body, hashEnd)
	body = appendStr(body, tenant)
	conn, _, err := roundTrip(ctx, addr, body)
	if conn != nil {
		conn.Close()
	}
	return err
}

func DropRange(ctx context.Context, addr, tenant string, hashStart, hashEnd, newEpoch int64) error {
	body := []byte{methodDrop}
	body = appendI64(body, newEpoch)
	body = appendI64(body, hashStart)
	body = appendI64(body, hashEnd)
	body = appendStr(body, tenant)
	conn, _, err := roundTrip(ctx, addr, body)
	if conn != nil {
		conn.Close()
	}
	return err
}

// Pull asks the destination to scan the source and keep the records in a
// pending log. The caller does not receive the record bytes.
func Observe(ctx context.Context, addr string) error {
	conn, _, err := roundTrip(ctx, addr, []byte{methodObserve})
	if conn != nil {
		conn.Close()
	}
	return err
}

func Pull(ctx context.Context, dest, source, tenant string, hashStart, hashEnd int64, after, through uint64, epoch int64, moveID string) (count uint64, durable uint64, err error) {
	body := []byte{methodPull}
	body = appendI64(body, epoch)
	body = appendI64(body, hashStart)
	body = appendI64(body, hashEnd)
	body = appendU64(body, after)
	body = appendU64(body, through)
	body = appendStr(body, tenant)
	body = appendStr(body, source)
	body = appendStr(body, moveID)
	conn, payload, err := roundTrip(ctx, dest, body)
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		return 0, 0, err
	}
	if len(payload) < 16 {
		return 0, 0, fmt.Errorf("short pull reply")
	}
	return binary.LittleEndian.Uint64(payload[:8]), binary.LittleEndian.Uint64(payload[8:16]), nil
}

func Install(ctx context.Context, addr, tenant, moveID string, hashStart, hashEnd int64) error {
	body := []byte{methodInstall}
	body = appendI64(body, hashStart)
	body = appendI64(body, hashEnd)
	body = appendStr(body, tenant)
	body = appendStr(body, moveID)
	conn, _, err := roundTrip(ctx, addr, body)
	if conn != nil {
		conn.Close()
	}
	return err
}

func Scan(ctx context.Context, addr, tenant string, hashStart, hashEnd int64, after, through uint64, epoch int64) ([]Record, error) {
	var body []byte
	body = append(body, methodScan)
	body = appendI64(body, epoch)
	body = appendI64(body, hashStart)
	body = appendI64(body, hashEnd)
	body = appendU64(body, after)
	body = appendU64(body, through)
	body = appendStr(body, tenant)
	conn, _, err := roundTrip(ctx, addr, body)
	if err != nil && conn == nil {
		return nil, err
	}
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return nil, err
	}
	var out []Record
	for {
		frame, err := readFrame(conn)
		if err != nil {
			return nil, err
		}
		if len(frame) == 0 {
			return out, nil
		}
		rec, err := decodeRecord(frame)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
}

func roundTrip(ctx context.Context, addr string, body []byte) (net.Conn, []byte, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, ErrUnavailable
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := writeFrame(conn, body); err != nil {
		conn.Close()
		return nil, nil, ErrUnavailable
	}
	reply, err := readFrame(conn)
	if err != nil {
		conn.Close()
		return nil, nil, ErrUnavailable
	}
	if len(reply) < 1 {
		conn.Close()
		return nil, nil, fmt.Errorf("empty shard reply")
	}
	if err := statusErr(reply[0]); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, reply[1:], nil
}

func decodeRecord(frame []byte) (Record, error) {
	if len(frame) < 9 {
		return Record{}, fmt.Errorf("short scan record")
	}
	var rec Record
	rec.Position = binary.LittleEndian.Uint64(frame)
	rec.Deleted = frame[8] == 1
	key, n, err := readStr(frame, 9)
	if err != nil {
		return Record{}, err
	}
	value, _, err := readBytes(frame, n)
	if err != nil {
		return Record{}, err
	}
	rec.Key = key
	rec.Value = value
	return rec, nil
}

func statusErr(s byte) error {
	switch s {
	case statusOK:
		return nil
	case statusNotFound:
		return ErrNotFound
	case statusWrongShard:
		return ErrWrongShard
	case statusStaleEpoch:
		return ErrStaleEpoch
	case statusUnavailable:
		return ErrUnavailable
	case statusInvalid:
		return fmt.Errorf("bad request")
	case statusRetry:
		return ErrRetry
	default:
		return fmt.Errorf("shard status %d", s)
	}
}

func appendI64(b []byte, v int64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	return append(b, buf[:]...)
}

func appendU64(b []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}

func appendStr(b []byte, s string) []byte {
	return appendBytes(b, []byte(s))
}

func appendBytes(b []byte, p []byte) []byte {
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(p)))
	b = append(b, n[:]...)
	return append(b, p...)
}

func readStr(b []byte, at int) (string, int, error) {
	p, n, err := readBytes(b, at)
	return string(p), n, err
}

func readBytes(b []byte, at int) ([]byte, int, error) {
	if at+4 > len(b) {
		return nil, 0, fmt.Errorf("short length")
	}
	n := int(binary.LittleEndian.Uint32(b[at:]))
	at += 4
	if n < 0 || at+n > len(b) {
		return nil, 0, fmt.Errorf("short bytes")
	}
	return append([]byte(nil), b[at:at+n]...), at + n, nil
}

func writeFrame(w io.Writer, body []byte) error {
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(body)))
	if _, err := w.Write(n[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(n[:])
	if size > maxFrame {
		return nil, fmt.Errorf("frame too large")
	}
	if size == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
