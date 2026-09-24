package chdbpurego

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// ErrInsertABIUnavailable is returned by the binary-safe entry points when the
// loaded libchdb does not export chdb_query_n and the chdb_stream_insert
// family.
var ErrInsertABIUnavailable = errors.New("chdb: the loaded libchdb does not export chdb_query_n / chdb_stream_insert_n")

// ChdbInsertConn is the binary-safe surface a connection offers when the
// engine exports it.
//
// Query and QueryStreaming pass their SQL as Go strings, which purego hands to
// the engine as NUL-terminated C strings: the first zero byte ends the
// statement. That rules out inline binary payloads (RowBinary, Native,
// Parquet, Arrow) and makes every insert pay for a text encoding. The two
// methods here take explicit lengths instead.
//
// Like ChdbAdminConn it is a second interface rather than more methods on
// ChdbConn, so existing implementations of ChdbConn keep compiling; callers
// reach it with a type assertion, which is also how they learn the engine is
// too old.
type ChdbInsertConn interface {
	// QueryBytes runs query through chdb_query_n. query may hold NUL bytes.
	QueryBytes(query []byte, format string) (ChdbResult, error)

	// InsertStream begins a streaming INSERT. query is the statement with no
	// FORMAT clause and no data ("INSERT INTO db.t (a, b)"); format names the
	// input format of the chunks passed to Append ("RowBinary", "Native",
	// "JSONEachRow", ...).
	//
	// The stream holds the connection until Done or Cancel returns: the engine
	// accepts no other statement on it meanwhile, and Close waits for it.
	// Exactly one of Done or Cancel must be called.
	InsertStream(query, format string) (ChdbInsertStream, error)
}

// ChdbInsertStream is an open streaming INSERT.
type ChdbInsertStream interface {
	// Append pushes one chunk of format-encoded data. The engine copies it, so
	// the caller may reuse data as soon as Append returns. It may block while
	// the engine applies backpressure.
	Append(data []byte) error
	// Done signals end of input, commits, and releases the stream.
	Done() error
	// Cancel aborts without committing and releases the stream. Safe to call
	// after Done, where it does nothing.
	Cancel()
}

// InsertABIAvailable reports whether the loaded engine exports the
// binary-safe query and streaming insert symbols. It loads the library if
// needed.
func InsertABIAvailable() (bool, error) {
	if err := ensureLoaded(); err != nil {
		return false, err
	}
	return insertABIAvailable(), nil
}

func insertABIAvailable() bool {
	return chdbQueryN != nil && chdbStreamInsertN != nil
}

// bindInsertSymbols binds chdb_query_n and the streaming insert family when
// the loaded engine has them. Probed, not declared, for the reason given on
// bindAdminSymbols: RegisterLibFunc panics on a missing symbol. All or none,
// since a stream that can begin but not finish is worse than no stream.
func bindInsertSymbols(libchdb uintptr) {
	names := []string{
		"chdb_query_n", "chdb_stream_insert_n", "chdb_stream_append",
		"chdb_stream_done", "chdb_stream_cancel_insert",
		"chdb_stream_insert_error", "chdb_destroy_insert_stream",
	}
	for _, n := range names {
		if !has(libchdb, n) {
			return
		}
	}
	purego.RegisterLibFunc(&chdbStreamAppend, libchdb, "chdb_stream_append")
	purego.RegisterLibFunc(&chdbStreamDone, libchdb, "chdb_stream_done")
	purego.RegisterLibFunc(&chdbStreamCancelInsert, libchdb, "chdb_stream_cancel_insert")
	purego.RegisterLibFunc(&chdbStreamInsertError, libchdb, "chdb_stream_insert_error")
	purego.RegisterLibFunc(&chdbDestroyInsertStream, libchdb, "chdb_destroy_insert_stream")
	// Last two gate insertABIAvailable, so bind them after the rest.
	purego.RegisterLibFunc(&chdbStreamInsertN, libchdb, "chdb_stream_insert_n")
	purego.RegisterLibFunc(&chdbQueryN, libchdb, "chdb_query_n")
}

// QueryBytes implements ChdbInsertConn.
func (c *connection) QueryBytes(query []byte, formatStr string) (ChdbResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn == nil {
		return nil, fmt.Errorf("invalid connection")
	}
	if !insertABIAvailable() {
		return nil, ErrInsertABIUnavailable
	}
	f, fLen, fBuf := cString(formatStr)
	res := chdbQueryN(c.conn.internal_data, bytesPtr(query), uint(len(query)), f, fLen)
	runtime.KeepAlive(query)
	runtime.KeepAlive(fBuf)
	if res == nil {
		return newChdbResult(res), nil
	}
	if msg := chdbResultError(res); msg != "" {
		chdbDestroyQueryResult(res)
		return nil, errors.New(msg)
	}
	return newChdbResult(res), nil
}

// InsertStream implements ChdbInsertConn. On success the connection's read
// lock stays held until the stream's Done or Cancel, so a Close waits for the
// stream rather than freeing the connection underneath it.
func (c *connection) InsertStream(query, formatStr string) (ChdbInsertStream, error) {
	c.mu.RLock()
	if c.conn == nil {
		c.mu.RUnlock()
		return nil, fmt.Errorf("invalid connection")
	}
	if !insertABIAvailable() {
		c.mu.RUnlock()
		return nil, ErrInsertABIUnavailable
	}
	q, qLen, qBuf := cString(query)
	f, fLen, fBuf := cString(formatStr)
	h := chdbStreamInsertN(c.conn.internal_data, q, qLen, f, fLen)
	runtime.KeepAlive(qBuf)
	runtime.KeepAlive(fBuf)
	if h == nil {
		c.mu.RUnlock()
		return nil, errors.New("chdb: chdb_stream_insert_n returned no stream")
	}
	if msg := chdbStreamInsertError(h); msg != "" {
		chdbDestroyInsertStream(h)
		c.mu.RUnlock()
		return nil, errors.New(msg)
	}
	return &insertStream{conn: c, h: h}, nil
}

type insertStream struct {
	once sync.Once
	conn *connection
	h    *chdb_insert_stream
}

func (s *insertStream) Append(data []byte) error {
	if s.h == nil {
		return errors.New("chdb: append on a finished insert stream")
	}
	if len(data) == 0 {
		return nil
	}
	state := chdbStreamAppend(s.h, unsafe.Pointer(&data[0]), uint(len(data)))
	runtime.KeepAlive(data)
	if state != 0 {
		if msg := chdbStreamInsertError(s.h); msg != "" {
			return errors.New(msg)
		}
		return errors.New("chdb: chdb_stream_append failed")
	}
	return nil
}

func (s *insertStream) Done() error {
	if s.h == nil {
		return errors.New("chdb: insert stream already finished")
	}
	var err error
	s.once.Do(func() {
		res := chdbStreamDone(s.h)
		if res == nil {
			if msg := chdbStreamInsertError(s.h); msg != "" {
				err = errors.New(msg)
			} else {
				err = errors.New("chdb: chdb_stream_done returned no result")
			}
		} else {
			if msg := chdbResultError(res); msg != "" {
				err = errors.New(msg)
			}
			chdbDestroyQueryResult(res)
		}
		s.release()
	})
	return err
}

func (s *insertStream) Cancel() {
	s.once.Do(func() {
		chdbStreamCancelInsert(s.h)
		s.release()
	})
}

func (s *insertStream) release() {
	chdbDestroyInsertStream(s.h)
	s.h = nil
	s.conn.mu.RUnlock()
}

func bytesPtr(b []byte) *byte {
	if len(b) == 0 {
		return nil
	}
	return &b[0]
}
