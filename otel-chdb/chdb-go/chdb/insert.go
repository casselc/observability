package chdb

import (
	"fmt"

	chdbpurego "github.com/chdb-io/chdb-go/v2/chdb-purego"
)

// insertConn returns this session's binary-safe surface, or an error saying
// why there is not one. The caller holds the session's read lock.
func (s *Session) insertConn() (chdbpurego.ChdbInsertConn, error) {
	if s.closed || s.conn == nil {
		return nil, fmt.Errorf("chdb: query on a closed session")
	}
	ic, ok := s.conn.(chdbpurego.ChdbInsertConn)
	if !ok {
		return nil, chdbpurego.ErrInsertABIUnavailable
	}
	return ic, nil
}

// QueryBytes is Query for SQL that may hold NUL bytes. It goes through
// chdb_query_n, which takes an explicit length, where Query's statement ends at
// the first zero byte.
func (s *Session) QueryBytes(query []byte, outputFormats ...string) (chdbpurego.ChdbResult, error) {
	outputFormat := "CSV"
	if len(outputFormats) > 0 {
		outputFormat = outputFormats[0]
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ic, err := s.insertConn()
	if err != nil {
		return nil, err
	}
	return ic.QueryBytes(query, outputFormat)
}

// InsertStream begins a streaming INSERT on this session:
//
//	st, err := sess.InsertStream("INSERT INTO db.t (a, b)", "RowBinary")
//	for _, chunk := range chunks {
//		if err := st.Append(chunk); err != nil { st.Cancel(); return err }
//	}
//	return st.Done()
//
// query carries no FORMAT clause and no data. Chunks are raw bytes in format,
// so binary formats (RowBinary, Native, Parquet, Arrow) need no text encoding
// and no escaping. The engine copies each chunk, so a caller can reuse one
// buffer for the whole stream.
//
// The session's native connection runs nothing else until Done or Cancel;
// open a second session on the same path to query concurrently.
func (s *Session) InsertStream(query, format string) (chdbpurego.ChdbInsertStream, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ic, err := s.insertConn()
	if err != nil {
		return nil, err
	}
	return ic.InsertStream(query, format)
}

// Insert is InsertStream with a single chunk.
func (s *Session) Insert(query, format string, data []byte) error {
	st, err := s.InsertStream(query, format)
	if err != nil {
		return err
	}
	if err := st.Append(data); err != nil {
		st.Cancel()
		return err
	}
	return st.Done()
}
