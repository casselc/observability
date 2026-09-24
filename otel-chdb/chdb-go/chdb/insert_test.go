package chdb

import (
	"encoding/binary"
	"strings"
	"testing"
)

// rowBinary encodes (UInt32, String) rows the way RowBinary lays them out:
// fixed-width little-endian integers, strings as LEB128 length then bytes.
func rowBinary(buf []byte, id uint32, s string) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, id)
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func insertSession(t *testing.T) *Session {
	t.Helper()
	s, err := NewSession()
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	t.Cleanup(s.Close)
	if _, err := s.Query("CREATE DATABASE IF NOT EXISTS ins"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Query("CREATE OR REPLACE TABLE ins.t (id UInt32, s String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestInsertRowBinaryWithNulBytes(t *testing.T) {
	s := insertSession(t)
	// Id 0 encodes as four zero bytes and the strings hold NULs: every one of
	// them would end a C string, which is why Query cannot carry this.
	var buf []byte
	buf = rowBinary(buf, 0, "a\x00b")
	buf = rowBinary(buf, 1, "")
	buf = rowBinary(buf, 2, strings.Repeat("\x00", 3))
	if err := s.Insert("INSERT INTO ins.t (id, s)", "RowBinary", buf); err != nil {
		t.Fatalf("insert: %v", err)
	}
	res, err := s.Query("SELECT id, hex(s) FROM ins.t ORDER BY id", "CSV")
	if err != nil {
		t.Fatal(err)
	}
	want := "0,\"610062\"\n1,\"\"\n2,\"000000\"\n"
	if got := res.String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestInsertStreamManyChunksReusedBuffer(t *testing.T) {
	s := insertSession(t)
	st, err := s.InsertStream("INSERT INTO ins.t (id, s)", "RowBinary")
	if err != nil {
		t.Fatal(err)
	}
	var buf []byte
	for chunk := 0; chunk < 50; chunk++ {
		buf = buf[:0] // the engine copies each chunk, so the buffer is reused
		for i := 0; i < 200; i++ {
			buf = rowBinary(buf, uint32(chunk*200+i), "row")
		}
		if err := st.Append(buf); err != nil {
			st.Cancel()
			t.Fatal(err)
		}
	}
	if err := st.Done(); err != nil {
		t.Fatal(err)
	}
	res, err := s.Query("SELECT count(), sum(id) FROM ins.t", "CSV")
	if err != nil {
		t.Fatal(err)
	}
	if got := res.String(); got != "10000,49995000\n" {
		t.Fatalf("got %q", got)
	}
}

func TestInsertStreamBadDataFailsAndSessionRecovers(t *testing.T) {
	s := insertSession(t)
	// Three bytes cannot be a UInt32 followed by a string.
	err := s.Insert("INSERT INTO ins.t (id, s)", "RowBinary", []byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected an error for truncated RowBinary")
	}
	if err := s.Insert("INSERT INTO ins.t (id, s)", "RowBinary", rowBinary(nil, 7, "ok")); err != nil {
		t.Fatalf("session unusable after a failed stream: %v", err)
	}
}

func TestInsertStreamCancelCommitsNothingAppendedLater(t *testing.T) {
	s := insertSession(t)
	st, err := s.InsertStream("INSERT INTO ins.t (id, s)", "RowBinary")
	if err != nil {
		t.Fatal(err)
	}
	st.Cancel()
	st.Cancel() // idempotent
	if err := st.Append(rowBinary(nil, 1, "x")); err == nil {
		t.Fatal("append after cancel should fail")
	}
	if _, err := s.Query("SELECT 1"); err != nil {
		t.Fatalf("session unusable after cancel: %v", err)
	}
}

func TestInsertStreamBadQuery(t *testing.T) {
	s := insertSession(t)
	if _, err := s.InsertStream("INSERT INTO ins.missing (id)", "RowBinary"); err == nil {
		t.Fatal("expected an error for a missing table")
	}
	if _, err := s.Query("SELECT 1"); err != nil {
		t.Fatalf("session unusable after a failed begin: %v", err)
	}
}

func TestQueryBytesKeepsNul(t *testing.T) {
	s := insertSession(t)
	res, err := s.QueryBytes([]byte("SELECT length('a\x00b')"), "CSV")
	if err != nil {
		t.Fatal(err)
	}
	if got := res.String(); got != "3\n" {
		t.Fatalf("got %q, want 3", got)
	}
}
