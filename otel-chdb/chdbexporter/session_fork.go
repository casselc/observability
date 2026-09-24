//go:build !stockchdb

package chdbexporter

import "github.com/chdb-io/chdb-go/v2/chdb"

// insertSupported: this build uses the chdb-go fork, whose Session.Insert
// streams binary data through chdb_stream_insert_n.
const insertSupported = true

func openSession(path string) (session, error) {
	s, err := chdb.NewSession(path)
	if err != nil {
		return nil, err
	}
	return chdbSession{s}, nil
}
