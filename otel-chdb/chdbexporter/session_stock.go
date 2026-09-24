//go:build stockchdb

package chdbexporter

import (
	"errors"

	"github.com/chdb-io/chdb-go/v2/chdb"
)

// insertSupported: stock chdb-go has no binary-safe insert. Build with
// -tags stockchdb -modfile=go.stock.mod to check that json and file need
// nothing from the fork.
const insertSupported = false

type stockSession struct{ chdbSession }

func (stockSession) Insert(string, string, []byte) error {
	return errors.New("stock chdb-go has no binary-safe insert")
}

func openSession(path string) (session, error) {
	s, err := chdb.NewSession(path)
	if err != nil {
		return nil, err
	}
	return stockSession{chdbSession{s}}, nil
}
