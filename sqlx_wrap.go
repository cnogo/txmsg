package txmsg

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

type SqlxDBWrap struct {
	*sqlx.DB
	Url string
}

func NewSqlxDBWrap(db *sqlx.DB, dbName, host string, port int64) *SqlxDBWrap {

	return &SqlxDBWrap{
		Url: DBKey(dbName, host, port),
		DB: db,
	}
}

func DBKey(dbName, host string, port int64) string {
	return fmt.Sprintf("%s:%d:%s", host, port, dbName)
}
