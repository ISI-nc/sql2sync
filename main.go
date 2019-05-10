package main

import (
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/mcluseau/sql2sync/pkg/cmd/sql2sync"
	"github.com/mcluseau/sql2sync/pkg/db"
)

func main() {
	cmd := sql2sync.New()

	db.RegisterFlags("postgres", "postgres://postgres@localhost/default", cmd.PersistentFlags())

	cmd.Execute()
}
