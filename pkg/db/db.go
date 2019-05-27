package db

import (
	"database/sql"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
)

var (
	driver, dsn string
	DB          *sql.DB
	firstConnOk = false
	lock        sync.Mutex

	Debug = false
)

func RegisterFlags(defaultDriver, defaultConnStr string, flag *pflag.FlagSet) {
	// Driver
	driver = os.Getenv("DATA_SOURCE_DRIVER")
	if driver == "" {
		driver = defaultDriver
	}
	flag.StringVar(&driver, "data-source-driver", driver, "The driver to use to connect to the data source (env: DATA_SOURCE_DRIVER)")

	// Database connection
	dsn = os.Getenv("DATA_SOURCE")
	if dsn == "" {
		dsn = defaultConnStr
	}
	flag.StringVar(&dsn, "data-source", dsn, "The data source name in the form user/pass@host:port/sid (env: DATA_SOURCE)")
}

func Connect() {
	if DB != nil {
		return
	}

	log.Print("Connecting to database")

	lock.Lock()
	defer lock.Unlock()

	if !strings.HasSuffix(os.Getenv("NLS_LANG"), "UTF8") {
		os.Setenv("NLS_LANG", "AMERICAN_AMERICA.AL32UTF8")
	}

	for !tryConnect() {
		// On first try, a connection failure is fatal (validation).
		// When reconnecting, we retry forever to connect.
		if firstConnOk {
			time.Sleep(1 * time.Second)
			continue
		} else {
			log.Fatal("Database connection failed.")
		}
	}
	firstConnOk = true
	log.Print("Connected to database.")
}

// Tries to connect to database, returns true iff the connection is successful
func tryConnect() bool {
	var err error

	if Debug {
		log.Print("sql.Open(\"", driver, "\", ...)")
	}

	DB, err = sql.Open(driver, dsn)
	if err != nil {
		log.Print("Failed to connect to database (", dsn, "): ", err)
		return false
	}

	if Debug {
		log.Print("sql.Ping()")
	}

	if err = DB.Ping(); err != nil {
		log.Print("Failed to ping database: ", err)
		return false
	}
	return true
}

func Close() {
	if DB == nil {
		return
	}

	log.Print("Closing connection to database...")

	lock.Lock()
	defer lock.Unlock()

	if Debug {
		log.Print("sql.Close()")
	}

	if err := DB.Close(); err != nil {
		log.Print("error closing connection: ", err)
	}
	DB = nil
}

func Reconnect() {
	Close()
	Connect()
}
