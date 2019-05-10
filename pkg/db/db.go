package db

import (
	"database/sql"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/spf13/pflag"
)

var (
	driver, dsn string
	DB          *sql.DB
	firstConnOk = false
	lock        sync.Mutex
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

	glog.Info("Connecting to database")

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
			glog.Fatal("Database connection failed.")
		}
	}
	firstConnOk = true
	glog.Info("Connected to database.")
}

// Tries to connect to database, returns true iff the connection is successful
func tryConnect() bool {
	var err error

	glog.V(4).Info("sql.Open(\"", driver, "\", ...)")
	DB, err = sql.Open(driver, dsn)
	if err != nil {
		glog.Error("Failed to connect to database (", dsn, "): ", err)
		return false
	}
	glog.V(4).Info("sql.Ping()")
	if err = DB.Ping(); err != nil {
		glog.Error("Failed to ping database (driver ", driver, ", ", dsn, "): ", err)
		return false
	}
	return true
}

func Close() {
	if DB == nil {
		return
	}

	glog.Info("Closing connection to database...")

	lock.Lock()
	defer lock.Unlock()

	glog.V(4).Info("sql.Close()")
	if err := DB.Close(); err != nil {
		glog.Error("error closing connection: ", err)
	}
	DB = nil
	glog.V(1).Info("Database connection closed.")
}

func Reconnect() {
	Close()
	Connect()
}
