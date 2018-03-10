package log

import (
	"log"
	"os"
)

// Logger interface
type Logger interface {
	// Printf calls Output to print to the logger.
	Printf(format string, v ...interface{})
}

// New return a simple logger
func New() *log.Logger {
	return log.New(os.Stdout, "", log.Lshortfile)
}
