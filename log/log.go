package log

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	LogNone = iota
	LogInfo
	LogWarning
	LogError
	LogVerbose
	LogDebug
)

var LogMap = map[int]string{
	LogInfo:    "LogInfo",
	LogWarning: "LogWarning",
	LogError:   "LogError",
	LogVerbose: "LogVerbose",
	LogDebug:   "LogDebug",
}

type MyFileLogger struct {
	logger   *log.Logger
	logFile  *os.File
	logLevel int
}

func NewFileLogger() *MyFileLogger {
	return &MyFileLogger{
		logger:   nil,
		logFile:  nil,
		logLevel: LogNone,
	}
}

func StartLogger(file string) *MyFileLogger {
	res := &MyFileLogger{
		logger:   nil,
		logFile:  nil,
		logLevel: LogNone,
	}
	if err := res.StartLog(LogNone, file); err != nil {
		panic(err.Error())
	}
	return res
}

func (myLogger *MyFileLogger) StartLog(level int, file string) error {
	if len(file) == 0 {
		return nil
	}
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err
	}
	myLogger.logger = log.New(f, "", 0)
	myLogger.logLevel = level
	myLogger.logFile = f
	return nil
}
func (myLogger *MyFileLogger) StopLog() error {
	if myLogger.logFile != nil {
		return myLogger.logFile.Close()
	}
	return nil
}

// You can add a log of auxiliary functions here to make the log more easier
func (myLogger *MyFileLogger) Log(level int, msg string, a ...interface{}) error {
	if myLogger.logFile == nil {
		return nil
	}
	msg = fmt.Sprintf(msg, a...)
	msg = fmt.Sprintf("%s %s: %s\n", time.Now().Format("2006-01-02 15:04:05.000 "), LogMap[level], msg)
	if myLogger.logger == nil {
		return errors.New("MyFileLogger is not initialized correctly")
	}
	if level >= myLogger.logLevel {
		myLogger.logger.Print(msg) // maybe you want to include the loglevel here, modify it as you want
	}
	return nil
}
