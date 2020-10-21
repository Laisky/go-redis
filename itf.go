package redis

import "time"

// RdbItf redis SDK
type RdbItf interface {
	Get(string) RdbStringCmdItf
	LPop(string) RdbStringCmdItf
	LLen(string) RdbIntCmdItf
	LTrim(string, int64, int64) RdbStatusCmdItf
	MGet(...string) RdbSliceCmdItf
	Del(...string) RdbIntCmdItf
	Set(string, interface{}, time.Duration) RdbStatusCmdItf
	Scan(uint64, string, int64) RdbScanCmdItf
	RPush(string, ...interface{}) RdbIntCmdItf
}

// RdbStringCmdItf string result
type RdbStringCmdItf interface {
	Result() (string, error)
	Err() error
}

// RdbStatusCmdItf string result
type RdbStatusCmdItf interface {
	Result() (string, error)
	Err() error
}

// RdbIntCmdItf string result
type RdbIntCmdItf interface {
	Result() (int64, error)
	Err() error
}

// RdbScanCmdItf string result
type RdbScanCmdItf interface {
	Result() ([]string, uint64, error)
	Err() error
}

// RdbSliceCmdItf string result
type RdbSliceCmdItf interface {
	Result() ([]interface{}, error)
	Err() error
}
