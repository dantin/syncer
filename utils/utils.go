package utils

import (
	"fmt"
	"runtime"
)

// Version information
var (
	Version = "None"
	BuildTS = "None"
	GitHash = "None"
)

func GetRawInfo(app string) string {
	info := ""
	info += fmt.Sprintf("%s: v%s\n", app, Version)
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("UTC Build Time: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s\n", runtime.Version())
	return info
}
