package redis

import "strings"

const (
	connectionWatchState = 1 << iota
	connectionMultiState
)

// commandInfo is used for control the
// state of the redis multi command
type commandInfo struct {
	set, clear int
}

var commandInfos = map[string]commandInfo{
	"WATCH": {set: connectionWatchState},
	"UNWATCH": {clear: connectionWatchState},
	"MULTI": {set: connectionMultiState},
	"EXEC": {clear: connectionMultiState | connectionWatchState},
	"DISCARD": {clear: connectionMultiState | connectionWatchState},
}

func init() {
	for n, ci := range commandInfos {
		commandInfos[strings.ToLower(n)] = ci
	}
}

func lookupCommandInfo(commandName string) commandInfo {
	if ci, ok := commandInfos[commandName]; ok {
		return ci
	}
	return commandInfos[strings.ToUpper(commandName)]
}