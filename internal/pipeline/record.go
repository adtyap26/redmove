package pipeline

import "time"

// Record is the unit of data flowing through the pipeline.
type Record struct {
	Key    string
	Type   string         // "string", "hash", "list", "set", "zset", "stream", "json"
	TTL    time.Duration  // -1 means no expiry, 0 means key does not exist
	Raw    []byte         // DUMP payload (for dump/restore mode)
	Fields map[string]any // structured data (for struct mode)
}
