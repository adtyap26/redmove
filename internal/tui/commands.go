package tui

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/adtyap26/redmove/internal/format"
	"github.com/adtyap26/redmove/internal/generate"
	"github.com/adtyap26/redmove/internal/pipeline"
	redisclient "github.com/adtyap26/redmove/internal/redis"
	goredis "github.com/redis/go-redis/v9"
)

// Operation identifies which command to run.
type Operation int

const (
	OpPing Operation = iota
	OpReplicate
	OpStats
	OpImport
	OpExport
	OpGenerate
	OpCompare
)

func (o Operation) String() string {
	switch o {
	case OpPing:
		return "Ping"
	case OpReplicate:
		return "Replicate"
	case OpStats:
		return "Stats"
	case OpImport:
		return "Import"
	case OpExport:
		return "Export"
	case OpGenerate:
		return "Generate"
	case OpCompare:
		return "Compare"
	default:
		return "Unknown"
	}
}

// FieldKind determines how a form field is rendered.
type FieldKind int

const (
	FieldText   FieldKind = iota
	FieldToggle           // boolean [x] / [ ]
	FieldSelect           // cycle through choices
)

// FieldSpec describes one form field.
type FieldSpec struct {
	Key          string
	Label        string
	Placeholder  string
	DefaultValue string
	Required     bool
	Kind         FieldKind
	Choices      []string // only for FieldSelect
}

// FormSpec describes the form for an operation.
type FormSpec struct {
	Op     Operation
	Title  string
	Fields []FieldSpec
}

// specForOperation returns the form definition for a given command.
func specForOperation(op Operation) FormSpec {
	switch op {
	case OpPing:
		return FormSpec{
			Op: op, Title: "Ping",
			Fields: []FieldSpec{
				{Key: "uri", Label: "Redis URI", Placeholder: "redis://localhost:6379", DefaultValue: "redis://localhost:6379"},
				{Key: "count", Label: "Ping count", Placeholder: "10", DefaultValue: "10"},
			},
		}
	case OpReplicate:
		return FormSpec{
			Op: op, Title: "Replicate",
			Fields: []FieldSpec{
				{Key: "source", Label: "Source URI", Placeholder: "redis://source:6379", Required: true},
				{Key: "target", Label: "Target URI", Placeholder: "redis://target:6379", Required: true},
				{Key: "match", Label: "Key pattern", Placeholder: "*", DefaultValue: "*"},
				{Key: "mode", Label: "Mode", Kind: FieldSelect, Choices: []string{"scan", "live", "liveonly"}, DefaultValue: "scan"},
				{Key: "key-type", Label: "Key types filter", Placeholder: "hash,string (empty=all)"},
				{Key: "key-include", Label: "Include patterns", Placeholder: "user:*,session:* (empty=all)"},
				{Key: "key-exclude", Label: "Exclude patterns", Placeholder: "temp:*,cache:* (empty=none)"},
				{Key: "mem-limit", Label: "Memory limit per key", Placeholder: "10MB (empty=no limit)"},
				{Key: "batch-size", Label: "Batch size (keys per pipeline)", Placeholder: "500", DefaultValue: "500"},
				{Key: "scan-count", Label: "Scan count hint", Placeholder: "5000", DefaultValue: "5000"},
				{Key: "threads", Label: "Writer threads", Placeholder: "4", DefaultValue: "4"},
				{Key: "queue-size", Label: "Queue size (channel buffer)", Placeholder: "10000", DefaultValue: "10000"},
				{Key: "stream-id", Label: "Stream ID mode", Kind: FieldSelect, Choices: []string{"preserve", "reset"}, DefaultValue: "preserve"},
				{Key: "struct", Label: "Struct mode (type-specific)", Kind: FieldToggle},
				{Key: "target-cluster", Label: "Target is cluster", Kind: FieldToggle},
				{Key: "target-password", Label: "Target password", Placeholder: "(optional)"},
				{Key: "target-db", Label: "Target DB", Placeholder: "0", DefaultValue: "0"},
			},
		}
	case OpStats:
		return FormSpec{
			Op: op, Title: "Stats",
			Fields: []FieldSpec{
				{Key: "uri", Label: "Redis URI", Placeholder: "redis://localhost:6379", DefaultValue: "redis://localhost:6379"},
				{Key: "match", Label: "Key pattern", Placeholder: "*", DefaultValue: "*"},
				{Key: "key-type", Label: "Key types filter", Placeholder: "hash,string (empty=all)"},
				{Key: "key-include", Label: "Include patterns", Placeholder: "user:*,session:* (empty=all)"},
				{Key: "key-exclude", Label: "Exclude patterns", Placeholder: "temp:*,cache:* (empty=none)"},
				{Key: "top", Label: "Top N prefixes", Placeholder: "20", DefaultValue: "20"},
			},
		}
	case OpImport:
		return FormSpec{
			Op: op, Title: "Import",
			Fields: []FieldSpec{
				{Key: "uri", Label: "Redis URI", Placeholder: "redis://localhost:6379", DefaultValue: "redis://localhost:6379"},
				{Key: "file", Label: "Input file", Placeholder: "/path/to/data.csv", Required: true},
				{Key: "key-template", Label: "Key template", Placeholder: "user:#{id}", Required: true},
				{Key: "type", Label: "Redis type", Kind: FieldSelect, Choices: []string{"hash", "string", "list", "set", "zset"}, DefaultValue: "hash"},
			},
		}
	case OpExport:
		return FormSpec{
			Op: op, Title: "Export",
			Fields: []FieldSpec{
				{Key: "uri", Label: "Redis URI", Placeholder: "redis://localhost:6379", DefaultValue: "redis://localhost:6379"},
				{Key: "match", Label: "Key pattern", Placeholder: "*", DefaultValue: "*"},
				{Key: "key-type", Label: "Key types filter", Placeholder: "hash,string (empty=all)"},
				{Key: "key-include", Label: "Include patterns", Placeholder: "user:*,session:* (empty=all)"},
				{Key: "key-exclude", Label: "Exclude patterns", Placeholder: "temp:*,cache:* (empty=none)"},
				{Key: "format", Label: "Output format", Kind: FieldSelect, Choices: []string{"jsonl", "csv", "json", "xml"}, DefaultValue: "jsonl"},
				{Key: "output", Label: "Output file", Placeholder: "/path/to/output.jsonl (empty=stdout)"},
			},
		}
	case OpGenerate:
		return FormSpec{
			Op: op, Title: "Generate",
			Fields: []FieldSpec{
				{Key: "uri", Label: "Redis URI", Placeholder: "redis://localhost:6379", DefaultValue: "redis://localhost:6379"},
				{Key: "count", Label: "Record count", Placeholder: "1000", Required: true},
				{Key: "key-template", Label: "Key template", Placeholder: "user:#{seq}", Required: true},
				{Key: "fields", Label: "Field specs", Placeholder: "name:name,email:email,age:number", Required: true},
				{Key: "type", Label: "Redis type", Kind: FieldSelect, Choices: []string{"hash", "string", "list", "set", "zset"}, DefaultValue: "hash"},
			},
		}
	case OpCompare:
		return FormSpec{
			Op: op, Title: "Compare",
			Fields: []FieldSpec{
				{Key: "source", Label: "Source URI", Placeholder: "redis://source:6379", Required: true},
				{Key: "target", Label: "Target URI", Placeholder: "redis://target:6379", Required: true},
				{Key: "sample", Label: "Sample size", Placeholder: "10000", DefaultValue: "10000"},
				{Key: "match", Label: "Key pattern", Placeholder: "*", DefaultValue: "*"},
				{Key: "target-cluster", Label: "Target is cluster", Kind: FieldToggle},
				{Key: "target-password", Label: "Target password", Placeholder: "(optional)"},
				{Key: "target-db", Label: "Target DB", Placeholder: "0", DefaultValue: "0"},
			},
		}
	default:
		return FormSpec{Title: "Unknown"}
	}
}

// ExecuteResult holds the outcome of a command execution.
type ExecuteResult struct {
	Stats   *pipeline.Stats // non-nil for pipeline commands
	Summary string          // text summary for all commands
	Err     error
}

// executeOperation runs the given operation with the provided form values.
// onReady is called with the pipeline stats pointer as soon as the pipeline starts
// (before Run returns), allowing callers to poll live progress. For non-pipeline
// operations it is never called.
func executeOperation(ctx context.Context, op Operation, vals map[string]string, onReady func(*pipeline.Stats)) ExecuteResult {
	switch op {
	case OpPing:
		return executePing(ctx, vals)
	case OpReplicate:
		return executeReplicate(ctx, vals, onReady)
	case OpStats:
		return executeStats(ctx, vals)
	case OpImport:
		return executeImport(ctx, vals, onReady)
	case OpExport:
		return executeExport(ctx, vals, onReady)
	case OpGenerate:
		return executeGenerate(ctx, vals, onReady)
	case OpCompare:
		return executeCompare(ctx, vals)
	default:
		return ExecuteResult{Err: fmt.Errorf("unknown operation")}
	}
}

// fetchTotal returns the expected total record count for a progress bar.
// Returns 0 when count is unknown.
func fetchTotal(op Operation, vals map[string]string) int64 {
	ctx := context.Background()
	switch op {
	case OpReplicate:
		client, err := redisclient.NewClient(redisclient.ConnectOpts{URI: vals["source"]})
		if err != nil {
			return 0
		}
		defer client.Close()
		n, err := client.DBSize(ctx).Result()
		if err != nil {
			return 0
		}
		return n
	case OpExport:
		client, err := redisclient.NewClient(redisclient.ConnectOpts{URI: vals["uri"]})
		if err != nil {
			return 0
		}
		defer client.Close()
		n, err := client.DBSize(ctx).Result()
		if err != nil {
			return 0
		}
		return n
	case OpGenerate:
		n, _ := strconv.ParseInt(vals["count"], 10, 64)
		return n
	case OpCompare:
		n, _ := strconv.ParseInt(vals["sample"], 10, 64)
		return n
	default:
		return 0
	}
}

// parseCSV splits a comma-separated string into trimmed, non-empty parts.
func parseCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func connectFromVals(vals map[string]string) (goredis.UniversalClient, error) {
	uri := vals["uri"]
	if uri == "" {
		uri = "redis://localhost:6379"
	}
	return redisclient.NewClient(redisclient.ConnectOpts{URI: uri})
}

func executePing(ctx context.Context, vals map[string]string) ExecuteResult {
	client, err := connectFromVals(vals)
	if err != nil {
		return ExecuteResult{Err: err}
	}
	defer client.Close()

	count := 10
	if c, err := strconv.Atoi(vals["count"]); err == nil && c > 0 {
		count = c
	}

	uri := vals["uri"]
	if uri == "" {
		uri = "redis://localhost:6379"
	}

	var durations []time.Duration
	var errors int

	for range count {
		start := time.Now()
		_, err := client.Ping(ctx).Result()
		elapsed := time.Since(start)
		if err != nil {
			errors++
			continue
		}
		durations = append(durations, elapsed)
	}

	if len(durations) == 0 {
		return ExecuteResult{
			Summary: fmt.Sprintf("All %d pings failed", count),
			Err:     fmt.Errorf("all pings failed"),
		}
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	avg := total / time.Duration(len(durations))

	pct := func(p float64) time.Duration {
		idx := int(math.Ceil(p*float64(len(durations)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(durations) {
			idx = len(durations) - 1
		}
		return durations[idx]
	}
	fmtMs := func(d time.Duration) string {
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "PING %s — %d pings\n\n", uri, count)
	fmt.Fprintf(&b, "  min    %s\n", fmtMs(durations[0]))
	fmt.Fprintf(&b, "  avg    %s\n", fmtMs(avg))
	fmt.Fprintf(&b, "  p50    %s\n", fmtMs(pct(0.50)))
	fmt.Fprintf(&b, "  p95    %s\n", fmtMs(pct(0.95)))
	fmt.Fprintf(&b, "  p99    %s\n", fmtMs(pct(0.99)))
	fmt.Fprintf(&b, "  max    %s\n\n", fmtMs(durations[len(durations)-1]))
	fmt.Fprintf(&b, "  ok: %d/%d    errors: %d/%d", len(durations), count, errors, count)

	return ExecuteResult{Summary: b.String()}
}

func executeReplicate(ctx context.Context, vals map[string]string, onReady func(*pipeline.Stats)) ExecuteResult {
	srcClient, err := redisclient.NewClient(redisclient.ConnectOpts{URI: vals["source"]})
	if err != nil {
		return ExecuteResult{Err: fmt.Errorf("source connect: %w", err)}
	}
	defer srcClient.Close()

	targetDB, _ := strconv.Atoi(vals["target-db"])
	dstClient, err := redisclient.NewClient(redisclient.ConnectOpts{
		URI:      vals["target"],
		Cluster:  vals["target-cluster"] == "true",
		Password: vals["target-password"],
		DB:       targetDB,
	})
	if err != nil {
		return ExecuteResult{Err: fmt.Errorf("target connect: %w", err)}
	}
	defer dstClient.Close()

	match := vals["match"]
	if match == "" {
		match = "*"
	}

	batchSize := 500
	if n, err := strconv.Atoi(vals["batch-size"]); err == nil && n > 0 {
		batchSize = n
	}
	scanCount := int64(5000)
	if n, err := strconv.ParseInt(vals["scan-count"], 10, 64); err == nil && n > 0 {
		scanCount = n
	}
	threads := 4
	if n, err := strconv.Atoi(vals["threads"]); err == nil && n > 0 {
		threads = n
	}
	queueSize := 10000
	if n, err := strconv.Atoi(vals["queue-size"]); err == nil && n > 0 {
		queueSize = n
	}

	keyTypes := parseCSV(vals["key-type"])
	keyInclude := parseCSV(vals["key-include"])
	keyExclude := parseCSV(vals["key-exclude"])
	var memLimitBytes int64
	if vals["mem-limit"] != "" {
		memLimitBytes, _ = redisclient.ParseMemLimit(vals["mem-limit"])
	}

	scanOpts := redisclient.ScanOpts{
		Match: match, Count: scanCount, BatchSize: batchSize,
		KeyTypes: keyTypes, Include: keyInclude, Exclude: keyExclude,
		MemLimit: memLimitBytes,
	}

	mode := vals["mode"]
	if mode == "" {
		mode = "scan"
	}

	var reader pipeline.Reader
	var writer pipeline.Writer

	switch mode {
	case "live", "liveonly":
		reader = redisclient.NewNotificationReader(srcClient, redisclient.NotifyOpts{
			Match:     match,
			UseStruct: vals["struct"] == "true",
			KeyTypes:  keyTypes,
			Include:   keyInclude,
			Exclude:   keyExclude,
		})
	default: // "scan"
		if vals["struct"] == "true" {
			reader = redisclient.NewStructReader(srcClient, scanOpts)
		} else {
			reader = redisclient.NewDumpReader(srcClient, scanOpts)
		}
	}

	if vals["struct"] == "true" {
		streamIDMode := vals["stream-id"]
		if streamIDMode == "" {
			streamIDMode = "preserve"
		}
		writer = redisclient.NewStructWriter(dstClient, redisclient.StructWriteOpts{BatchSize: batchSize, StreamIDMode: streamIDMode})
	} else {
		writer = redisclient.NewRestoreWriter(dstClient, redisclient.RestoreOpts{BatchSize: batchSize, Replace: true, Threads: threads})
	}

	p := pipeline.New(reader, nil, writer, pipeline.WithQueueSize(queueSize))
	if onReady != nil {
		onReady(p.LiveStats())
	}
	stats, runErr := p.Run(ctx)

	written := stats.RecordsWritten.Load()
	elapsed := stats.Elapsed()
	rps := float64(0)
	if elapsed > 0 {
		rps = float64(written) / elapsed.Seconds()
	}

	transferMode := "dump/restore"
	if vals["struct"] == "true" {
		transferMode = "struct"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Replicated %s → %s (mode: %s)\n\n", vals["source"], vals["target"], transferMode)
	fmt.Fprintf(&b, "  records: %d\n", written)
	fmt.Fprintf(&b, "  elapsed: %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Fprintf(&b, "  speed:   %.0f records/sec", rps)

	return ExecuteResult{Stats: stats, Summary: b.String(), Err: runErr}
}

func executeStats(ctx context.Context, vals map[string]string) ExecuteResult {
	client, err := connectFromVals(vals)
	if err != nil {
		return ExecuteResult{Err: err}
	}
	defer client.Close()

	match := vals["match"]
	if match == "" {
		match = "*"
	}
	topN := 20
	if n, err := strconv.Atoi(vals["top"]); err == nil && n > 0 {
		topN = n
	}

	scanOpts := redisclient.ScanOpts{
		Match: match, Count: 1000, BatchSize: 100,
		KeyTypes: parseCSV(vals["key-type"]),
		Include:  parseCSV(vals["key-include"]),
		Exclude:  parseCSV(vals["key-exclude"]),
	}
	ch := make(chan pipeline.Record, 10000)

	scanErr := make(chan error, 1)
	go func() {
		reader := redisclient.NewScanReader(client, scanOpts)
		scanErr <- reader.Read(ctx, ch)
	}()

	typeCounts := make(map[string]int64)
	prefixes := make(map[string]int64)
	ttlBuckets := [6]int64{}
	var totalKeys int64

	for rec := range ch {
		totalKeys++
		typeCounts[rec.Type]++
		prefix := rec.Key
		if idx := strings.Index(rec.Key, ":"); idx != -1 {
			prefix = rec.Key[:idx+1]
		}
		prefixes[prefix]++
		switch {
		case rec.TTL < 0:
			ttlBuckets[0]++
		case rec.TTL < time.Minute:
			ttlBuckets[1]++
		case rec.TTL < time.Hour:
			ttlBuckets[2]++
		case rec.TTL < 24*time.Hour:
			ttlBuckets[3]++
		case rec.TTL < 7*24*time.Hour:
			ttlBuckets[4]++
		default:
			ttlBuckets[5]++
		}
	}

	if err := <-scanErr; err != nil {
		return ExecuteResult{Err: err}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Total keys: %d\n\n", totalKeys)

	// Type distribution.
	fmt.Fprintf(&b, "Type Distribution:\n")
	types := make([]string, 0, len(typeCounts))
	for t := range typeCounts {
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool { return typeCounts[types[i]] > typeCounts[types[j]] })
	for _, t := range types {
		pct := float64(typeCounts[t]) / float64(totalKeys) * 100
		fmt.Fprintf(&b, "  %-10s %8d  (%5.1f%%)\n", t, typeCounts[t], pct)
	}

	// Top prefixes.
	fmt.Fprintf(&b, "\nTop %d Prefixes:\n", topN)
	type pc struct {
		p string
		c int64
	}
	var pcs []pc
	for p, c := range prefixes {
		pcs = append(pcs, pc{p, c})
	}
	sort.Slice(pcs, func(i, j int) bool { return pcs[i].c > pcs[j].c })
	if len(pcs) > topN {
		pcs = pcs[:topN]
	}
	for _, p := range pcs {
		pct := float64(p.c) / float64(totalKeys) * 100
		fmt.Fprintf(&b, "  %-30s %8d  (%5.1f%%)\n", p.p, p.c, pct)
	}

	// TTL distribution.
	ttlLabels := [6]string{"no expiry", "< 1 min", "1m - 1h", "1h - 24h", "1d - 7d", "> 7 days"}
	fmt.Fprintf(&b, "\nTTL Distribution:\n")
	for i, label := range ttlLabels {
		if ttlBuckets[i] == 0 {
			continue
		}
		pct := float64(ttlBuckets[i]) / float64(totalKeys) * 100
		fmt.Fprintf(&b, "  %-12s %8d  (%5.1f%%)\n", label, ttlBuckets[i], pct)
	}

	return ExecuteResult{Summary: b.String()}
}

func executeImport(ctx context.Context, vals map[string]string, onReady func(*pipeline.Stats)) ExecuteResult {
	client, err := connectFromVals(vals)
	if err != nil {
		return ExecuteResult{Err: err}
	}
	defer client.Close()

	recType := vals["type"]
	if recType == "" {
		recType = "hash"
	}

	reader, err := format.NewFileReader(format.FileReaderOpts{
		Path:        vals["file"],
		KeyTemplate: vals["key-template"],
		RecordType:  recType,
	})
	if err != nil {
		return ExecuteResult{Err: err}
	}

	writer := redisclient.NewStructWriter(client, redisclient.StructWriteOpts{BatchSize: 50})

	p := pipeline.New(reader, nil, writer, pipeline.WithQueueSize(10000))
	if onReady != nil {
		onReady(p.LiveStats())
	}
	stats, runErr := p.Run(ctx)

	written := stats.RecordsWritten.Load()
	elapsed := stats.Elapsed()
	rps := float64(0)
	if elapsed > 0 {
		rps = float64(written) / elapsed.Seconds()
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Imported %s → Redis (type: %s)\n\n", vals["file"], recType)
	fmt.Fprintf(&b, "  records: %d\n", written)
	fmt.Fprintf(&b, "  elapsed: %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Fprintf(&b, "  speed:   %.0f records/sec", rps)

	return ExecuteResult{Stats: stats, Summary: b.String(), Err: runErr}
}

func executeExport(ctx context.Context, vals map[string]string, onReady func(*pipeline.Stats)) ExecuteResult {
	client, err := connectFromVals(vals)
	if err != nil {
		return ExecuteResult{Err: err}
	}
	defer client.Close()

	match := vals["match"]
	if match == "" {
		match = "*"
	}

	outFmt, err := format.ParseFormat(vals["format"])
	if err != nil {
		return ExecuteResult{Err: err}
	}

	reader := redisclient.NewStructReader(client, redisclient.ScanOpts{
		Match:     match,
		Count:     1000,
		BatchSize: 50,
		KeyTypes:  parseCSV(vals["key-type"]),
		Include:   parseCSV(vals["key-include"]),
		Exclude:   parseCSV(vals["key-exclude"]),
	})

	writer := format.NewFileWriter(format.FileWriterOpts{
		Path:   vals["output"],
		Format: outFmt,
	})

	p := pipeline.New(reader, nil, writer, pipeline.WithQueueSize(10000))
	if onReady != nil {
		onReady(p.LiveStats())
	}
	stats, runErr := p.Run(ctx)

	written := stats.RecordsWritten.Load()
	elapsed := stats.Elapsed()
	rps := float64(0)
	if elapsed > 0 {
		rps = float64(written) / elapsed.Seconds()
	}

	dest := vals["output"]
	if dest == "" {
		dest = "stdout"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Exported Redis → %s (format: %s)\n\n", dest, vals["format"])
	fmt.Fprintf(&b, "  records: %d\n", written)
	fmt.Fprintf(&b, "  elapsed: %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Fprintf(&b, "  speed:   %.0f records/sec", rps)

	return ExecuteResult{Stats: stats, Summary: b.String(), Err: runErr}
}

func executeGenerate(ctx context.Context, vals map[string]string, onReady func(*pipeline.Stats)) ExecuteResult {
	client, err := connectFromVals(vals)
	if err != nil {
		return ExecuteResult{Err: err}
	}
	defer client.Close()

	count, _ := strconv.Atoi(vals["count"])
	if count <= 0 {
		return ExecuteResult{Err: fmt.Errorf("count must be > 0")}
	}

	recType := vals["type"]
	if recType == "" {
		recType = "hash"
	}

	reader, err := generate.NewFakerReader(generate.FakerOpts{
		Count:       count,
		KeyTemplate: vals["key-template"],
		Fields:      vals["fields"],
		RecordType:  recType,
	})
	if err != nil {
		return ExecuteResult{Err: err}
	}

	writer := redisclient.NewStructWriter(client, redisclient.StructWriteOpts{BatchSize: 50})

	p := pipeline.New(reader, nil, writer, pipeline.WithQueueSize(10000))
	if onReady != nil {
		onReady(p.LiveStats())
	}
	stats, runErr := p.Run(ctx)

	written := stats.RecordsWritten.Load()
	elapsed := stats.Elapsed()
	rps := float64(0)
	if elapsed > 0 {
		rps = float64(written) / elapsed.Seconds()
	}

	uri := vals["uri"]
	if uri == "" {
		uri = "redis://localhost:6379"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Generated %d records → %s (type: %s)\n\n", count, uri, recType)
	fmt.Fprintf(&b, "  records: %d\n", written)
	fmt.Fprintf(&b, "  elapsed: %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Fprintf(&b, "  speed:   %.0f records/sec", rps)

	return ExecuteResult{Stats: stats, Summary: b.String(), Err: runErr}
}

func executeCompare(ctx context.Context, vals map[string]string) ExecuteResult {
	srcClient, err := redisclient.NewClient(redisclient.ConnectOpts{URI: vals["source"]})
	if err != nil {
		return ExecuteResult{Err: fmt.Errorf("source connect: %w", err)}
	}
	defer srcClient.Close()

	targetDB, _ := strconv.Atoi(vals["target-db"])
	dstClient, err := redisclient.NewClient(redisclient.ConnectOpts{
		URI:      vals["target"],
		Cluster:  vals["target-cluster"] == "true",
		Password: vals["target-password"],
		DB:       targetDB,
	})
	if err != nil {
		return ExecuteResult{Err: fmt.Errorf("target connect: %w", err)}
	}
	defer dstClient.Close()

	sampleSize := 10000
	if n, err := strconv.Atoi(vals["sample"]); err == nil && n > 0 {
		sampleSize = n
	}

	match := vals["match"]
	if match == "" {
		match = "*"
	}

	start := time.Now()

	// Reservoir sampling.
	ch := make(chan pipeline.Record, 10000)
	scanOpts := redisclient.ScanOpts{Match: match, Count: 1000, BatchSize: 100}
	scanErr := make(chan error, 1)
	go func() {
		reader := redisclient.NewScanReader(srcClient, scanOpts)
		scanErr <- reader.Read(ctx, ch)
	}()

	reservoir := make([]string, 0, sampleSize)
	i := 0
	for rec := range ch {
		if i < sampleSize {
			reservoir = append(reservoir, rec.Key)
		} else {
			j := rand.IntN(i + 1)
			if j < sampleSize {
				reservoir[j] = rec.Key
			}
		}
		i++
	}
	if err := <-scanErr; err != nil {
		return ExecuteResult{Err: fmt.Errorf("sample keys: %w", err)}
	}

	if len(reservoir) == 0 {
		return ExecuteResult{Summary: "No keys found matching pattern."}
	}

	// Compare each key.
	type cmpResult struct {
		sampled, missing, typeMismatch, valueDiff, ttlDiff, matching int
	}
	result := cmpResult{sampled: len(reservoir)}

	batchSize := 50
	for bi := 0; bi < len(reservoir); bi += batchSize {
		end := bi + batchSize
		if end > len(reservoir) {
			end = len(reservoir)
		}
		batch := reservoir[bi:end]

		dstPipe := dstClient.Pipeline()
		existsCmds := make([]*goredis.IntCmd, len(batch))
		dstTypeCmds := make([]*goredis.StatusCmd, len(batch))
		dstTTLCmds := make([]*goredis.DurationCmd, len(batch))
		for ki, key := range batch {
			existsCmds[ki] = dstPipe.Exists(ctx, key)
			dstTypeCmds[ki] = dstPipe.Type(ctx, key)
			dstTTLCmds[ki] = dstPipe.PTTL(ctx, key)
		}
		if _, err := dstPipe.Exec(ctx); err != nil && err != goredis.Nil {
			return ExecuteResult{Err: err}
		}

		srcPipe := srcClient.Pipeline()
		srcTypeCmds := make([]*goredis.StatusCmd, len(batch))
		srcTTLCmds := make([]*goredis.DurationCmd, len(batch))
		for ki, key := range batch {
			srcTypeCmds[ki] = srcPipe.Type(ctx, key)
			srcTTLCmds[ki] = srcPipe.PTTL(ctx, key)
		}
		if _, err := srcPipe.Exec(ctx); err != nil && err != goredis.Nil {
			return ExecuteResult{Err: err}
		}

		for ki, key := range batch {
			if existsCmds[ki].Val() == 0 {
				result.missing++
				continue
			}
			srcType := srcTypeCmds[ki].Val()
			dstType := dstTypeCmds[ki].Val()
			if srcType != dstType {
				result.typeMismatch++
				continue
			}
			srcVal, err := redisclient.ReadValue(ctx, srcClient, key, srcType)
			if err != nil {
				continue
			}
			dstVal, err := redisclient.ReadValue(ctx, dstClient, key, dstType)
			if err != nil {
				continue
			}
			if !reflect.DeepEqual(srcVal, dstVal) {
				result.valueDiff++
				continue
			}
			srcTTL := srcTTLCmds[ki].Val()
			dstTTL := dstTTLCmds[ki].Val()
			diff := srcTTL - dstTTL
			if diff < 0 {
				diff = -diff
			}
			if diff > 5*time.Second {
				result.ttlDiff++
				continue
			}
			result.matching++
		}
	}

	elapsed := time.Since(start)

	pct := func(n int) float64 {
		if result.sampled == 0 {
			return 0
		}
		return float64(n) / float64(result.sampled) * 100
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Compared %s → %s (sample: %d, match: %q)\n\n", vals["source"], vals["target"], result.sampled, match)
	fmt.Fprintf(&b, "  Sampled:        %d keys\n", result.sampled)
	fmt.Fprintf(&b, "  Matching:       %5d  (%5.1f%%)\n", result.matching, pct(result.matching))
	fmt.Fprintf(&b, "  Missing:        %5d  (%5.1f%%)\n", result.missing, pct(result.missing))
	fmt.Fprintf(&b, "  Type mismatch:  %5d  (%5.1f%%)\n", result.typeMismatch, pct(result.typeMismatch))
	fmt.Fprintf(&b, "  Value diff:     %5d  (%5.1f%%)\n", result.valueDiff, pct(result.valueDiff))
	fmt.Fprintf(&b, "  TTL diff:       %5d  (%5.1f%%)\n", result.ttlDiff, pct(result.ttlDiff))
	fmt.Fprintf(&b, "\n  Elapsed: %s", elapsed.Truncate(time.Millisecond))

	return ExecuteResult{Summary: b.String()}
}
