# Redmove — Redis Data Migration & Operations Tool

## Goal

MIT-licensed alternative to RIOTx. Single binary, fast, TUI-enabled Redis
data migration and operations tool built in Go. Works with Redis, Valkey,
KeyDB, Dragonfly — any Redis-compatible store.

---

## Core Principles

- Engine first, TUI second — build the pipeline, then wrap it
- Single static binary, ~15MB target
- Dual-mode: CLI (for scripts/CI) and TUI (interactive)
- Stream-oriented: never load full dataset into memory
- Pipeline architecture: Reader → Processor → Writer

---

## Tech Stack

| Library                   | Purpose                                      |
| ------------------------- | -------------------------------------------- |
| `go-redis/v9`             | Redis client (standalone, cluster, sentinel) |
| `charmbracelet/bubbletea` | TUI framework                                |
| `charmbracelet/lipgloss`  | TUI styling                                  |
| `charmbracelet/bubbles`   | Progress bars, tables, spinners              |
| `spf13/cobra`             | CLI subcommands                              |
| `brianvoe/gofakeit/v7`    | Synthetic data generation                    |
| `expr-lang/expr`          | Expression evaluation (field transforms)     |
| `log/slog`                | Structured logging (stdlib, no dependency)   |

Do NOT add: Viper (overkill), zerolog/zap (slog is enough), any ORM.

---

## Project Structure

```
redmove/
├── main.go                     # entry point
├── cmd/                        # cobra command definitions
│   ├── root.go                 # global flags (--uri, --cluster, --tls)
│   ├── replicate.go            # redis → redis
│   ├── import.go               # file → redis
│   ├── export.go               # redis → file
│   ├── generate.go             # fake data → redis
│   ├── stats.go                # keyspace analysis
│   ├── compare.go              # diff two redis instances
│   └── ping.go                 # connectivity + latency
├── internal/
│   ├── pipeline/               # core engine
│   │   ├── pipeline.go         # Reader → Processor → Writer orchestration
│   │   ├── reader.go           # Reader interface
│   │   ├── processor.go        # Processor interface (transform, filter)
│   │   └── writer.go           # Writer interface
│   ├── redis/                  # redis operations
│   │   ├── client.go           # connection factory (standalone/cluster/sentinel)
│   │   ├── scanner.go          # SCAN-based key iteration
│   │   ├── dumper.go           # DUMP/RESTORE operations
│   │   ├── struct_reader.go    # type-aware reads (HGETALL, LRANGE, etc.)
│   │   ├── struct_writer.go    # type-aware writes
│   │   └── notifier.go         # keyspace notification listener (live mode)
│   ├── format/                 # file format codecs
│   │   ├── csv.go
│   │   ├── json.go
│   │   ├── jsonl.go
│   │   └── detect.go           # auto-detect format from extension
│   ├── generate/               # data generation
│   │   ├── faker.go            # gofakeit wrapper
│   │   └── template.go         # key/value templates
│   ├── transform/              # data transformation
│   │   └── expr.go             # expression evaluation
│   ├── tui/                    # bubbletea TUI
│   │   ├── app.go              # top-level model, delegates to views
│   │   ├── progress.go         # progress bar view (for long ops)
│   │   ├── stats.go            # keyspace stats view
│   │   ├── keybrowser.go       # interactive key explorer
│   │   └── styles.go           # lipgloss theme
│   └── config/
│       └── config.go           # YAML config loading + CLI flag merge
├── go.mod
├── go.sum
├── Makefile
└── CLAUDE.md
```

---

## Architecture: Pipeline Engine

The pipeline is the core abstraction. Every command is a pipeline.

```
┌──────────┐    ┌─────────────┐    ┌──────────┐
│  Reader  │───→│  Processor  │───→│  Writer  │
└──────────┘    └─────────────┘    └──────────┘
     │                │                  │
  Produces        Transforms          Consumes
  records         /filters            records
  (channel)       records             (channel)
```

### Reader implementations

- `ScanReader` — iterates keys via SCAN
- `DumpReader` — reads key + DUMP payload
- `StructReader` — reads key + typed value (hash fields, list items, etc.)
- `FileReader` — reads CSV/JSON/JSONL rows
- `FakerReader` — generates synthetic records
- `NotificationReader` — listens to keyspace notifications (live mode)

### Processor implementations

- `FilterProcessor` — key pattern / type filter
- `TransformProcessor` — field-level transforms via expr
- `PassthroughProcessor` — identity (no-op)

### Writer implementations

- `RestoreWriter` — writes via RESTORE command
- `StructWriter` — writes via type-specific commands (HSET, RPUSH, etc.)
- `FileWriter` — writes to CSV/JSON/JSONL
- `StdoutWriter` — prints to terminal (for export preview)

### Data Record

```go
type Record struct {
    Key    string
    Type   string            // string, hash, list, set, zset, stream, json
    TTL    time.Duration
    Raw    []byte            // DUMP payload (for dump/restore mode)
    Fields map[string]any    // structured data (for struct mode)
}
```

### Concurrency model

```
Reader goroutine(s) → buffered channel → Processor → buffered channel → Writer goroutine(s)
```

- Reader: 1 goroutine per node in cluster mode, 1 for standalone
- Writer: configurable concurrency (`--threads`, default 4)
- Channel buffer size: configurable (`--queue-size`, default 10000)
- Writer batches commands into Redis pipelines (`--batch-size`, default 50)

---

## Build Order (phases)

### Phase 1: Foundation (week 1)

1. `go mod init`, cobra scaffold, root command with global Redis flags
2. Redis client factory (`internal/redis/client.go`) — connect to standalone/cluster/sentinel
3. `ping` command — verify connectivity, show latency percentiles
4. Pipeline engine skeleton — Reader/Processor/Writer interfaces + orchestrator

### Phase 2: Core Commands (week 2)

5. `ScanReader` + `DumpReader` + `RestoreWriter` → `replicate` command (scan mode)
6. `stats` command — keyspace analysis (count by type, memory by prefix, TTL distribution)
7. `FileReader` + `StructWriter` → `import` command (CSV, JSON, JSONL)
8. `ScanReader` + `StructReader` + `FileWriter` → `export` command

### Phase 3: Advanced Features (week 3)

9. `NotificationReader` → live replication mode
10. `compare` command — sample-based diff between source and target
11. `FakerReader` → `generate` command
12. `TransformProcessor` — field expressions via expr-lang

### Phase 4: TUI (week 4)

13. Progress bar view — wraps any pipeline with live throughput/ETA
14. Stats view — interactive keyspace explorer with tables
15. Key browser — browse keys, inspect values, filter by pattern
16. Top-level app model — mode switching between views

### Phase 5: Polish

17. YAML config file support
18. `--dry-run` flag for all write commands
19. TLS / AUTH / ACL support
20. Makefile with cross-compilation targets
21. goreleaser config for releases

---

## Command Reference

```
redmove ping       --uri redis://localhost:6379
redmove stats      --uri redis://localhost:6379 --match "user:*" --top 20
redmove replicate  --source redis://src:6379 --target redis://dst:6379 --mode scan|live
redmove import     --uri redis://localhost:6379 --file data.csv --type hash --key-template "user:#{id}"
redmove export     --uri redis://localhost:6379 --match "user:*" --format jsonl --output users.jsonl
redmove generate   --uri redis://localhost:6379 --type hash --count 100000 --key-template "user:#{seq}"
redmove compare    --source redis://src:6379 --target redis://dst:6379 --sample 10000
```

Add `--tui` to any command for interactive mode with progress/stats.

---

## Global Flags

```
--uri              Redis URI (redis://, rediss://, redis-sentinel://)
--cluster          Enable cluster mode
--tls              Enable TLS
--tls-cert         Client certificate path
--tls-key          Client key path
--tls-ca           CA certificate path
--password         Redis password (prefer REDMOVE_PASSWORD env var)
--username         Redis ACL username
--db               Redis database number (default 0)
--threads          Writer concurrency (default 4)
--batch-size       Pipeline batch size (default 50)
--queue-size       Internal channel buffer (default 10000)
--log-level        debug|info|warn|error (default info)
--log-file         Log to file instead of stderr
--tui              Launch TUI mode instead of CLI output
--dry-run          Show what would be done without writing
```

---

## Key Design Decisions

1. **DUMP/RESTORE as default replication mode** — fastest, preserves TTL, works same-version. Fall back to struct mode with `--struct` flag for cross-version.
2. **Channel-based pipeline** — backpressure is automatic. If writer is slow, channel fills up, reader blocks. No memory explosion.
3. **No plugin system in v1** — just add Reader/Writer implementations directly. Plugins are premature abstraction.
4. **slog, not zap/zerolog** — stdlib is enough. One less dependency.
5. **No Viper** — Cobra flags + a simple YAML unmarshaller. Viper pulls in too many transitive deps.
6. **TUI is optional** — every command works headless. TUI is a view layer, not a requirement.

---

## Testing Strategy

- **Unit tests**: each Reader/Processor/Writer in isolation with mock Redis (miniredis)
- **Integration tests**: docker-compose with real Redis, run full pipelines
- **TUI tests**: bubbletea has `teatest` package for programmatic TUI testing
- **Benchmark tests**: `go test -bench` for pipeline throughput

---

## Build & Release

```bash
# Dev build
go build -o redmove .

# Production build
CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=$(git describe --tags)" -o redmove .

# Compress (optional, ~60% size reduction)
upx --best redmove

# Cross-compile
GOOS=darwin GOARCH=arm64 go build -o redmove-darwin-arm64 .
GOOS=linux GOARCH=amd64 go build -o redmove-linux-amd64 .
GOOS=windows GOARCH=amd64 go build -o redmove-windows-amd64.exe .
```
