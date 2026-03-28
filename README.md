# Redmove

A fast, MIT-licensed alternative to RIOTx. Single static binary, TUI-enabled Redis data migration and operations tool built in Go. Works with Redis, Valkey, KeyDB, Dragonfly — any Redis-compatible store.

## Features

- **Replicate** — Redis → Redis via DUMP/RESTORE (fastest) or struct mode
- **Live replication** — keyspace notifications for real-time sync (`scan`, `live`, `liveonly` modes)
- **Import** — CSV, JSON, JSONL, XML → Redis
- **Export** — Redis → CSV, JSON, JSONL, XML
- **Generate** — Synthetic fake data for testing/benchmarking
- **Compare** — Sample-based diff between two Redis instances
- **Stats** — Keyspace analysis (type distribution, top prefixes, TTL buckets)
- **Ping** — Connectivity check with latency percentiles
- **TUI** — Interactive terminal UI for all operations
- **Key filtering** — `--key-type`, `--key-include`, `--key-exclude`, `--mem-limit`
- **Shell completion** — bash, zsh, fish, powershell

## Installation

### Build from source

```bash
git clone https://github.com/adtyap26/redmove.git
cd redmove
go build -o redmove .
```

### Static build (for servers with old glibc)

```bash
make build-static
```

## Quick Start

```bash
# Replicate one Redis to another
./redmove replicate --source redis://src:6379 --target redis://dst:6379

# Live replication (keyspace notifications)
./redmove replicate --source redis://src:6379 --target redis://dst:6379 --mode live

# Export to JSONL
./redmove export --uri redis://localhost:6379 --format jsonl --output data.jsonl

# Import from CSV
./redmove import --uri redis://localhost:6379 --file data.csv --key-template "user:#{id}" --type hash

# Generate 100K fake users
./redmove generate --uri redis://localhost:6379 --count 100000 \
  --key-template "user:#{seq}" --fields "name:name,email:email,age:number" --type hash

# Keyspace analysis
./redmove stats --uri redis://localhost:6379

# Compare two instances
./redmove compare --source redis://src:6379 --target redis://dst:6379 --sample 10000

# Interactive TUI
./redmove tui
```

## Commands

| Command | Description |
|---|---|
| `replicate` | Replicate keys from source to target Redis |
| `import` | Import data from file into Redis |
| `export` | Export Redis keys to file |
| `generate` | Generate synthetic data into Redis |
| `compare` | Sample-based diff between two instances |
| `stats` | Analyze keyspace: types, prefixes, TTL distribution |
| `ping` | Check connectivity and measure latency |
| `tui` | Launch interactive terminal UI |
| `completion` | Generate shell completion script |

## Replicate

```bash
# DUMP/RESTORE mode (default, fastest)
./redmove replicate --source redis://src:6379 --target redis://dst:6379

# Struct mode (cross-version compatible)
./redmove replicate --source redis://src:6379 --target redis://dst:6379 --struct

# Filter by key type
./redmove replicate --source redis://src:6379 --target redis://dst:6379 --key-type hash

# Include/exclude patterns
./redmove replicate --source redis://src:6379 --target redis://dst:6379 \
  --key-include "user:*" --key-exclude "user:temp:*"

# Skip keys larger than 10MB
./redmove replicate --source redis://src:6379 --target redis://dst:6379 --mem-limit 10MB

# Live replication modes
./redmove replicate --source redis://src:6379 --target redis://dst:6379 --mode live
./redmove replicate --source redis://src:6379 --target redis://dst:6379 --mode liveonly
```

## Performance Tuning

```bash
./redmove replicate \
  --source redis://src:6379 \
  --target redis://dst:6379 \
  --threads 8 \
  --batch-size 500 \
  --scan-count 10000 \
  --queue-size 50000
```

## Global Flags

```
--uri           Redis URI (redis://, rediss://, redis-sentinel://)
--cluster       Enable cluster mode
--tls           Enable TLS
--tls-cert      Client certificate path
--tls-key       Client key path
--tls-ca        CA certificate path
--password      Redis password (prefer REDMOVE_PASSWORD env var)
--username      Redis ACL username
--db            Redis database number (default 0)
--threads       Writer concurrency (default 4)
--batch-size    Pipeline batch size (default 200)
--queue-size    Internal channel buffer (default 10000)
--log-level     debug|info|warn|error (default info)
--log-file      Log to file instead of stderr
--tui           Launch TUI mode
--dry-run       Show what would be done without writing
```

## Connection Examples

```bash
# Standalone
redis://localhost:6379
redis://:password@localhost:6379
redis://username:password@localhost:6379

# TLS
rediss://localhost:6380

# Sentinel
redis-sentinel://sentinel1:26379,sentinel2:26379/0?master=mymaster

# Cluster
./redmove stats --uri redis://node1:7000,node2:7001,node3:7002 --cluster
```

## Shell Completion

```bash
# Bash
source <(./redmove completion bash)

# Zsh
./redmove completion zsh > "${fpath[1]}/_redmove"

# Fish
./redmove completion fish | source
```

## Build

```bash
make build          # dev build
make build-static   # CGO_ENABLED=0 static binary
make build-linux    # linux/amd64
make build-darwin   # darwin/arm64
make build-windows  # windows/amd64
```

## Architecture

```
Reader → Processor → Writer
```

Stream-oriented: never loads the full dataset into memory. Buffered channels provide automatic backpressure.

## Tech Stack

| Library | Purpose |
|---|---|
| `go-redis/v9` | Redis client (standalone, cluster, sentinel) |
| `charmbracelet/bubbletea` | TUI framework |
| `charmbracelet/lipgloss` | TUI styling |
| `charmbracelet/bubbles` | Progress bars, spinners |
| `spf13/cobra` | CLI subcommands |
| `brianvoe/gofakeit/v7` | Synthetic data generation |
| `expr-lang/expr` | Expression evaluation (field transforms) |

## License

[MIT](LICENSE)
