package cmd

import (
	"fmt"
	"log/slog"
	"os"

	redisclient "github.com/adtyap26/redmove/internal/redis"
	"github.com/spf13/cobra"
)

// GlobalOpts holds all global flag values.
type GlobalOpts struct {
	URI       string
	Cluster   bool
	TLS       bool
	TLSCert   string
	TLSKey    string
	TLSCA     string
	Password  string
	Username  string
	DB        int
	Threads   int
	BatchSize int
	QueueSize int
	LogLevel  string
	LogFile   string
	TUI       bool
	DryRun    bool
}

// Globals is the package-level instance accessed by all subcommands.
var Globals GlobalOpts

// OptsFromGlobals converts global flags to a ConnectOpts for the Redis client factory.
func OptsFromGlobals() redisclient.ConnectOpts {
	return redisclient.ConnectOpts{
		URI:      Globals.URI,
		Cluster:  Globals.Cluster,
		TLS:      Globals.TLS,
		TLSCert:  Globals.TLSCert,
		TLSKey:   Globals.TLSKey,
		TLSCA:    Globals.TLSCA,
		Password: Globals.Password,
		Username: Globals.Username,
		DB:       Globals.DB,
	}
}

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "redmove",
		Short: "Redis data migration & operations tool",
		Long:  "Redmove is a fast, TUI-enabled Redis data migration and operations tool.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if Globals.Password == "" {
				Globals.Password = os.Getenv("REDMOVE_PASSWORD")
			}
			return setupLogger(Globals.LogLevel, Globals.LogFile)
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	f := rootCmd.PersistentFlags()
	f.StringVar(&Globals.URI, "uri", "", "Redis URI (redis://, rediss://, redis-sentinel://)")
	f.BoolVar(&Globals.Cluster, "cluster", false, "Enable cluster mode")
	f.BoolVar(&Globals.TLS, "tls", false, "Enable TLS")
	f.StringVar(&Globals.TLSCert, "tls-cert", "", "Client certificate path")
	f.StringVar(&Globals.TLSKey, "tls-key", "", "Client key path")
	f.StringVar(&Globals.TLSCA, "tls-ca", "", "CA certificate path")
	f.StringVar(&Globals.Password, "password", "", "Redis password (prefer REDMOVE_PASSWORD env var)")
	f.StringVar(&Globals.Username, "username", "", "Redis ACL username")
	f.IntVar(&Globals.DB, "db", 0, "Redis database number")
	f.IntVar(&Globals.Threads, "threads", 4, "Writer concurrency")
	f.IntVar(&Globals.BatchSize, "batch-size", 200, "Pipeline batch size")
	f.IntVar(&Globals.QueueSize, "queue-size", 10000, "Internal channel buffer")
	f.StringVar(&Globals.LogLevel, "log-level", "info", "Log level: debug|info|warn|error")
	f.StringVar(&Globals.LogFile, "log-file", "", "Log to file instead of stderr")
	f.BoolVar(&Globals.TUI, "tui", false, "Launch TUI mode")
	f.BoolVar(&Globals.DryRun, "dry-run", false, "Show what would be done without writing")

	rootCmd.AddCommand(newPingCmd())
	rootCmd.AddCommand(newReplicateCmd())
	rootCmd.AddCommand(newStatsCmd())
	rootCmd.AddCommand(newImportCmd())
	rootCmd.AddCommand(newExportCmd())
	rootCmd.AddCommand(newGenerateCmd())
	rootCmd.AddCommand(newCompareCmd())
	rootCmd.AddCommand(newTuiCmd())
	rootCmd.AddCommand(newCompletionCmd())

	return rootCmd
}

// Execute runs the root command.
func Execute() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setupLogger(level, file string) error {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return fmt.Errorf("unknown log level: %q (use debug|info|warn|error)", level)
	}

	var w *os.File
	if file != "" {
		var err error
		w, err = os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("open log file: %w", err)
		}
	} else {
		w = os.Stderr
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	if file != "" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}
	slog.SetDefault(slog.New(handler))
	return nil
}
