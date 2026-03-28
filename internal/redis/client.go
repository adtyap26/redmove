package redis

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// ConnectOpts holds the parameters needed to create a Redis client.
type ConnectOpts struct {
	URI      string
	Cluster  bool
	TLS      bool
	TLSCert  string
	TLSKey   string
	TLSCA    string
	Password string
	Username string
	DB       int
}

// NewClient creates a redis.UniversalClient based on the provided options.
// It detects standalone, cluster, or sentinel mode from flags and URI scheme.
func NewClient(opts ConnectOpts) (goredis.UniversalClient, error) {
	var tlsCfg *tls.Config
	if opts.TLS || opts.TLSCert != "" || strings.HasPrefix(opts.URI, "rediss://") {
		var err error
		tlsCfg, err = buildTLSConfig(opts.TLSCert, opts.TLSKey, opts.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("tls config: %w", err)
		}
	}

	// Sentinel mode
	if strings.HasPrefix(opts.URI, "redis-sentinel://") {
		fopts, err := parseSentinelURI(opts.URI, opts)
		if err != nil {
			return nil, fmt.Errorf("parse sentinel URI: %w", err)
		}
		fopts.TLSConfig = tlsCfg
		return goredis.NewFailoverClient(fopts), nil
	}

	// Cluster mode
	if opts.Cluster {
		addrs, err := parseClusterAddrs(opts.URI)
		if err != nil {
			return nil, fmt.Errorf("parse cluster addrs: %w", err)
		}
		return goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:     addrs,
			Username:  opts.Username,
			Password:  opts.Password,
			TLSConfig: tlsCfg,
		}), nil
	}

	// Standalone mode
	uri := opts.URI
	if uri == "" {
		uri = "redis://localhost:6379/0"
	}
	ropts, err := goredis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("parse redis URI: %w", err)
	}
	if opts.Password != "" {
		ropts.Password = opts.Password
	}
	if opts.Username != "" {
		ropts.Username = opts.Username
	}
	if opts.DB != 0 {
		ropts.DB = opts.DB
	}
	if tlsCfg != nil {
		ropts.TLSConfig = tlsCfg
	}
	return goredis.NewClient(ropts), nil
}

// buildTLSConfig constructs a *tls.Config from certificate paths.
func buildTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cfg := &tls.Config{}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		cfg.RootCAs = pool
	}

	return cfg, nil
}

// parseSentinelURI parses a redis-sentinel:// URI into FailoverOptions.
// Format: redis-sentinel://[:password@]host1:port1[,host2:port2,...]/db?master=name
func parseSentinelURI(uri string, opts ConnectOpts) (*goredis.FailoverOptions, error) {
	// Replace scheme so net/url can parse it.
	normalized := strings.Replace(uri, "redis-sentinel://", "http://", 1)
	u, err := url.Parse(normalized)
	if err != nil {
		return nil, fmt.Errorf("parse URI: %w", err)
	}

	masterName := u.Query().Get("master")
	if masterName == "" {
		return nil, fmt.Errorf("sentinel URI must include ?master=<name>")
	}

	// Parse sentinel addresses from host (may be comma-separated).
	hostStr := u.Host
	var addrs []string
	for _, h := range strings.Split(hostStr, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(h); err != nil {
			h = h + ":26379"
		}
		addrs = append(addrs, h)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no sentinel addresses found in URI")
	}

	db := opts.DB
	if dbStr := strings.TrimPrefix(u.Path, "/"); dbStr != "" {
		if parsed, err := strconv.Atoi(dbStr); err == nil {
			db = parsed
		}
	}

	password := opts.Password
	username := opts.Username
	if u.User != nil {
		if p, ok := u.User.Password(); ok && password == "" {
			password = p
		}
		if uname := u.User.Username(); uname != "" && username == "" {
			username = uname
		}
	}

	return &goredis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: addrs,
		Password:      password,
		Username:      username,
		DB:            db,
	}, nil
}

// parseClusterAddrs extracts addresses from a URI or comma-separated list.
func parseClusterAddrs(uri string) ([]string, error) {
	if uri == "" {
		return []string{"localhost:6379"}, nil
	}

	// Strip scheme if present.
	stripped := uri
	for _, scheme := range []string{"redis://", "rediss://"} {
		stripped = strings.TrimPrefix(stripped, scheme)
	}
	// Remove path/query.
	if idx := strings.IndexAny(stripped, "/?"); idx != -1 {
		stripped = stripped[:idx]
	}
	// Remove userinfo.
	if idx := strings.LastIndex(stripped, "@"); idx != -1 {
		stripped = stripped[idx+1:]
	}

	var addrs []string
	for _, h := range strings.Split(stripped, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(h); err != nil {
			h = h + ":6379"
		}
		addrs = append(addrs, h)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses found")
	}
	return addrs, nil
}
