package main

import (
	"flag"
	"fmt"
	"time"
)

const VERSION = "0.32.2"

func main() {
	var since string
	flag.StringVar(&since, "since", "", "Sync changes since this time (e.g., '24h' or ISO timestamp)")
	
	config := LoadConfig()

	if len(flag.Args()) == 0 {
		start(config)
		return
	}

	command := flag.Arg(0)

	switch command {
	case "start":
		start(config)
	case "sync":
		if config.Pg.SyncInterval != "" {
			duration, err := time.ParseDuration(config.Pg.SyncInterval)
			if err != nil {
				panic("Invalid interval format: " + config.Pg.SyncInterval)
			}
			LogInfo(config, "Starting sync loop with interval:", config.Pg.SyncInterval)
			for {
				syncFromPg(config, since)
				LogInfo(config, "Sleeping for", config.Pg.SyncInterval)
				time.Sleep(duration)
			}
		} else {
			syncFromPg(config, since)
		}
	case "version":
		fmt.Println("BemiDB version:", VERSION)
	default:
		panic("Unknown command: " + command)
	}
}

func start(config *Config) {
	tcpListener := NewTcpListener(config)
	LogInfo(config, "BemiDB: Listening on", tcpListener.Addr())

	duckdb := NewDuckdb(config)
	LogInfo(config, "DuckDB: Connected")
	defer duckdb.Close()

	icebergReader := NewIcebergReader(config)
	queryHandler := NewQueryHandler(config, duckdb, icebergReader)

	for {
		conn := AcceptConnection(tcpListener)
		LogInfo(config, "BemiDB: Accepted connection from", conn.RemoteAddr())
		postgres := NewPostgres(config, &conn)

		go func() {
			postgres.Run(queryHandler)
			defer postgres.Close()
			LogInfo(config, "BemiDB: Closed connection from", conn.RemoteAddr())
		}()
	}
}

func syncFromPg(config *Config, since string) {
	syncer := NewSyncer(config)
	
	var options *SyncOptions
	if since != "" {
		options = &SyncOptions{}
		
		var err error
		duration, err := time.ParseDuration(since)
		if err == nil {
			options.Since = time.Now().Add(-duration)
			LogDebug(config, "Syncing changes since:", options.Since.Format(time.RFC3339))
		} else {
			// Try parsing as ISO timestamp
			t, err := time.Parse(time.RFC3339, since)
			if err != nil {
				panic("Invalid time format for --since. Use duration (e.g., '24h') or ISO timestamp")
			}
			options.Since = t
			LogDebug(config, "Syncing changes since:", options.Since.Format(time.RFC3339))
		}
	} else {
		LogDebug(config, "No sync options provided, performing full sync")
	}
	
	syncer.SyncFromPostgres(options)
	LogInfo(config, "Sync from PostgreSQL completed successfully.")
}
