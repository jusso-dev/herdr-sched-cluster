package main

import (
	"path/filepath"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/clusterconf"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/herdrx"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/transport"

	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/config"
	"github.com/jusso-dev/herdr-sched-cluster/internal/daemon"
	"github.com/jusso-dev/herdr-sched-cluster/internal/fire"
	"github.com/jusso-dev/herdr-sched-cluster/internal/store"
	"github.com/jusso-dev/herdr-sched-cluster/internal/version"
)

// serve is the daemon: it owns the store and the tick, and answers the socket
// both doors dial.
func serve(f *daemonFlags) error {
	// The log is opened here, before anything worth reading is said. Where it
	// goes is the daemon's own call for the same reason the socket, the lock
	// and the store are: a shell line that redirects elsewhere can be lost on
	// a restart, and it is only ever missed once the log is already needed.
	if err := config.EnsureStateDir(); err != nil {
		return err
	}
	logOut, logErr := daemon.OpenLog(f.logPath, os.Stdout)
	if logOut != os.Stdout {
		defer logOut.Close()
	}
	log.SetOutput(logOut)
	if logErr != nil {
		f.logPath = ""
		log.Printf("%v; logging to stdout alone", logErr)
	}

	// §10.1: read once, at startup. The daemon holds the document it started
	// with, and a change to the file takes effect on the next start.
	cfg, err := config.LoadFrom(f.configPath)
	if err != nil {
		return err
	}

	// One daemon per user (§2.3). The lock is what makes one answer true
	// across every caller rather than per process.
	lock, err := daemon.Lock()
	if err != nil {
		return err
	}
	defer lock.Close()

	st, err := store.Open(config.StorePath())
	if err != nil {
		return err
	}

	// The webhook keys, in a file of their own beside the store. They are read
	// here, once, for the same reason the store is: the daemon is the only
	// writer of both.
	secrets, err := store.OpenSecrets(config.SecretsPath())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ln, err := daemon.Listen()
	if err != nil {
		return err
	}
	defer ln.Close()

	interval := f.interval
	if interval <= 0 {
		interval = time.Duration(cfg.TickSeconds) * time.Second
	}
	log.Printf("listening on %s, ticking every %s", config.SocketPath(), interval)
	if !cfg.Present {
		// Said out loud rather than left to be inferred: §10.1 fixes one
		// config path per plugin, and an operator editing a file somewhere
		// else would otherwise see no effect and no reason.
		log.Printf("no config file at %s: every default applies", cfg.Path)
	}
	if cfg.WebhookAddr == "" || cfg.WebhookAddr == daemon.WebhookOff {
		// Said out loud rather than left to be inferred: a webhook trigger
		// written against a daemon with no inbound door is a trigger nothing
		// can ever reach.
		log.Print("no webhook address configured: no inbound door, and only file watchers can fire")
	}
	if len(cfg.GateCommand) == 0 {
		log.Print("no policy gate configured: every verb is allowed (§9.2)")
	}
	for _, dir := range config.OrphanStoreDirs() {
		log.Printf("a store left by another build could be under %s; this daemon is not using it", dir)
	}

	runner := &fire.Runner{Store: st, Log: log.Printf}
	d := &daemon.Daemon{
		Store:       st,
		Secrets:     secrets,
		Fire:        runner,
		Config:      cfg,
		Interval:    interval,
		WebhookAddr: cfg.WebhookAddr,
		Version:     version.Version,
		Log:         log.Default(),
		LogPath:     f.logPath,
		Lock:        lock,
		Cluster:     openCluster(cfg),
	}
	if d.Cluster.Coord != nil {
		defer d.Cluster.Coord.L.Close()
	}
	// Every run reaches the followers and the §8.3 hook the same way every
	// other event does.
	runner.Emit = d.Emitted
	err = d.Serve(ctx, ln)
	// A shell action is detached from the tick, so the ones still running
	// have their outcome to write. Waiting for them is what keeps a run off
	// the trail from being a run that never happened.
	runner.Wait()
	log.Print("stopping")
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// openCluster builds the distributed scheduler from the config. A cluster
// that cannot start is logged and refused per verb; it never stops the
// cron and trigger half of the daemon from serving.
func openCluster(cfg *config.Config) *daemon.Cluster {
	set, err := clusterconf.Build(cfg)
	if err != nil {
		log.Printf("cluster scheduler off: %v", err)
		return &daemon.Cluster{Disabled: "the cluster scheduler is off: " + err.Error()}
	}
	if !set.Enabled {
		return &daemon.Cluster{Disabled: "the cluster scheduler is off (cluster_enabled = false)"}
	}
	l, err := ledger.Open(filepath.Join(config.StateDir(), "cluster.db"))
	if err != nil {
		log.Printf("cluster scheduler off: %v", err)
		return &daemon.Cluster{Disabled: "the cluster ledger could not be opened: " + err.Error()}
	}
	prober := coord.DefaultProber{SSH: transport.SSH{Timeout: set.ProbeTimeout}}
	c := coord.New(l, herdrx.New(), prober, set.Coord)
	log.Printf("cluster scheduler on: ledger %s, policy %s, tick %s", l.Path, set.Coord.Policy, set.Tick)
	return &daemon.Cluster{Coord: c, Tick: set.Tick, MetricsAddr: set.MetricsAddr}
}
