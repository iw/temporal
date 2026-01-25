package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	configPath := flag.String("config", "", "Path to config file (YAML or JSON)")
	outPath := flag.String("out", "", "Path to output CSV file")
	flag.Parse()

	// Load or create config
	var cfg *Config
	var err error
	if *configPath != "" {
		cfg, err = LoadConfig(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = DefaultConfig()
	}

	// Seed random for reproducibility
	rand.Seed(cfg.Seed)

	// Create simulation
	startTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sim := NewSim(startTime, cfg.Duration)

	// Create rate limiter
	limiter := NewPerSecondLimiter(cfg.LimiterPerSec)

	// Create metrics collector
	metrics := NewMetrics()

	// Create services
	services := make(map[string]*Service)
	for _, scfg := range cfg.Services {
		svc := NewService(scfg, cfg.Endpoint, limiter, metrics, sim)
		services[scfg.Name] = svc

		// Schedule service start with delay
		startAt := sim.Now().Add(scfg.StartDelay)
		svc.Start(startAt)
	}

	// Apply scenario (rolling restart, mass drop, etc.)
	if err := ApplyScenario(cfg, sim, services); err != nil {
		fmt.Fprintf(os.Stderr, "Error applying scenario: %v\n", err)
		os.Exit(1)
	}

	// Schedule periodic sampling
	scheduleSampling(sim, cfg, services, metrics)

	// Run simulation
	sim.Run()

	// Write CSV if requested
	if *outPath != "" {
		if err := metrics.WriteCSV(*outPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing CSV: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("CSV: %s\n", *outPath)
	}

	// Check assertions
	if err := checkAssertions(cfg, sim, services, metrics); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK")
}

func scheduleSampling(sim *Sim, cfg *Config, services map[string]*Service, metrics *Metrics) {
	var sample func()
	sample = func() {
		now := sim.Now()
		for _, svc := range services {
			// Process any pending lifetime expiry
			svc.Pool.Tick(now)
			// Record snapshot
			metrics.Snapshot(now, svc.Name, svc.Pool, svc.Config.TargetOpen)
		}
		sim.After(cfg.SampleEvery, "sample", sample)
	}
	sim.After(cfg.SampleEvery, "sample", sample)
}

func checkAssertions(cfg *Config, sim *Sim, services map[string]*Service, metrics *Metrics) error {
	// Check rate limit compliance
	if err := AssertConnectRate(cfg, metrics); err != nil {
		return err
	}

	// Check convergence and stability
	if err := AssertConverge(cfg, metrics); err != nil {
		return err
	}

	// Check for closure spikes (thundering herd)
	if err := AssertClosureSpike(cfg, metrics); err != nil {
		return err
	}

	// Print summary
	maxPerSec := metrics.MaxConnectsPerSecond()
	fmt.Printf("Max connects/sec: %d (limit: %d)\n", maxPerSec, cfg.AssertMaxConnectsPerSec)

	return nil
}
