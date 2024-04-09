package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	// ErrInvalidValue is returned when a benchmark reads an unexpected value.
	ErrInvalidValue = errors.New("invalid value")

	// ErrNonDivisibleBatchSize is returned when the batch size can't be evenly
	// divided by the iteration count.
	ErrNonDivisibleBatchSize = errors.New("number of iterations must be divisible by the batch size")
)

func main() {
	benchCmd := newBenchCommand()
	if err := benchCmd.Run(os.Args[1:]...); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

var benchBucketName = []byte("bench")

type baseCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// benchCommand represents the "bench" command execution.
type benchCommand struct {
	baseCommand
}

// newBenchCommand returns a BenchCommand using the
func newBenchCommand() *benchCommand {
	c := &benchCommand{}
	c.baseCommand = baseCommand{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	return c
}

// Run executes the "bench" command.
func (cmd *benchCommand) Run(args ...string) error {
	// Parse CLI arguments.
	options, err := cmd.ParseFlags(args)
	if err != nil {
		return err
	}

	// Remove path if "-work" is not set. Otherwise keep path.
	if options.Work {
		fmt.Fprintf(cmd.Stderr, "work: %s\n", options.Path)
	} else {
		defer os.Remove(options.Path)
	}

	// Create database.
	db, err := bolt.Open(options.Path, 0600, nil)
	if err != nil {
		return err
	}
	db.NoSync = options.NoSync
	defer db.Close()

	// Write to the database.
	var writeResults BenchResults
	fmt.Fprintf(cmd.Stderr, "starting write benchmark.\n")
	if err := cmd.runWrites(db, options, &writeResults); err != nil {
		return fmt.Errorf("write: %v", err)
	}

	var readResults BenchResults
	fmt.Fprintf(cmd.Stderr, "starting read benchmark.\n")
	// Read from the database.
	if err := cmd.runReads(db, options, &readResults); err != nil {
		return fmt.Errorf("bench: read: %s", err)
	}

	// Print results.
	fmt.Fprintf(cmd.Stderr, "# Write\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", writeResults.CompletedOps(), writeResults.Duration(), writeResults.OpDuration(), writeResults.OpsPerSecond())
	fmt.Fprintf(cmd.Stderr, "# Read\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", readResults.CompletedOps(), readResults.Duration(), readResults.OpDuration(), readResults.OpsPerSecond())
	fmt.Fprintln(cmd.Stderr, "")
	return nil
}

// ParseFlags parses the command line flags.
func (cmd *benchCommand) ParseFlags(args []string) (*BenchOptions, error) {
	var options BenchOptions

	// Parse flagset.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&options.ProfileMode, "profile-mode", "rw", "")
	fs.StringVar(&options.WriteMode, "write-mode", "seq", "")
	fs.StringVar(&options.ReadMode, "read-mode", "seq", "")
	fs.Int64Var(&options.Iterations, "count", 1000, "")
	fs.Int64Var(&options.BatchSize, "batch-size", 0, "")
	fs.IntVar(&options.KeySize, "key-size", 8, "")
	fs.IntVar(&options.ValueSize, "value-size", 32, "")
	fs.StringVar(&options.CPUProfile, "cpuprofile", "", "")
	fs.StringVar(&options.MemProfile, "memprofile", "", "")
	fs.StringVar(&options.BlockProfile, "blockprofile", "", "")
	fs.Float64Var(&options.FillPercent, "fill-percent", bolt.DefaultFillPercent, "")
	fs.BoolVar(&options.NoSync, "no-sync", false, "")
	fs.BoolVar(&options.Work, "work", false, "")
	fs.StringVar(&options.Path, "path", "", "")
	fs.SetOutput(cmd.Stderr)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Set batch size to iteration size if not set.
	// Require that batch size can be evenly divided by the iteration count.
	if options.BatchSize == 0 {
		options.BatchSize = options.Iterations
	} else if options.Iterations%options.BatchSize != 0 {
		return nil, ErrNonDivisibleBatchSize
	}

	// Generate temp path if one is not passed in.
	if options.Path == "" {
		f, err := os.CreateTemp("", "bolt-bench-")
		if err != nil {
			return nil, fmt.Errorf("temp file: %s", err)
		}
		f.Close()
		os.Remove(f.Name())
		options.Path = f.Name()
	}

	return &options, nil
}

// Writes to the database.
func (cmd *benchCommand) runWrites(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	// Start profiling for writes.
	if options.ProfileMode == "rw" || options.ProfileMode == "w" {
		cmd.startProfiling(options)
	}

	finishChan := make(chan interface{})
	go checkProgress(results, finishChan, cmd.Stderr)
	defer close(finishChan)

	t := time.Now()

	var err error
	switch options.WriteMode {
	case "seq":
		err = cmd.runWritesSequential(db, options, results)
	case "rnd":
		err = cmd.runWritesRandom(db, options, results)
	case "seq-nest":
		err = cmd.runWritesSequentialNested(db, options, results)
	case "rnd-nest":
		err = cmd.runWritesRandomNested(db, options, results)
	default:
		return fmt.Errorf("invalid write mode: %s", options.WriteMode)
	}

	// Save time to write.
	results.SetDuration(time.Since(t))

	// Stop profiling for writes only.
	if options.ProfileMode == "w" {
		cmd.stopProfiling()
	}

	return err
}

func (cmd *benchCommand) runWritesSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	var i = uint32(0)
	return cmd.runWritesWithSource(db, options, results, func() uint32 { i++; return i })
}

func (cmd *benchCommand) runWritesRandom(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return cmd.runWritesWithSource(db, options, results, func() uint32 { return r.Uint32() })
}

func (cmd *benchCommand) runWritesSequentialNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	var i = uint32(0)
	return cmd.runWritesNestedWithSource(db, options, results, func() uint32 { i++; return i })
}

func (cmd *benchCommand) runWritesRandomNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return cmd.runWritesNestedWithSource(db, options, results, func() uint32 { return r.Uint32() })
}

func (cmd *benchCommand) runWritesWithSource(db *bolt.DB, options *BenchOptions, results *BenchResults, keySource func() uint32) error {
	for i := int64(0); i < options.Iterations; i += options.BatchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = options.FillPercent

			fmt.Fprintf(cmd.Stderr, "Starting write iteration %d\n", i)
			for j := int64(0); j < options.BatchSize; j++ {
				key := make([]byte, options.KeySize)
				value := make([]byte, options.ValueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}

				results.AddCompletedOps(1)
			}
			fmt.Fprintf(cmd.Stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *benchCommand) runWritesNestedWithSource(db *bolt.DB, options *BenchOptions, results *BenchResults, keySource func() uint32) error {
	for i := int64(0); i < options.Iterations; i += options.BatchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			top, err := tx.CreateBucketIfNotExists(benchBucketName)
			if err != nil {
				return err
			}
			top.FillPercent = options.FillPercent

			// Create bucket key.
			name := make([]byte, options.KeySize)
			binary.BigEndian.PutUint32(name, keySource())

			// Create bucket.
			b, err := top.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
			b.FillPercent = options.FillPercent

			fmt.Fprintf(cmd.Stderr, "Starting write iteration %d\n", i)
			for j := int64(0); j < options.BatchSize; j++ {
				var key = make([]byte, options.KeySize)
				var value = make([]byte, options.ValueSize)

				// Generate key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert value into subbucket.
				if err := b.Put(key, value); err != nil {
					return err
				}

				results.AddCompletedOps(1)
			}
			fmt.Fprintf(cmd.Stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Reads from the database.
func (cmd *benchCommand) runReads(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	// Start profiling for reads.
	if options.ProfileMode == "r" {
		cmd.startProfiling(options)
	}

	finishChan := make(chan interface{})
	go checkProgress(results, finishChan, cmd.Stderr)
	defer close(finishChan)

	t := time.Now()

	var err error
	switch options.ReadMode {
	case "seq":
		switch options.WriteMode {
		case "seq-nest", "rnd-nest":
			err = cmd.runReadsSequentialNested(db, options, results)
		default:
			err = cmd.runReadsSequential(db, options, results)
		}
	default:
		return fmt.Errorf("invalid read mode: %s", options.ReadMode)
	}

	// Save read time.
	results.SetDuration(time.Since(t))

	// Stop profiling for reads.
	if options.ProfileMode == "rw" || options.ProfileMode == "r" {
		cmd.stopProfiling()
	}

	return err
}

func (cmd *benchCommand) runReadsSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			c := tx.Bucket(benchBucketName).Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				numReads++
				results.AddCompletedOps(1)
				if v == nil {
					return errors.New("invalid value")
				}
			}

			if options.WriteMode == "seq" && numReads != options.Iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.Iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func (cmd *benchCommand) runReadsSequentialNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			var top = tx.Bucket(benchBucketName)
			if err := top.ForEach(func(name, _ []byte) error {
				if b := top.Bucket(name); b != nil {
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						numReads++
						results.AddCompletedOps(1)
						if v == nil {
							return ErrInvalidValue
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}

			if options.WriteMode == "seq-nest" && numReads != options.Iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.Iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func checkProgress(results *BenchResults, finishChan chan interface{}, stderr io.Writer) {
	ticker := time.Tick(time.Second)
	lastCompleted, lastTime := int64(0), time.Now()
	for {
		select {
		case <-finishChan:
			return
		case t := <-ticker:
			completed, taken := results.CompletedOps(), t.Sub(lastTime)
			fmt.Fprintf(stderr, "Completed %d requests, %d/s \n",
				completed, ((completed-lastCompleted)*int64(time.Second))/int64(taken),
			)
			lastCompleted, lastTime = completed, t
		}
	}
}

// File handlers for the various profiles.
var cpuprofile, memprofile, blockprofile *os.File

// Starts all profiles set on the options.
func (cmd *benchCommand) startProfiling(options *BenchOptions) {
	var err error

	// Start CPU profiling.
	if options.CPUProfile != "" {
		cpuprofile, err = os.Create(options.CPUProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create cpu profile %q: %v\n", options.CPUProfile, err)
			os.Exit(1)
		}
		err = pprof.StartCPUProfile(cpuprofile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not start cpu profile %q: %v\n", options.CPUProfile, err)
			os.Exit(1)
		}
	}

	// Start memory profiling.
	if options.MemProfile != "" {
		memprofile, err = os.Create(options.MemProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create memory profile %q: %v\n", options.MemProfile, err)
			os.Exit(1)
		}
		runtime.MemProfileRate = 4096
	}

	// Start fatal profiling.
	if options.BlockProfile != "" {
		blockprofile, err = os.Create(options.BlockProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create block profile %q: %v\n", options.BlockProfile, err)
			os.Exit(1)
		}
		runtime.SetBlockProfileRate(1)
	}
}

// Stops all profiles.
func (cmd *benchCommand) stopProfiling() {
	if cpuprofile != nil {
		pprof.StopCPUProfile()
		cpuprofile.Close()
		cpuprofile = nil
	}

	if memprofile != nil {
		err := pprof.Lookup("heap").WriteTo(memprofile, 0)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not write mem profile")
		}
		memprofile.Close()
		memprofile = nil
	}

	if blockprofile != nil {
		err := pprof.Lookup("block").WriteTo(blockprofile, 0)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not write block profile")
		}
		blockprofile.Close()
		blockprofile = nil
		runtime.SetBlockProfileRate(0)
	}
}

// BenchOptions represents the set of options that can be passed to "bolt bench".
type BenchOptions struct {
	ProfileMode   string
	WriteMode     string
	ReadMode      string
	Iterations    int64
	BatchSize     int64
	KeySize       int
	ValueSize     int
	CPUProfile    string
	MemProfile    string
	BlockProfile  string
	StatsInterval time.Duration
	FillPercent   float64
	NoSync        bool
	Work          bool
	Path          string
}

// BenchResults represents the performance results of the benchmark and is thread-safe.
type BenchResults struct {
	completedOps int64
	duration     int64
}

func (r *BenchResults) AddCompletedOps(amount int64) {
	atomic.AddInt64(&r.completedOps, amount)
}

func (r *BenchResults) CompletedOps() int64 {
	return atomic.LoadInt64(&r.completedOps)
}

func (r *BenchResults) SetDuration(dur time.Duration) {
	atomic.StoreInt64(&r.duration, int64(dur))
}

func (r *BenchResults) Duration() time.Duration {
	return time.Duration(atomic.LoadInt64(&r.duration))
}

// Returns the duration for a single read/write operation.
func (r *BenchResults) OpDuration() time.Duration {
	if r.CompletedOps() == 0 {
		return 0
	}
	return r.Duration() / time.Duration(r.CompletedOps())
}

// Returns average number of read/write operations that can be performed per second.
func (r *BenchResults) OpsPerSecond() int {
	var op = r.OpDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
}
