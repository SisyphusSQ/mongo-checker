package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/spf13/cobra"

	"mongo-checker/internal/config"
	l "mongo-checker/pkg/log"
	"mongo-checker/vars"
)

var (
	debug bool

	configPath string
	cpuprofile string
	memprofile string

	source      string
	destination string
	connMode    string

	excludeDBs   string
	includeDBs   string
	excludeColls string
	includeColls string
	dbTrans      string

	checkIndexes bool
	chunkSize    int
	parallel     int
	logPath      string
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "Start mongodb checker",
	Long:    `Start mongodb checker`,
	Example: fmt.Sprintf("%s run -c --config <config file>\n", vars.AppName),
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			err error
			cfg *config.Config
		)
		if configPath != "" {
			cfg, err = config.NewConfig(configPath)
			if err != nil {
				return err
			}
		} else {
			cfg = &config.Config{
				Source:       source,
				Destination:  destination,
				ExcludeDBs:   excludeDBs,
				IncludeDBs:   includeDBs,
				ExcludeColls: excludeColls,
				IncludeColls: includeColls,
				CheckIndexes: checkIndexes,
				ChunkSize:    chunkSize,
				Parallel:     parallel,
				LogPath:      logPath,
			}
			cfg.PreCheck()
		}
		l.New(cfg)

		f := StartCpuProfile()
		defer StopCpuProfile(f)

		// finish cpu perf profiling before ctrl-C/kill/kill -15
		ch := make(chan os.Signal, 5)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			for {
				sig := <-ch
				switch sig {
				case syscall.SIGINT, syscall.SIGTERM:
					l.Logger.Debug("Terminating process, will finish cpu pprof before exit(if specified)...")
					StopCpuProfile(f)
					os.Exit(1)
				default:
				}
			}
		}()

		// main logic
		//err = task.RunTask()
		//if err != nil {
		//	l.Logger.Errorf("check task got err: %v", err.Error())
		//	return err
		//}

		// do memory profiling before exit
		MemProfile()
		return nil
	},
}

func initRun() {
	runCmd.Flags().StringVarP(&configPath, "config", "c", "", "config file path")
	runCmd.Flags().StringVar(&logPath, "log-path", "./logs", "log and sqlite db file path")
	runCmd.Flags().StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to `file`")
	runCmd.Flags().StringVar(&memprofile, "memprofile", "", "write memory profile to `file`")

	runCmd.Flags().IntVar(&chunkSize, "chunk-size", 1000, "Number of rows to act on in chunks.\nZero(0) means all rows updated in one operation.\nOne(1) means update/delete one row everytime.\nThe lower the number, the shorter any locks are held, but the more operations required and the more total running time.")
	runCmd.Flags().IntVar(&parallel, "parallel", 1000, "Number of collections will be checked in parallel")

	runCmd.Flags().BoolVar(&debug, "debug", false, "If debug_mode is true, print debug logs")
	runCmd.Flags().BoolVar(&checkIndexes, "check-index", false, "If check indexes is true, checker will check indexes num which is equaled between source and destination")

	runCmd.Flags().StringVarP(&source, "source", "s", "", "E.g., mongodb://username:password@primaryA,secondaryB,secondaryC")
	runCmd.Flags().StringVarP(&destination, "destination", "d", "", "E.g., mongodb://username:password@primaryA,secondaryB,secondaryC")
	runCmd.Flags().StringVar(&connMode, "conn-mode", "primary", "_connect_mode should in [primary, secondaryPreferred, secondary, nearest, standalone]")

	runCmd.Flags().StringVar(&includeDBs, "include-dbs", "", "which database(s) should be include, include_dbs and exclude_dbs are mutually exclusive.\nex: db1 or db1,db2,...")
	runCmd.Flags().StringVar(&excludeDBs, "exclude-dbs", "", "which database(s) should be include, include_dbs and exclude_dbs are mutually exclusive.\nex: db1 or db1,db2,...")
	runCmd.Flags().StringVar(&includeColls, "include-colls", "", "which collection(s) should be include, include_coll and exclude_coll are mutually exclusive.\nex: coll1 or coll1,coll2,...")
	runCmd.Flags().StringVar(&excludeColls, "exclude-colls", "", "which collection(s) should be include, include_coll and exclude_coll are mutually exclusive.\nex: coll1 or coll1,coll2,...")
	runCmd.Flags().StringVar(&dbTrans, "db-trans", "", "transform from source database A to destination database B\nex: A:B,C:D,...")

	rootCmd.AddCommand(runCmd)
}

func StartCpuProfile() *os.File {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			l.Logger.Fatalf("could not create CPU profile: %v", err)
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			l.Logger.Fatalf("could not start CPU profile: %v", err)
		}
		l.Logger.Infof("cpu pprof start ...")
		return f
	}
	return nil
}

func StopCpuProfile(f *os.File) {
	if f != nil {
		pprof.StopCPUProfile()
		f.Close()
		l.Logger.Infof("cpu pprof stopped [file=%s]!", cpuprofile)
		return
	}
}

func MemProfile() {
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			l.Logger.Fatalf("could not create memory profile: %v", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err = pprof.WriteHeapProfile(f); err != nil {
			l.Logger.Fatalf("could not write memory profile: %v", err)
		}
		l.Logger.Infof("mem pprof done [file=%s]!", memprofile)
	}
}
