package main

import (
	"context"
	"fmt"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"
)

var (
	datadir   string
	chaindata string
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
	withDatadir(cmd00)
	rootCmd.AddCommand(cmd00)
}

func rootContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
		case <-ctx.Done():
		}

		cancel()
	}()
	return ctx
}

var rootCmd = &cobra.Command{
	Use:   "generate_snapshot",
	Short: "generate snapshot",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}
		if chaindata == "" {
			chaindata = path.Join(datadir, "chaindata")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withDatadir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadir, "datadir", paths.DefaultDataDir(), "data")
	must(cmd.MarkFlagDirname("datadir"))

	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))
}

var cmd00 = &cobra.Command{
	Use: "cmd00",
	RunE: func(cmd *cobra.Command, args []string) error {
		return Cmd00(cmd.Context(), log.New(), chaindata)
	},
}

func Cmd00(ctx context.Context, logger log.Logger, dbPath string) error {
	db := kv2.NewMDBX(logger).Path(dbPath).MustOpen()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	blockNumber := uint64(13000000)
	t := time.Now()
	var hash common.Hash
	hash, err = rawdb.ReadCanonicalHash(tx, blockNumber)
	body := rawdb.ReadBodyRLP(tx, hash, blockNumber)
	log.Info("read block ", "len", len(body), "hash", hash.Hex())
	log.Info("Finished", "duration", time.Since(t))
	return nil
}

func main() {
	if err := rootCmd.ExecuteContext(rootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
