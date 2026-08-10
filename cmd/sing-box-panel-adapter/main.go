package main

import (
	"fmt"
	"os"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/config"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/spf13/cobra"
)

var configPath string

const (
	defaultTokenRotationInterval = 24 * time.Hour
	tokenRotationRetryInterval   = 5 * time.Minute
)

var rootCommand = &cobra.Command{
	Use:   "sing-box-panel-adapter",
	Short: "Panel adapter for sing-box",
	Run: func(_ *cobra.Command, _ []string) {
		if err := runAdapter(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCommand.PersistentFlags().StringVarP(&configPath, "config", "c", "", "adapter configuration file path")
	_ = rootCommand.MarkPersistentFlagRequired("config")
}

func main() {
	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runAdapter() error {
	configuration, err := config.Load(configPath)
	if err != nil {
		return E.Cause(err, "load adapter config")
	}
	if configuration.IsAgentMode() {
		return runAgentAdapter(configuration)
	}
	return runScalarAdapter(configuration)
}
