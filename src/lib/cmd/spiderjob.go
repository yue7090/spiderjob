package cmd

import (
	"fmt"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile      string
	spiderjobCmd = &cobra.Command{
		Use:   "spiderJob",
		Short: "Open source distributed job scheduling system",
		Long:  "a spider system service that runs scheduled jobs",
	}
)

//Execute command
func Execute() {
	if err := spiderjobCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

//init configure
func init() {
	cobra.OnInitialize(initConfig)
	spiderjobCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.cobra.yaml)")
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".cobra")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
