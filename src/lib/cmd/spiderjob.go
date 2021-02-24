package cmd

import (
	"fmt"
	"os"
	"spiderjob/lib/core"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile      string
	config       = core.DefaultConfig()
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
		viper.SetConfigName("spiderjob")        // name of config file (without extension)
		viper.AddConfigPath("/etc/spiderjob")   // call multiple times to add many search paths
		viper.AddConfigPath("$HOME/.spiderjob") // call multiple times to add many search paths
		viper.AddConfigPath("./config")         // call multiple times to add many search paths
	}
	viper.SetEnvPrefix("spiderjob")
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		logrus.WithError(err).Info("No valid config found: Applying default values.")
	}

	if err := viper.Unmarshal(config); err != nil {
		logrus.WithError(err).Fatal("config: Error unmarshaling config")
	}

	cliTags := viper.GetStringSlice("tag")
	var tags map[string]string

	if len(cliTags) > 0 {
		tags, err = UnmarshalTags(cliTags)
		if err != nil {
			logrus.WithError(err).Fatal("config: Error unmarshaling cli tags")
		}
	} else {
		tags = viper.GetStringMapString("tags")
	}

	config.Tags = tags

	// dkron.InitLogger(viper.GetString("log-level"), config.NodeName)
}

func UnmarshalTags(tags []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, tag := range tags {
		parts := strings.SplitN(tag, "=", 2)
		if len(parts) != 2 || len(parts[0]) == 0 {
			return nil, fmt.Errorf("invalid tag: '%s'", tag)
		}
		result[parts[0]] = parts[1]
	}
	return result, nil
}
