package cmd

import (
	"fmt"
	"spiderjob/lib/core"

	"github.com/hashicorp/serf/serf"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Comman{
	Use:   "version",
	Short: "Show version",
	Long:  `Show the version`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Name: %s\n", core.Name)
		fmt.Printf("Version: %s\n", core.Version)
		fmt.Printf("Codename: %s\n", core.Codename)
		fmt.Printf("Agent Protocol: %d (Understands back to: %d)\n", serf.ProtocolVersionMax, serf.ProtocolVersionMin)
	},
}

func init() {
	spiderjobCmd.AddCommand(versionCmd)
}
