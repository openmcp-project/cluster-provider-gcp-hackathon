package main

import (
	"fmt"
	"os"

	"github.com/openmcp-project/cluster-provider-gcp/cmd/cluster-provider-gcp/app"
)

func main() {
	cmd := app.NewClusterProviderCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Print(err)
		os.Exit(1)
	}
}
