package app

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	"github.com/openmcp-project/controller-utils/pkg/logging"
	ctrl "sigs.k8s.io/controller-runtime"
)

func NewClusterProviderCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "cluster-provider-gcp",
	}
	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	so := &SharedOptions{
		RawSharedOptions: &RawSharedOptions{},
		PlatformCluster:  clusters.New("platform"),
	}
	so.AddPersistentFlags(cmd)
	cmd.AddCommand(NewInitCommand(so))
	cmd.AddCommand(NewRunCommand(so))
	return cmd
}

type RawSharedOptions struct {
	Environment  string `json:"environment"`
	ProviderName string `json:"provider-name"`
	DryRun       bool   `json:"dry-run"`
}

type SharedOptions struct {
	*RawSharedOptions
	PlatformCluster *clusters.Cluster

	// filled by Complete()
	Log logging.Logger
}

func (o *SharedOptions) AddPersistentFlags(cmd *cobra.Command) {
	logging.InitFlags(cmd.PersistentFlags())
	o.PlatformCluster.RegisterSingleConfigPathFlag(cmd.PersistentFlags())
	cmd.PersistentFlags().StringVar(&o.Environment, "environment", "", "Environment name. Must be globally unique across all providers watching the same platform cluster.")
	cmd.PersistentFlags().StringVar(&o.ProviderName, "provider-name", "", "Name of the ClusterProvider resource.")
	cmd.PersistentFlags().BoolVar(&o.DryRun, "dry-run", false, "If set, the command aborts after evaluating flags.")
}

func (o *SharedOptions) Complete() error {
	if o.Environment == "" {
		return fmt.Errorf("--environment must not be empty")
	}
	if o.ProviderName == "" {
		return fmt.Errorf("--provider-name must not be empty")
	}

	log, err := logging.GetLogger()
	if err != nil {
		return err
	}
	o.Log = log
	ctrl.SetLogger(o.Log.Logr())

	if err := o.PlatformCluster.InitializeRESTConfig(); err != nil {
		return err
	}
	return nil
}
