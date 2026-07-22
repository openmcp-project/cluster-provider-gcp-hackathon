package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"

	crdutil "github.com/openmcp-project/controller-utils/pkg/crds"
	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	openmcpconst "github.com/openmcp-project/openmcp-operator/api/constants"
	"github.com/openmcp-project/openmcp-operator/lib/clusteraccess"

	"github.com/openmcp-project/cluster-provider-gcp/api/crds"
	"github.com/openmcp-project/cluster-provider-gcp/api/providerscheme"
)

func NewInitCommand(so *SharedOptions) *cobra.Command {
	opts := &InitOptions{SharedOptions: so}
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Install CRDs and perform one-time initialisation",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.PrintRawOptions(cmd)
			if err := opts.Complete(cmd.Context()); err != nil {
				return fmt.Errorf("error completing options: %w", err)
			}
			opts.PrintCompletedOptions(cmd)
			if opts.DryRun {
				cmd.Println("=== END OF DRY RUN ===")
				return nil
			}
			return opts.Run(cmd.Context())
		},
	}
	return cmd
}

type InitOptions struct {
	*SharedOptions
}

func (o *InitOptions) Complete(ctx context.Context) error {
	return o.SharedOptions.Complete()
}

func (o *InitOptions) Run(ctx context.Context) error {
	scheme := runtime.NewScheme()
	providerscheme.InstallOperatorAPIsPlatform(scheme)
	providerscheme.InstallCRDAPIs(scheme)
	if err := o.PlatformCluster.InitializeClient(scheme); err != nil {
		return err
	}

	log := o.Log.WithName("init")
	log.Info("Environment", "value", o.Environment)
	log.Info("ProviderName", "value", o.ProviderName)

	providerSystemNamespace := os.Getenv(openmcpconst.EnvVariablePodNamespace)
	if providerSystemNamespace == "" {
		return fmt.Errorf("environment variable %s is not set", openmcpconst.EnvVariablePodNamespace)
	}

	clusterAccessManager := clusteraccess.NewClusterAccessManager(o.PlatformCluster.Client(), o.ProviderName, providerSystemNamespace)
	clusterAccessManager.WithLogger(&log).
		WithInterval(10 * time.Second).
		WithTimeout(30 * time.Minute)

	log.Info("Creating/updating CRDs")
	crdManager := crdutil.NewCRDManager(openmcpconst.ClusterLabel, crds.CRDs)
	crdManager.AddCRDLabelToClusterMapping(clustersv1alpha1.PURPOSE_PLATFORM, o.PlatformCluster)
	if err := crdManager.CreateOrUpdateCRDs(ctx, &log); err != nil {
		return fmt.Errorf("error creating/updating CRDs: %w", err)
	}

	log.Info("Finished init command")
	return nil
}
