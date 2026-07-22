package shared

import (
	"context"
	"fmt"
	"time"

	container "cloud.google.com/go/container/apiv1"
	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	providerv1alpha1 "github.com/openmcp-project/cluster-provider-gcp/api/v1alpha1"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// GKEProvisioningRequeueInterval is the interval between checks while a GKE cluster is being provisioned or deleted.
const GKEProvisioningRequeueInterval = 30 * time.Second

// GKEService wraps the GCP Cluster Manager client for easier testing.
type GKEService struct {
	client *container.ClusterManagerClient
}

func NewGKEService(ctx context.Context, creds *GCPCredentials) (*GKEService, error) {
	c, err := container.NewClusterManagerClient(ctx, creds.ClientOptions()...)
	if err != nil {
		return nil, fmt.Errorf("error creating GKE ClusterManager client: %w", err)
	}
	return &GKEService{client: c}, nil
}

func (s *GKEService) Close() {
	_ = s.client.Close()
}

// GetCluster fetches a GKE cluster by its full resource path (projects/P/locations/L/clusters/C).
// Returns nil, nil if the cluster does not exist.
func (s *GKEService) GetCluster(ctx context.Context, name string) (*containerpb.Cluster, error) {
	cluster, err := s.client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name})
	if err != nil {
		if IsGCPNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return cluster, nil
}

// CreateCluster submits a cluster creation request and returns the long-running operation.
func (s *GKEService) CreateCluster(ctx context.Context, req *containerpb.CreateClusterRequest) (*containerpb.Operation, error) {
	return s.client.CreateCluster(ctx, req)
}

// DeleteCluster submits a cluster deletion request and returns the long-running operation.
func (s *GKEService) DeleteCluster(ctx context.Context, name string) (*containerpb.Operation, error) {
	return s.client.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name})
}

// IsGCPNotFound returns true for gRPC NOT_FOUND errors from GCP APIs.
func IsGCPNotFound(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.NotFound
}

// BuildCreateClusterRequest builds the GKE cluster creation request from the Cluster and ProviderConfig.
func BuildCreateClusterRequest(parent, clusterName, k8sVersion string, pc *providerv1alpha1.ProviderConfig) *containerpb.CreateClusterRequest {
	var initialNodeCount int32 = 1
	machineType := "e2-standard-2"
	var diskSizeGB int32 = 50
	network := ""
	subnetwork := ""

	if pc.Spec.ClusterTemplate != nil {
		tmpl := pc.Spec.ClusterTemplate
		if tmpl.InitialNodeCount > 0 {
			initialNodeCount = tmpl.InitialNodeCount
		}
		if tmpl.MachineType != "" {
			machineType = tmpl.MachineType
		}
		if tmpl.DiskSizeGB > 0 {
			diskSizeGB = tmpl.DiskSizeGB
		}
		network = tmpl.Network
		subnetwork = tmpl.Subnetwork
	}

	cluster := &containerpb.Cluster{
		Name:             clusterName,
		InitialNodeCount: initialNodeCount,
		NodeConfig: &containerpb.NodeConfig{
			MachineType: machineType,
			DiskSizeGb:  diskSizeGB,
			OauthScopes: []string{
				"https://www.googleapis.com/auth/cloud-platform",
			},
		},
		Network:    network,
		Subnetwork: subnetwork,
	}
	if k8sVersion != "" {
		cluster.InitialClusterVersion = k8sVersion
	}

	return &containerpb.CreateClusterRequest{
		Parent:  parent,
		Cluster: cluster,
	}
}

// GKEAccess holds a client and REST config for accessing a GKE cluster.
type GKEAccess struct {
	Client     client.Client
	RESTConfig *rest.Config
	// TokenAudience is the audience to use when requesting SA tokens for this cluster.
	// On GKE this is the full container.googleapis.com resource path, not the IP.
	TokenAudience string
}

var gkeClientScheme *k8sruntime.Scheme

func init() {
	gkeClientScheme = k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(gkeClientScheme))
}

// GetGKEAccess builds a controller-runtime client for the given GKE cluster using its kubeconfig
// obtained from the GKE API (generates a short-lived credential via gke-gcloud-auth-plugin equivalent).
func GetGKEAccess(ctx context.Context, creds *GCPCredentials, clusterPath string) (*GKEAccess, error) {
	svc, err := NewGKEService(ctx, creds)
	if err != nil {
		return nil, fmt.Errorf("error creating GKE service: %w", err)
	}
	defer svc.Close()

	cluster, err := svc.GetCluster(ctx, clusterPath)
	if err != nil {
		return nil, fmt.Errorf("error getting GKE cluster '%s': %w", clusterPath, err)
	}
	if cluster == nil {
		return nil, fmt.Errorf("GKE cluster '%s' not found", clusterPath)
	}

	// Build a kubeconfig from the cluster's master auth / endpoint.
	// We use the cluster CA and a short-lived access token from the credential token source.
	token, err := creds.TokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("error obtaining GCP access token: %w", err)
	}

	kubeconfig := buildGKEKubeconfig(cluster.Endpoint, cluster.MasterAuth.ClusterCaCertificate, token.AccessToken)
	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("error building REST config for GKE cluster '%s': %w", clusterPath, err)
	}

	cl, err := client.New(restCfg, client.Options{Scheme: gkeClientScheme})
	if err != nil {
		return nil, fmt.Errorf("error creating client for GKE cluster '%s': %w", clusterPath, err)
	}

	return &GKEAccess{
		Client:        cl,
		RESTConfig:    restCfg,
		TokenAudience: "https://container.googleapis.com/v1/" + clusterPath,
	}, nil
}

// buildGKEKubeconfig produces a minimal kubeconfig YAML for the given GKE cluster endpoint.
func buildGKEKubeconfig(endpoint, caCertBase64, accessToken string) []byte {
	return []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: gke-cluster
  cluster:
    server: https://%s
    certificate-authority-data: %s
contexts:
- name: gke-context
  context:
    cluster: gke-cluster
    user: gke-user
current-context: gke-context
users:
- name: gke-user
  user:
    token: %s
`, endpoint, caCertBase64, accessToken))
}

// FetchGKEServerConfig retrieves supported Kubernetes versions from the GKE server config for a region.
func FetchGKEServerConfig(ctx context.Context, creds *GCPCredentials, projectID, region string) ([]K8sVersion, error) {
	svc, err := NewGKEService(ctx, creds)
	if err != nil {
		return nil, err
	}
	defer svc.Close()

	name := fmt.Sprintf("projects/%s/locations/%s", projectID, region)
	cfg, err := svc.client.GetServerConfig(ctx, &containerpb.GetServerConfigRequest{Name: name})
	if err != nil {
		return nil, fmt.Errorf("error fetching GKE server config for '%s': %w", name, err)
	}

	versions := make([]K8sVersion, 0, len(cfg.ValidMasterVersions))
	for _, v := range cfg.ValidMasterVersions {
		versions = append(versions, K8sVersion{Version: v})
	}
	return versions, nil
}
