package shared

import (
	"fmt"
	"sync"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	ctrlutils "github.com/openmcp-project/controller-utils/pkg/controller"

	providerv1alpha1 "github.com/openmcp-project/cluster-provider-gcp/api/v1alpha1"
)

// AccessRequestServiceAccountNamespace is the namespace used for service accounts created on GKE clusters.
const AccessRequestServiceAccountNamespace = "openmcp-system"

// RuntimeConfiguration is the shared runtime state for all controllers of this cluster provider.
// It is safe to read concurrently; writes must hold the write lock.
type RuntimeConfiguration struct {
	Lock *sync.RWMutex

	environment  string
	providerName string

	// providerConfigs maps ProviderConfig name → resource
	providerConfigs map[string]*providerv1alpha1.ProviderConfig
	// profiles maps ClusterProfile k8s-name → Profile
	profiles map[string]*Profile
}

func NewRuntimeConfiguration(environment, providerName string) *RuntimeConfiguration {
	return &RuntimeConfiguration{
		Lock:            &sync.RWMutex{},
		environment:     environment,
		providerName:    providerName,
		providerConfigs: make(map[string]*providerv1alpha1.ProviderConfig),
		profiles:        make(map[string]*Profile),
	}
}

func (rc *RuntimeConfiguration) Environment() string { return rc.environment }
func (rc *RuntimeConfiguration) ProviderName() string { return rc.providerName }

func (rc *RuntimeConfiguration) SetProviderConfig(name string, pc *providerv1alpha1.ProviderConfig) {
	rc.providerConfigs[name] = pc.DeepCopy()
}

func (rc *RuntimeConfiguration) UnsetProviderConfig(name string) {
	delete(rc.providerConfigs, name)
}

func (rc *RuntimeConfiguration) SetProfile(providerConfigName string, p *Profile) {
	key := ProfileK8sName(rc.environment, rc.providerName, providerConfigName)
	if p == nil {
		delete(rc.profiles, key)
		return
	}
	rc.profiles[key] = p.DeepCopy()
}

func (rc *RuntimeConfiguration) UnsetProfile(providerConfigName string) {
	rc.SetProfile(providerConfigName, nil)
}

// GetProfile returns the Profile for the given ClusterProfile k8s name, or nil if not found.
// The caller must hold at least a read lock.
func (rc *RuntimeConfiguration) GetProfile(profileK8sName string) *Profile {
	p := rc.profiles[profileK8sName]
	if p == nil {
		return nil
	}
	return p.DeepCopy()
}

// Profile bundles the ProviderConfig, its loaded credentials and derived version information.
type Profile struct {
	ProviderConfig       *providerv1alpha1.ProviderConfig
	Credentials          *GCPCredentials
	SupportedK8sVersions []K8sVersion
}

func (p *Profile) DeepCopy() *Profile {
	if p == nil {
		return nil
	}
	versions := make([]K8sVersion, len(p.SupportedK8sVersions))
	copy(versions, p.SupportedK8sVersions)
	return &Profile{
		ProviderConfig:       p.ProviderConfig.DeepCopy(),
		Credentials:          p.Credentials,
		SupportedK8sVersions: versions,
	}
}

func (p *Profile) SupportedVersionsForResource() []clustersv1alpha1.SupportedK8sVersion {
	out := make([]clustersv1alpha1.SupportedK8sVersion, len(p.SupportedK8sVersions))
	for i, v := range p.SupportedK8sVersions {
		out[i] = clustersv1alpha1.SupportedK8sVersion{
			Version:    v.Version,
			Deprecated: v.Deprecated,
		}
	}
	return out
}

// K8sVersion represents a supported Kubernetes version.
type K8sVersion struct {
	Version    string
	Deprecated bool
}

// ProfileK8sName returns the Kubernetes resource name for a ClusterProfile.
// Format: <environment>.<providerName>.<providerConfigName>
func ProfileK8sName(environment, providerName, providerConfigName string) string {
	return fmt.Sprintf("%s.%s.%s", environment, providerName, providerConfigName)
}

// GKEClusterName derives a deterministic, DNS-label-safe GKE cluster name from the Cluster resource.
func GKEClusterName(clusterNamespace, clusterName string) string {
	return fmt.Sprintf("c-%s", ctrlutils.NameHashSHAKE128Base32(clusterNamespace, clusterName))
}
