package v1alpha1

import (
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ProviderConfigSpec defines the desired state of ProviderConfig for GCP GKE clusters.
type ProviderConfigSpec struct {
	// ProviderRef is a reference to the ClusterProvider resource this configuration belongs to.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="providerRef is immutable"
	ProviderRef commonapi.LocalObjectReference `json:"providerRef"`

	// ProjectID is the GCP project ID in which to create GKE clusters.
	// +kubebuilder:validation:MinLength=1
	ProjectID string `json:"projectID"`

	// Region is the GCP region (e.g. "europe-west1") in which to create GKE clusters.
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`

	// CredentialsSecretRef references a Secret in the same namespace as the ProviderConfig
	// that contains GCP service account credentials.
	// The secret must have a key "credentials.json" with the service account JSON key file contents,
	// or a key "token" with a bearer token.
	CredentialsSecretRef CredentialsSecretRef `json:"credentialsSecretRef"`

	// ClusterTemplate contains default settings applied to all GKE clusters created from this config.
	// +optional
	ClusterTemplate *ClusterTemplate `json:"clusterTemplate,omitempty"`
}

// CredentialsSecretRef holds a reference to a secret containing GCP credentials.
type CredentialsSecretRef struct {
	// Name is the name of the secret.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace is the namespace of the secret.
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
}

// ClusterTemplate defines the default GKE cluster configuration.
type ClusterTemplate struct {
	// InitialNodeCount is the number of nodes to create in the default node pool.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	InitialNodeCount int32 `json:"initialNodeCount,omitempty"`

	// MachineType is the GCP machine type for the default node pool (e.g. "e2-standard-4").
	// +optional
	MachineType string `json:"machineType,omitempty"`

	// DiskSizeGB is the disk size in GB for each node.
	// +optional
	// +kubebuilder:validation:Minimum=10
	DiskSizeGB int32 `json:"diskSizeGB,omitempty"`

	// Network is the VPC network name or self-link. Uses the "default" network if empty.
	// +optional
	Network string `json:"network,omitempty"`

	// Subnetwork is the VPC subnetwork name or self-link.
	// +optional
	Subnetwork string `json:"subnetwork,omitempty"`
}

// ProviderConfigStatus defines the observed state of ProviderConfig.
type ProviderConfigStatus struct {
	commonapi.Status `json:",inline"`
}

// ProviderConfig is the Schema for the GCP cluster provider configuration.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=gcpcfg
// +kubebuilder:printcolumn:JSONPath=".status.phase",name="Phase",type=string
// +kubebuilder:metadata:labels="openmcp.cloud/cluster=platform"
type ProviderConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProviderConfigSpec   `json:"spec,omitempty"`
	Status ProviderConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProviderConfigList contains a list of ProviderConfig.
type ProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ProviderConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &ProviderConfig{}, &ProviderConfigList{})
		return nil
	})
}
