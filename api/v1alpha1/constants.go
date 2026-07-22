package v1alpha1

const (
	// GroupName is the API group name for this provider.
	GroupName = "gcp.cluster.open-control-plane.io"

	// ProviderConfigFinalizer is placed on ProviderConfig resources while they are being managed.
	ProviderConfigFinalizer = GroupName + "/providerconfig"

	// ClusterFinalizer is placed on Cluster resources while the GKE cluster exists.
	ClusterFinalizer = GroupName + "/cluster"

	// AccessRequestFinalizer is placed on AccessRequest resources while RBAC is being managed.
	AccessRequestFinalizer = GroupName + "/accessrequest"

	// ManagedByNameLabel marks resources on the GKE cluster that are managed by this provider.
	ManagedByNameLabel = GroupName + "/managed-by-name"
	// ManagedByNamespaceLabel marks resources on the GKE cluster that are managed by this provider.
	ManagedByNamespaceLabel = GroupName + "/managed-by-namespace"
)

const (
	// ConditionMeta is the condition type for meta-level checks (finalizer, etc.).
	ConditionMeta = "Meta"

	// ConditionCredentials is the condition type for GCP credential validation.
	ConditionCredentials = "Credentials"

	// ConditionClusterProfileManagement is the condition type for ClusterProfile lifecycle.
	ConditionClusterProfileManagement = "ClusterProfileManagement"

	// ClusterConditionGKEManagement is the condition type for GKE cluster lifecycle.
	ClusterConditionGKEManagement = "GKEManagement"

	// ClusterConditionForeignFinalizers is the condition type while waiting for external finalizers.
	ClusterConditionForeignFinalizers = "ForeignFinalizers"

	// AccessRequestConditionFoundClusterAndProfile tracks whether the referenced cluster/profile were resolved.
	AccessRequestConditionFoundClusterAndProfile = "FoundClusterAndProfile"

	// AccessRequestConditionGKEAccess tracks whether GKE cluster access was obtained.
	AccessRequestConditionGKEAccess = "GKEAccess"

	// AccessRequestConditionSecretExistsAndIsValid tracks whether the kubeconfig secret is up-to-date.
	AccessRequestConditionSecretExistsAndIsValid = "SecretExistsAndIsValid"

	// AccessRequestConditionCleanup tracks cleanup of stale resources on the GKE cluster.
	AccessRequestConditionCleanup = "Cleanup"
)

const (
	// ReasonGCPInteractionProblem indicates a GCP API call failed.
	ReasonGCPInteractionProblem = "GCPInteractionProblem"

	// ReasonGKEClusterInteractionProblem indicates the GKE cluster API server could not be reached.
	ReasonGKEClusterInteractionProblem = "GKEClusterInteractionProblem"

	// ReasonCredentialsProblem indicates the GCP credentials could not be loaded or are invalid.
	ReasonCredentialsProblem = "CredentialsProblem"

	// ReasonConfigurationProblem indicates invalid or missing configuration.
	ReasonConfigurationProblem = "ConfigurationProblem"

	// ReasonWaitingForDeletion indicates the provider is waiting for an external resource to be deleted.
	ReasonWaitingForDeletion = "WaitingForDeletion"

	// ReasonInternalError indicates an internal, unexpected error.
	ReasonInternalError = "InternalError"

	// ReasonUnknownProfile indicates a referenced ClusterProfile is not known to this provider.
	ReasonUnknownProfile = "UnknownProfile"
)
