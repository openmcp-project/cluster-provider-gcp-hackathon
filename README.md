# ClusterProvider: GCP (GKE)

A [ClusterProvider](https://github.com/openmcp-project/docs) for [Google Kubernetes Engine (GKE)](https://cloud.google.com/kubernetes-engine). It reconciles `Cluster` resources by creating and managing GKE clusters via the [GKE API](https://cloud.google.com/go/docs/reference/cloud.google.com/go/container/latest/apiv1) and grants access to them via `AccessRequest` resources.

---

## Running locally

### Prerequisites

- A running **platform cluster** (e.g. a local [kind](https://kind.sigs.k8s.io/) cluster) with its kubeconfig available
- A GCP project with the **Kubernetes Engine API** enabled
- A GCP **service account** with the `roles/container.admin` role (or equivalent) and a JSON key file, **or** a valid GCP bearer token
- `go` 1.23+

### 1. Create the GCP credentials secret

The provider reads GCP credentials from a Kubernetes Secret. Create it on the platform cluster before starting:

**Option A — Service account JSON key (recommended):**

```shell
kubectl create secret generic gcp-credentials \
  --namespace openmcp-system \
  --from-file=credentials.json=/path/to/sa-key.json
```

**Option B — Bearer token:**

```shell
kubectl create secret generic gcp-credentials \
  --namespace openmcp-system \
  --from-literal=token=$(gcloud auth print-access-token)
```

> Bearer tokens expire after ~1 hour. For long-running local sessions, use a service account JSON key.

### 2. Install CRDs

The `init` subcommand installs the provider's CRDs onto the platform cluster and exits:

```shell
export POD_NAMESPACE=openmcp-system

go run ./cmd/cluster-provider-gcp/main.go init \
  --kubeconfig /path/to/platform-kubeconfig \
  --environment local \
  --provider-name gcp
```

`--environment` is a unique string that scopes resource names (ClusterProfiles, GKE cluster names, etc.) for this provider instance. Use any short identifier — `local` works fine for development.

`--provider-name` must match the `name` field of the `ClusterProvider` resource registered in the openmcp-operator, or any short name you choose when testing standalone.

### 3. Create a ProviderConfig

Apply a `ProviderConfig` to tell the provider which GCP project/region to use and where to find the credentials:

```yaml
apiVersion: gcp.cluster.open-control-plane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: my-gcp-config
spec:
  providerRef:
    name: gcp                     # must match --provider-name
  projectID: my-gcp-project-id
  region: europe-west1
  credentialsSecretRef:
    name: gcp-credentials
    namespace: openmcp-system
  clusterTemplate:                # optional defaults applied to all clusters
    initialNodeCount: 1
    machineType: e2-standard-2
    diskSizeGB: 50
```

```shell
kubectl apply -f providerconfig.yaml
```

The provider will create a `ClusterProfile` named `<environment>.<provider-name>.<providerconfig-name>` (e.g. `local.gcp.my-gcp-config`) once it reconciles the config successfully.

### 4. Start the controller

```shell
export POD_NAMESPACE=openmcp-system

go run ./cmd/cluster-provider-gcp/main.go run \
  --kubeconfig /path/to/platform-kubeconfig \
  --environment local \
  --provider-name gcp
```

Useful additional flags:

| Flag | Default | Description |
|---|---|---|
| `--leader-elect` | `false` | Enable leader election (needed for multi-replica deployments) |
| `--metrics-bind-address` | `0` | Set to `:8080` to expose Prometheus metrics |
| `--health-probe-bind-address` | `:8081` | Liveness/readiness probe address |
| `--metrics-secure` | `true` | Set to `false` to serve metrics over plain HTTP |

### 5. Trigger cluster provisioning (standalone test)

When running without the full openmcp-operator, you can create `Cluster` resources manually to test provisioning. The `spec.profile` field must reference the `ClusterProfile` created in step 3:

```yaml
apiVersion: clusters.openmcp.cloud/v1alpha1
kind: Cluster
metadata:
  name: my-test-cluster
  namespace: openmcp-system
spec:
  tenancy: Shared
  profile: local.gcp.my-gcp-config
  kubernetes:
    version: "1.31"
```

```shell
kubectl apply -f cluster.yaml
```

Watch the cluster status:

```shell
kubectl get clusters -n openmcp-system -w
```

Once the `ClusterConditionGKEManagement` condition is `True` and the phase is `Ready`, the GKE cluster's API server endpoint is available in `status.endpoints`.

### 6. Request cluster access

```yaml
apiVersion: clusters.openmcp.cloud/v1alpha1
kind: AccessRequest
metadata:
  name: my-access
  namespace: openmcp-system
  labels:
    clusters.openmcp.cloud/provider: gcp
    clusters.openmcp.cloud/profile: local.gcp.my-gcp-config
spec:
  clusterRef:
    name: my-test-cluster
    namespace: openmcp-system
  token:
    permissions:
    - rules:
      - apiGroups: ["*"]
        resources: ["*"]
        verbs: ["*"]
```

```shell
kubectl apply -f accessrequest.yaml
```

Once granted, a Secret containing a kubeconfig is written to the same namespace:

```shell
kubectl get secret my-access.kubeconfig -n openmcp-system -o jsonpath='{.data.kubeconfig}' | base64 -d > /tmp/gke-kubeconfig.yaml
kubectl --kubeconfig /tmp/gke-kubeconfig.yaml get nodes
```

---

## ProviderConfig reference

| Field | Required | Description |
|---|---|---|
| `spec.providerRef.name` | yes | Must match the `--provider-name` flag. Immutable. |
| `spec.projectID` | yes | GCP project ID (not project number). |
| `spec.region` | yes | GCP region, e.g. `europe-west1`. Clusters are created as regional clusters. |
| `spec.credentialsSecretRef.name` | yes | Name of the Secret with GCP credentials. |
| `spec.credentialsSecretRef.namespace` | yes | Namespace of the Secret. |
| `spec.clusterTemplate.initialNodeCount` | no | Default number of nodes in the default node pool. Defaults to `1`. |
| `spec.clusterTemplate.machineType` | no | GCP machine type, e.g. `e2-standard-4`. |
| `spec.clusterTemplate.diskSizeGB` | no | Boot disk size in GB. Minimum `10`. |
| `spec.clusterTemplate.network` | no | VPC network name or self-link. Uses `default` if empty. |
| `spec.clusterTemplate.subnetwork` | no | VPC subnetwork name or self-link. |

---

## Credentials secret format

The Secret referenced by `credentialsSecretRef` must contain **one** of:

| Key | Value |
|---|---|
| `credentials.json` | Full GCP service account JSON key file contents. |
| `token` | A GCP bearer access token string. |

Service account JSON keys are strongly preferred for production use — they do not expire and the provider can refresh tokens automatically.
