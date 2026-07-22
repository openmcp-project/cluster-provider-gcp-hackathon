package cluster

import (
	"context"
	"fmt"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	ctrlutils "github.com/openmcp-project/controller-utils/pkg/controller"
	errutils "github.com/openmcp-project/controller-utils/pkg/errors"
	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	clusterconst "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1/constants"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	openmcpconst "github.com/openmcp-project/openmcp-operator/api/constants"

	providerv1alpha1 "github.com/openmcp-project/cluster-provider-gcp/api/v1alpha1"
	"github.com/openmcp-project/cluster-provider-gcp/internal/controllers/shared"
)

const ControllerName = "Cluster"

type ClusterReconciler struct {
	platformCluster *clusters.Cluster
	rc              *shared.RuntimeConfiguration
}

func NewClusterReconciler(platformCluster *clusters.Cluster, rc *shared.RuntimeConfiguration) *ClusterReconciler {
	return &ClusterReconciler{
		platformCluster: platformCluster,
		rc:              rc,
	}
}

type ReconcileResult = ctrlutils.ReconcileResult[*clustersv1alpha1.Cluster]

func (r *ClusterReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx).WithName(ControllerName)
	ctx = logging.NewContext(ctx, log)
	log.Info("Starting reconcile")

	r.rc.Lock.RLock()
	defer r.rc.Lock.RUnlock()

	rr := r.reconcile(ctx, req)
	log.Info("Reconcile result", "conditions", len(rr.Conditions), "error", rr.ReconcileError, "requeueAfter", rr.Result.RequeueAfter)

	result, err := ctrlutils.NewOpenMCPStatusUpdaterBuilder[*clustersv1alpha1.Cluster]().
		WithNestedStruct("Status").
		WithPhaseUpdateFunc(func(obj *clustersv1alpha1.Cluster, rr ReconcileResult) (string, error) {
			if rr.Object != nil && !rr.Object.DeletionTimestamp.IsZero() {
				return commonapi.StatusPhaseTerminating, nil
			}
			for _, cond := range obj.Status.Conditions {
				if cond.Status != metav1.ConditionTrue {
					return commonapi.StatusPhaseProgressing, nil
				}
			}
			if len(obj.Status.Conditions) == 0 {
				return commonapi.StatusPhaseProgressing, nil
			}
			return commonapi.StatusPhaseReady, nil
		}).
		WithConditionUpdater(false).
		Build().
		UpdateStatus(ctx, r.platformCluster.Client(), rr)
	log.Info("Status update result", "result", result, "error", err)
	return result, err
}

func (r *ClusterReconciler) reconcile(ctx context.Context, req reconcile.Request) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)

	c := &clustersv1alpha1.Cluster{}
	if err := r.platformCluster.Client().Get(ctx, req.NamespacedName, c); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Resource not found")
			return ReconcileResult{}
		}
		return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("unable to get resource '%s': %w", req.String(), err), clusterconst.ReasonPlatformClusterInteractionProblem)}
	}

	// handle operation annotation
	if c.GetAnnotations() != nil {
		op, ok := c.GetAnnotations()[openmcpconst.OperationAnnotation]
		if ok {
			switch op {
			case openmcpconst.OperationAnnotationValueIgnore:
				log.Info("Ignoring resource due to ignore operation annotation")
				return ReconcileResult{}
			case openmcpconst.OperationAnnotationValueReconcile:
				if err := ctrlutils.EnsureAnnotation(ctx, r.platformCluster.Client(), c, openmcpconst.OperationAnnotation, "", true, ctrlutils.DELETE); err != nil {
					return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("error removing operation annotation: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)}
				}
			}
		}
	}

	// check that this provider owns the referenced profile
	profile := r.rc.GetProfile(c.Spec.Profile)
	if profile == nil {
		log.Info("Ignoring cluster: profile not owned by this provider", "profile", c.Spec.Profile)
		return ReconcileResult{}
	}

	rr := ReconcileResult{
		Object:     c,
		OldObject:  c.DeepCopy(),
		Conditions: []metav1.Condition{},
	}

	if !c.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, c, profile, &rr)
	}
	return r.handleCreateOrUpdate(ctx, c, profile, &rr)
}

func (r *ClusterReconciler) handleCreateOrUpdate(ctx context.Context, c *clustersv1alpha1.Cluster, profile *shared.Profile, rr *ReconcileResult) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Creating/updating GKE cluster")
	createCon := ctrlutils.GenerateCreateConditionFunc(rr)

	// ensure finalizer
	if controllerutil.AddFinalizer(c, providerv1alpha1.ClusterFinalizer) {
		if err := r.platformCluster.Client().Patch(ctx, c, client.MergeFrom(rr.OldObject)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error adding finalizer: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
			createCon(providerv1alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return *rr
		}
		// Update OldObject so the status patch uses the correct resourceVersion after the metadata patch.
		rr.OldObject = c.DeepCopy()
	}
	createCon(providerv1alpha1.ConditionMeta, metav1.ConditionTrue, "FinalizerEnsured", "")

	pc := profile.ProviderConfig
	gkeClusterName := shared.GKEClusterName(c.Namespace, c.Name)

	gkeSvc, err := shared.NewGKEService(ctx, profile.Credentials)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error creating GKE client: %w", err), providerv1alpha1.ReasonCredentialsProblem)
		createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return *rr
	}
	defer gkeSvc.Close()

	parent := fmt.Sprintf("projects/%s/locations/%s", pc.Spec.ProjectID, pc.Spec.Region)
	clusterPath := fmt.Sprintf("%s/clusters/%s", parent, gkeClusterName)

	existingCluster, err := gkeSvc.GetCluster(ctx, clusterPath)
	if err != nil && !shared.IsGCPNotFound(err) {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error fetching GKE cluster '%s': %w", clusterPath, err), providerv1alpha1.ReasonGCPInteractionProblem)
		createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return *rr
	}

	if existingCluster == nil {
		// cluster does not exist yet – create it
		k8sVersion := c.Spec.Kubernetes.Version
		if k8sVersion == "" && len(profile.SupportedK8sVersions) > 0 {
			k8sVersion = profile.SupportedK8sVersions[0].Version
		}
		req := shared.BuildCreateClusterRequest(parent, gkeClusterName, k8sVersion, pc)
		log.Info("Creating GKE cluster", "clusterName", gkeClusterName, "parent", parent)
		if _, err := gkeSvc.CreateCluster(ctx, req); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error creating GKE cluster '%s': %w", gkeClusterName, err), providerv1alpha1.ReasonGCPInteractionProblem)
			createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return *rr
		}
		createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, "ClusterProvisioning", "GKE cluster creation initiated, waiting for it to become ready")
		rr.Result.RequeueAfter = shared.GKEProvisioningRequeueInterval
		return *rr
	}

	// cluster exists – reflect its status
	switch existingCluster.Status {
	case containerpb.Cluster_PROVISIONING, containerpb.Cluster_RECONCILING:
		createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, "ClusterProvisioning", fmt.Sprintf("GKE cluster is in state %s", existingCluster.Status.String()))
		rr.Result.RequeueAfter = shared.GKEProvisioningRequeueInterval
		return *rr
	case containerpb.Cluster_STOPPING, containerpb.Cluster_ERROR, containerpb.Cluster_DEGRADED:
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("GKE cluster is in unhealthy state: %s", existingCluster.Status.String()), providerv1alpha1.ReasonGCPInteractionProblem)
		createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return *rr
	}
	// RUNNING
	createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionTrue, "ClusterRunning", "")

	// expose the API server endpoint
	if existingCluster.Endpoint != "" {
		c.Status.Endpoints = clustersv1alpha1.Endpoints{}
		c.Status.Endpoints.Set("default", "https://"+existingCluster.Endpoint)
	}

	// set provider labels on Cluster resource
	labelsChanged, rerr := r.ensureClusterLabels(ctx, c, existingCluster.CurrentMasterVersion)
	if rerr != nil {
		rr.ReconcileError = rerr
		createCon(providerv1alpha1.ConditionMeta, metav1.ConditionFalse, rerr.Reason(), rerr.Error())
		return *rr
	}
	if labelsChanged {
		// Update OldObject so the status patch uses the correct resourceVersion after the metadata patch.
		rr.OldObject = c.DeepCopy()
	}
	createCon(providerv1alpha1.ConditionMeta, metav1.ConditionTrue, "LabelsEnsured", "")

	return *rr
}

func (r *ClusterReconciler) handleDelete(ctx context.Context, c *clustersv1alpha1.Cluster, profile *shared.Profile, rr *ReconcileResult) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Deleting GKE cluster")
	createCon := ctrlutils.GenerateCreateConditionFunc(rr)

	// wait for foreign finalizers
	foreignFinalizers := []string{}
	for _, fin := range c.Finalizers {
		if fin != providerv1alpha1.ClusterFinalizer {
			foreignFinalizers = append(foreignFinalizers, fin)
		}
	}
	if len(foreignFinalizers) > 0 {
		log.Info("Waiting for foreign finalizers", "finalizers", foreignFinalizers)
		createCon(providerv1alpha1.ClusterConditionForeignFinalizers, metav1.ConditionFalse, providerv1alpha1.ReasonWaitingForDeletion, fmt.Sprintf("Waiting for foreign finalizers: %v", foreignFinalizers))
		return *rr
	}
	createCon(providerv1alpha1.ClusterConditionForeignFinalizers, metav1.ConditionTrue, "NoForeignFinalizers", "")

	pc := profile.ProviderConfig
	gkeClusterName := shared.GKEClusterName(c.Namespace, c.Name)
	clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", pc.Spec.ProjectID, pc.Spec.Region, gkeClusterName)

	gkeSvc, err := shared.NewGKEService(ctx, profile.Credentials)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error creating GKE client: %w", err), providerv1alpha1.ReasonCredentialsProblem)
		createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return *rr
	}
	defer gkeSvc.Close()

	existingCluster, err := gkeSvc.GetCluster(ctx, clusterPath)
	if err != nil && !shared.IsGCPNotFound(err) {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error fetching GKE cluster '%s': %w", clusterPath, err), providerv1alpha1.ReasonGCPInteractionProblem)
		createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return *rr
	}

	if existingCluster != nil {
		if existingCluster.Status == containerpb.Cluster_STOPPING {
			log.Info("GKE cluster is already being deleted, waiting")
			createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, providerv1alpha1.ReasonWaitingForDeletion, "Waiting for GKE cluster to be deleted")
			rr.Result.RequeueAfter = shared.GKEProvisioningRequeueInterval
			return *rr
		}
		log.Info("Deleting GKE cluster", "clusterPath", clusterPath)
		if _, err := gkeSvc.DeleteCluster(ctx, clusterPath); err != nil && !shared.IsGCPNotFound(err) {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error deleting GKE cluster '%s': %w", clusterPath, err), providerv1alpha1.ReasonGCPInteractionProblem)
			createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return *rr
		}
		createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionFalse, providerv1alpha1.ReasonWaitingForDeletion, "GKE cluster deletion initiated")
		rr.Result.RequeueAfter = shared.GKEProvisioningRequeueInterval
		return *rr
	}
	createCon(providerv1alpha1.ClusterConditionGKEManagement, metav1.ConditionTrue, "ClusterDeleted", "GKE cluster no longer exists")

	// remove finalizer
	if controllerutil.RemoveFinalizer(c, providerv1alpha1.ClusterFinalizer) {
		if err := r.platformCluster.Client().Patch(ctx, c, client.MergeFrom(rr.OldObject)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error removing finalizer: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
			createCon(providerv1alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return *rr
		}
	}
	rr.Object = nil
	return *rr
}

func (r *ClusterReconciler) ensureClusterLabels(ctx context.Context, c *clustersv1alpha1.Cluster, k8sVersion string) (bool, errutils.ReasonableError) {
	old := c.DeepCopy()
	changed := false
	if c.Labels == nil {
		c.Labels = map[string]string{}
	}
	if c.Labels[clustersv1alpha1.K8sVersionLabel] != k8sVersion {
		c.Labels[clustersv1alpha1.K8sVersionLabel] = k8sVersion
		changed = true
	}
	if c.Labels[clustersv1alpha1.ProviderLabel] != r.rc.ProviderName() {
		c.Labels[clustersv1alpha1.ProviderLabel] = r.rc.ProviderName()
		changed = true
	}
	if changed {
		if err := r.platformCluster.Client().Patch(ctx, c, client.MergeFrom(old)); err != nil {
			return false, errutils.WithReason(fmt.Errorf("error patching labels on Cluster: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
		}
	}
	return changed, nil
}

func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clustersv1alpha1.Cluster{}).
		WithEventFilter(predicate.And(
			predicate.Or(
				predicate.GenerationChangedPredicate{},
				ctrlutils.DeletionTimestampChangedPredicate{},
				ctrlutils.GotAnnotationPredicate(openmcpconst.OperationAnnotation, openmcpconst.OperationAnnotationValueReconcile),
				ctrlutils.LostAnnotationPredicate(openmcpconst.OperationAnnotation, openmcpconst.OperationAnnotationValueIgnore),
			),
			predicate.Not(
				ctrlutils.HasAnnotationPredicate(openmcpconst.OperationAnnotation, openmcpconst.OperationAnnotationValueIgnore),
			),
		)).
		Watches(&clustersv1alpha1.ClusterProfile{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			if obj == nil {
				return nil
			}
			clusterList := &clustersv1alpha1.ClusterList{}
			if err := r.platformCluster.Client().List(ctx, clusterList, client.MatchingFields{
				"spec.profile": obj.GetName(),
			}); err != nil {
				return nil
			}
			reqs := make([]reconcile.Request, len(clusterList.Items))
			for i, cl := range clusterList.Items {
				reqs[i] = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&cl)}
			}
			return reqs
		}), builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}
