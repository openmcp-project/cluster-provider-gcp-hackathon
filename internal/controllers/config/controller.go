package config

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

const ControllerName = "ProviderConfig"

type ProviderConfigReconciler struct {
	platformCluster *clusters.Cluster
	rc              *shared.RuntimeConfiguration
}

func NewProviderConfigReconciler(platformCluster *clusters.Cluster, rc *shared.RuntimeConfiguration) *ProviderConfigReconciler {
	return &ProviderConfigReconciler{
		platformCluster: platformCluster,
		rc:              rc,
	}
}

type ReconcileResult = ctrlutils.ReconcileResult[*providerv1alpha1.ProviderConfig]

func (r *ProviderConfigReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx).WithName(ControllerName)
	ctx = logging.NewContext(ctx, log)
	log.Info("Starting reconcile")

	r.rc.Lock.Lock()
	defer r.rc.Lock.Unlock()

	rr, profile := r.reconcile(ctx, req)

	if rr.Object != nil {
		r.rc.SetProviderConfig(req.Name, rr.Object)
		if profile != nil {
			r.rc.SetProfile(req.Name, profile)
		}
	} else if rr.ReconcileError == nil {
		r.rc.UnsetProviderConfig(req.Name)
		r.rc.UnsetProfile(req.Name)
	}

	return ctrlutils.NewOpenMCPStatusUpdaterBuilder[*providerv1alpha1.ProviderConfig]().
		WithNestedStruct("Status").
		WithPhaseUpdateFunc(func(obj *providerv1alpha1.ProviderConfig, rr ReconcileResult) (string, error) {
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
}

func (r *ProviderConfigReconciler) reconcile(ctx context.Context, req reconcile.Request) (ReconcileResult, *shared.Profile) {
	log := logging.FromContextOrPanic(ctx)

	pc := &providerv1alpha1.ProviderConfig{}
	if err := r.platformCluster.Client().Get(ctx, req.NamespacedName, pc); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Resource not found")
			return ReconcileResult{}, nil
		}
		return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("unable to get resource '%s': %w", req.String(), err), clusterconst.ReasonPlatformClusterInteractionProblem)}, nil
	}

	// only act on configs that belong to this provider instance
	if pc.Spec.ProviderRef.Name != r.rc.ProviderName() {
		log.Info("Skipping resource belonging to a different provider", "provider", pc.Spec.ProviderRef.Name)
		return ReconcileResult{}, nil
	}

	// handle operation annotation
	if pc.GetAnnotations() != nil {
		op, ok := pc.GetAnnotations()[openmcpconst.OperationAnnotation]
		if ok {
			switch op {
			case openmcpconst.OperationAnnotationValueIgnore:
				log.Info("Ignoring resource due to ignore operation annotation")
				return ReconcileResult{}, nil
			case openmcpconst.OperationAnnotationValueReconcile:
				if err := ctrlutils.EnsureAnnotation(ctx, r.platformCluster.Client(), pc, openmcpconst.OperationAnnotation, "", true, ctrlutils.DELETE); err != nil {
					return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("error removing operation annotation: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)}, nil
				}
			}
		}
	}

	if !pc.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, req, pc), nil
	}
	return r.handleCreateOrUpdate(ctx, req, pc)
}

func (r *ProviderConfigReconciler) handleCreateOrUpdate(ctx context.Context, req reconcile.Request, pc *providerv1alpha1.ProviderConfig) (ReconcileResult, *shared.Profile) {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Creating/updating resource")

	rr := ReconcileResult{
		Object:     pc,
		OldObject:  pc.DeepCopy(),
		Conditions: []metav1.Condition{},
	}
	createCon := ctrlutils.GenerateCreateConditionFunc(&rr)

	// ensure finalizer
	if controllerutil.AddFinalizer(pc, providerv1alpha1.ProviderConfigFinalizer) {
		if err := r.platformCluster.Client().Patch(ctx, pc, client.MergeFrom(rr.OldObject)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error adding finalizer: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
			createCon(providerv1alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr, nil
		}
	}
	createCon(providerv1alpha1.ConditionMeta, metav1.ConditionTrue, "FinalizerEnsured", "")

	// validate GCP credentials by loading them from the referenced secret
	creds, err := shared.LoadCredentials(ctx, r.platformCluster.Client(), pc.Spec.CredentialsSecretRef.Namespace, pc.Spec.CredentialsSecretRef.Name)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error loading credentials from secret '%s/%s': %w", pc.Spec.CredentialsSecretRef.Namespace, pc.Spec.CredentialsSecretRef.Name, err), providerv1alpha1.ReasonCredentialsProblem)
		createCon(providerv1alpha1.ConditionCredentials, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr, nil
	}
	createCon(providerv1alpha1.ConditionCredentials, metav1.ConditionTrue, "CredentialsLoaded", "")

	// build a profile for the cluster scheduler to discover
	profile := &shared.Profile{
		ProviderConfig: pc,
		Credentials:   creds,
	}

	// derive supported k8s versions from the GKE server config for this region
	gkeVersions, err := shared.FetchGKEServerConfig(ctx, creds, pc.Spec.ProjectID, pc.Spec.Region)
	if err != nil {
		log.Info("Could not fetch GKE server config for supported versions, proceeding without version list", "error", err.Error())
		// not a fatal error – the ClusterProfile will have an empty version list
	} else {
		profile.SupportedK8sVersions = gkeVersions
	}

	// create/update ClusterProfile
	profileName := shared.ProfileK8sName(r.rc.Environment(), r.rc.ProviderName(), pc.Name)
	actual := &clustersv1alpha1.ClusterProfile{}
	actual.SetName(profileName)
	if _, err := ctrl.CreateOrUpdate(ctx, r.platformCluster.Client(), actual, func() error {
		actual.Spec.ProviderRef.Name = r.rc.ProviderName()
		actual.Spec.ProviderConfigRef.Name = pc.Name
		actual.Spec.SupportedVersions = profile.SupportedVersionsForResource()
		return nil
	}); err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error creating/updating ClusterProfile '%s': %w", profileName, err), clusterconst.ReasonPlatformClusterInteractionProblem)
		createCon(providerv1alpha1.ConditionClusterProfileManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr, nil
	}
	createCon(providerv1alpha1.ConditionClusterProfileManagement, metav1.ConditionTrue, "ClusterProfileReconciled", "")

	return rr, profile
}

func (r *ProviderConfigReconciler) handleDelete(ctx context.Context, req reconcile.Request, pc *providerv1alpha1.ProviderConfig) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Deleting resource")

	rr := ReconcileResult{
		Object:     pc,
		OldObject:  pc.DeepCopy(),
		Conditions: []metav1.Condition{},
	}
	createCon := ctrlutils.GenerateCreateConditionFunc(&rr)

	// delete the ClusterProfile
	profileName := shared.ProfileK8sName(r.rc.Environment(), r.rc.ProviderName(), pc.Name)
	cp := &clustersv1alpha1.ClusterProfile{}
	cp.SetName(profileName)
	if err := r.platformCluster.Client().Delete(ctx, cp); err != nil && !apierrors.IsNotFound(err) {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error deleting ClusterProfile '%s': %w", profileName, err), clusterconst.ReasonPlatformClusterInteractionProblem)
		createCon(providerv1alpha1.ConditionClusterProfileManagement, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	createCon(providerv1alpha1.ConditionClusterProfileManagement, metav1.ConditionTrue, "ClusterProfileDeleted", "")

	// remove finalizer
	if controllerutil.RemoveFinalizer(pc, providerv1alpha1.ProviderConfigFinalizer) {
		if err := r.platformCluster.Client().Patch(ctx, pc, client.MergeFrom(rr.OldObject)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error removing finalizer: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
			createCon(providerv1alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}
	}
	rr.Object = nil
	return rr
}

func (r *ProviderConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&providerv1alpha1.ProviderConfig{}).
		WithEventFilter(predicate.And(
			predicate.NewPredicateFuncs(func(obj client.Object) bool {
				pc, ok := obj.(*providerv1alpha1.ProviderConfig)
				if !ok {
					return false
				}
				return pc.Spec.ProviderRef.Name == r.rc.ProviderName()
			}),
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
		Complete(r)
}
