package accessrequest

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openmcp-project/controller-utils/pkg/clusteraccess"
	"github.com/openmcp-project/controller-utils/pkg/clusters"
	ctrlutils "github.com/openmcp-project/controller-utils/pkg/controller"
	errutils "github.com/openmcp-project/controller-utils/pkg/errors"
	"github.com/openmcp-project/controller-utils/pkg/logging"
	"github.com/openmcp-project/controller-utils/pkg/pairs"
	"github.com/openmcp-project/controller-utils/pkg/resources"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	clusterconst "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1/constants"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	openmcpconst "github.com/openmcp-project/openmcp-operator/api/constants"
	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"

	providerv1alpha1 "github.com/openmcp-project/cluster-provider-gcp/api/v1alpha1"
	"github.com/openmcp-project/cluster-provider-gcp/internal/controllers/shared"
)

const (
	ControllerName                          = "AccessRequest"
	RenewTokenAfterValidityPercentagePassed = 0.8

	KindRole               = "Role"
	KindClusterRole        = "ClusterRole"
	KindRoleBinding        = "RoleBinding"
	KindClusterRoleBinding = "ClusterRoleBinding"
)

var DefaultTokenValidity = 30 * 24 * time.Hour

type AccessRequestReconciler struct {
	platformCluster *clusters.Cluster
	rc              *shared.RuntimeConfiguration
}

func NewAccessRequestReconciler(platformCluster *clusters.Cluster, rc *shared.RuntimeConfiguration) *AccessRequestReconciler {
	return &AccessRequestReconciler{
		platformCluster: platformCluster,
		rc:              rc,
	}
}

type ReconcileResult = ctrlutils.ReconcileResult[*clustersv1alpha1.AccessRequest]

func (r *AccessRequestReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx).WithName(ControllerName)
	ctx = logging.NewContext(ctx, log)
	log.Info("Starting reconcile")

	r.rc.Lock.RLock()
	defer r.rc.Lock.RUnlock()

	rr := r.reconcile(ctx, req)

	return ctrlutils.NewOpenMCPStatusUpdaterBuilder[*clustersv1alpha1.AccessRequest]().
		WithNestedStruct("Status").
		WithPhaseUpdateFunc(func(obj *clustersv1alpha1.AccessRequest, rr ReconcileResult) (string, error) {
			if rr.ReconcileError != nil || rr.Object == nil {
				return clustersv1alpha1.REQUEST_PENDING, nil
			}
			return clustersv1alpha1.REQUEST_GRANTED, nil
		}).
		WithConditionUpdater(false).
		Build().
		UpdateStatus(ctx, r.platformCluster.Client(), rr)
}

func (r *AccessRequestReconciler) reconcile(ctx context.Context, req reconcile.Request) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)

	ar := &clustersv1alpha1.AccessRequest{}
	if err := r.platformCluster.Client().Get(ctx, req.NamespacedName, ar); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Resource not found")
			return ReconcileResult{}
		}
		return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("unable to get resource '%s': %w", req.String(), err), clusterconst.ReasonPlatformClusterInteractionProblem)}
	}

	if !libutils.IsClusterProviderResponsibleForAccessRequest(ar, r.rc.ProviderName()) {
		log.Info("Not responsible for this AccessRequest")
		return ReconcileResult{}
	}

	enforceReconcile := false
	if ar.GetAnnotations() != nil {
		op, ok := ar.GetAnnotations()[openmcpconst.OperationAnnotation]
		if ok {
			switch op {
			case openmcpconst.OperationAnnotationValueIgnore:
				log.Info("Ignoring resource due to ignore operation annotation")
				return ReconcileResult{}
			case openmcpconst.OperationAnnotationValueReconcile:
				enforceReconcile = true
				if err := ctrlutils.EnsureAnnotation(ctx, r.platformCluster.Client(), ar, openmcpconst.OperationAnnotation, "", true, ctrlutils.DELETE); err != nil {
					return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("error removing operation annotation: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)}
				}
			}
		}
	}

	rr := ReconcileResult{
		Object:     ar,
		OldObject:  ar.DeepCopy(),
		Conditions: []metav1.Condition{},
	}

	c, profile, rerr := r.resolveClusterAndProfile(ctx, ar)
	if rerr != nil {
		rr.Conditions = append(rr.Conditions, metav1.Condition{
			Type:               providerv1alpha1.AccessRequestConditionFoundClusterAndProfile,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: ar.Generation,
			Reason:             rerr.Reason(),
			Message:            rerr.Error(),
		})
		rr.ReconcileError = rerr
		return rr
	}
	rr.Conditions = append(rr.Conditions, metav1.Condition{
		Type:               providerv1alpha1.AccessRequestConditionFoundClusterAndProfile,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ar.Generation,
		Reason:             "Resolved",
	})

	if !ar.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, req, ar, c, profile, rr)
	}
	return r.handleCreateOrUpdate(ctx, req, ar, c, profile, enforceReconcile, rr)
}

func (r *AccessRequestReconciler) handleCreateOrUpdate(ctx context.Context, req reconcile.Request, ar *clustersv1alpha1.AccessRequest, c *clustersv1alpha1.Cluster, profile *shared.Profile, enforceReconcile bool, rr ReconcileResult) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Creating/updating resource")

	createCon := ctrlutils.GenerateCreateConditionFunc(&rr)

	// ensure finalizer
	if controllerutil.AddFinalizer(ar, providerv1alpha1.AccessRequestFinalizer) {
		if err := r.platformCluster.Client().Patch(ctx, ar, client.MergeFrom(rr.OldObject)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error adding finalizer: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
			createCon(providerv1alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}
	}
	createCon(providerv1alpha1.ConditionMeta, metav1.ConditionTrue, "FinalizerEnsured", "")

	// skip if already granted, secret exists and is still valid
	if ar.Status.Phase == clustersv1alpha1.REQUEST_GRANTED && ar.Status.ObservedGeneration == ar.Generation && !enforceReconcile {
		if ar.Spec.Token != nil {
			s := &corev1.Secret{}
			if err := r.platformCluster.Client().Get(ctx, ctrlutils.ObjectKey(ar.Status.SecretRef.Name, ar.Namespace), s); err == nil {
				creation, _ := strconv.ParseInt(string(s.Data[clustersv1alpha1.SecretKeyCreationTimestamp]), 10, 64)
				expiration, _ := strconv.ParseInt(string(s.Data[clustersv1alpha1.SecretKeyExpirationTimestamp]), 10, 64)
				if creation != 0 && expiration != 0 {
					renewAt := time.Unix(creation, 0).Add(time.Duration(float64(time.Unix(expiration, 0).Sub(time.Unix(creation, 0))) * RenewTokenAfterValidityPercentagePassed))
					if time.Now().Before(renewAt) {
						log.Info("Token still valid, nothing to do")
						rr.Result.RequeueAfter = time.Until(renewAt)
						createCon(providerv1alpha1.AccessRequestConditionSecretExistsAndIsValid, metav1.ConditionTrue, "TokenValid", "")
						return rr
					}
				}
			}
		} else if ar.Spec.OIDC != nil {
			// OIDC kubeconfig does not expire
			createCon(providerv1alpha1.AccessRequestConditionSecretExistsAndIsValid, metav1.ConditionTrue, "OIDCValid", "")
			return rr
		}
	}

	// get a client for the GKE cluster
	gkeAccess, rerr := r.getGKEAccess(ctx, c, profile)
	if rerr != nil {
		rr.ReconcileError = rerr
		createCon(providerv1alpha1.AccessRequestConditionGKEAccess, metav1.ConditionFalse, rerr.Reason(), rerr.Error())
		return rr
	}
	log.Info("GKE access obtained", "host", gkeAccess.RESTConfig.Host, "tokenAudience", gkeAccess.TokenAudience)
	createCon(providerv1alpha1.AccessRequestConditionGKEAccess, metav1.ConditionTrue, "AccessObtained", "")

	var keepObjects []client.Object
	if ar.Spec.Token != nil {
		keepObjects, rr = r.ensureTokenAccess(ctx, ar, gkeAccess, rr)
	} else if ar.Spec.OIDC != nil {
		keepObjects, rr = r.ensureOIDCAccess(ctx, ar, gkeAccess, rr)
	}
	if rr.ReconcileError != nil {
		createCon(providerv1alpha1.AccessRequestConditionSecretExistsAndIsValid, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	createCon(providerv1alpha1.AccessRequestConditionSecretExistsAndIsValid, metav1.ConditionTrue, "SecretReady", "")

	// cleanup stale RBAC resources from previous versions of this request
	if rerr := r.cleanupResources(ctx, gkeAccess.Client, keepObjects, managedLabels(ar)); rerr != nil {
		rr.ReconcileError = rerr
		createCon(providerv1alpha1.AccessRequestConditionCleanup, metav1.ConditionFalse, rerr.Reason(), rerr.Error())
		return rr
	}
	createCon(providerv1alpha1.AccessRequestConditionCleanup, metav1.ConditionTrue, "CleanupDone", "")

	return rr
}

func (r *AccessRequestReconciler) handleDelete(ctx context.Context, req reconcile.Request, ar *clustersv1alpha1.AccessRequest, c *clustersv1alpha1.Cluster, profile *shared.Profile, rr ReconcileResult) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Deleting resource")
	createCon := ctrlutils.GenerateCreateConditionFunc(&rr)

	gkeAccess, rerr := r.getGKEAccess(ctx, c, profile)
	if rerr != nil {
		rr.ReconcileError = rerr
		createCon(providerv1alpha1.AccessRequestConditionGKEAccess, metav1.ConditionFalse, rerr.Reason(), rerr.Error())
		return rr
	}
	createCon(providerv1alpha1.AccessRequestConditionGKEAccess, metav1.ConditionTrue, "AccessObtained", "")

	if rerr := r.cleanupResources(ctx, gkeAccess.Client, nil, managedLabels(ar)); rerr != nil {
		rr.ReconcileError = rerr
		createCon(providerv1alpha1.AccessRequestConditionCleanup, metav1.ConditionFalse, rerr.Reason(), rerr.Error())
		return rr
	}
	createCon(providerv1alpha1.AccessRequestConditionCleanup, metav1.ConditionTrue, "CleanupDone", "")

	// remove finalizer (the kubeconfig secret is garbage-collected via owner reference)
	if controllerutil.RemoveFinalizer(ar, providerv1alpha1.AccessRequestFinalizer) {
		if err := r.platformCluster.Client().Patch(ctx, ar, client.MergeFrom(rr.OldObject)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error removing finalizer: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
			createCon(providerv1alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}
	}
	rr.Object = nil
	return rr
}

// ensureTokenAccess creates/renews a ServiceAccount, RBAC resources and a TokenRequest kubeconfig.
func (r *AccessRequestReconciler) ensureTokenAccess(ctx context.Context, ar *clustersv1alpha1.AccessRequest, gkeAccess *shared.GKEAccess, rr ReconcileResult) ([]client.Object, ReconcileResult) {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Ensuring token-based access")

	expectedLabels := pairs.MapToPairs(managedLabels(ar))
	keep := []client.Object{}
	saNamespace := shared.AccessRequestServiceAccountNamespace

	if _, err := clusteraccess.EnsureNamespace(ctx, gkeAccess.Client, saNamespace); err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error ensuring SA namespace '%s': %w", saNamespace, err), providerv1alpha1.ReasonGKEClusterInteractionProblem)
		return nil, rr
	}

	saName := ctrlutils.NameHashSHAKE128Base32(r.rc.Environment(), r.rc.ProviderName(), ar.Namespace, ar.Name)
	sa, err := clusteraccess.EnsureServiceAccount(ctx, gkeAccess.Client, saName, saNamespace, expectedLabels...)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error ensuring ServiceAccount '%s/%s': %w", saNamespace, saName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem)
		return nil, rr
	}
	log.Info("ServiceAccount ensured", "name", sa.Name, "namespace", sa.Namespace, "uid", sa.UID)
	if sa.GroupVersionKind().Kind == "" {
		sa.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ServiceAccount"))
	}
	keep = append(keep, sa)

	subjects := []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: sa.Name, Namespace: sa.Namespace}}
	errs := errutils.NewReasonableErrorList()

	for i, perm := range ar.Spec.Token.Permissions {
		roleName := perm.Name
		if roleName == "" {
			roleName = fmt.Sprintf("openmcp:permission:%s:%d", ctrlutils.NameHashSHAKE128Base32(r.rc.Environment(), r.rc.ProviderName(), ar.Namespace, ar.Name), i)
		}
		if perm.Namespace != "" {
			if !perm.DisableAutomaticNamespaceCreation {
				if _, err := clusteraccess.EnsureNamespace(ctx, gkeAccess.Client, perm.Namespace); err != nil {
					errs.Append(errutils.WithReason(fmt.Errorf("error ensuring namespace '%s': %w", perm.Namespace, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
					continue
				}
			}
			rb, role, err := clusteraccess.EnsureRoleAndBinding(ctx, gkeAccess.Client, roleName, perm.Namespace, subjects, perm.Rules, expectedLabels...)
			if err != nil {
				errs.Append(errutils.WithReason(fmt.Errorf("error ensuring Role/RoleBinding '%s/%s': %w", perm.Namespace, roleName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				continue
			}
			if rb.GroupVersionKind().Kind == "" {
				rb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindRoleBinding))
			}
			if role.GroupVersionKind().Kind == "" {
				role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindRole))
			}
			keep = append(keep, rb, role)
		} else {
			crb, cr, err := clusteraccess.EnsureClusterRoleAndBinding(ctx, gkeAccess.Client, roleName, subjects, perm.Rules, expectedLabels...)
			if err != nil {
				errs.Append(errutils.WithReason(fmt.Errorf("error ensuring ClusterRole/ClusterRoleBinding '%s': %w", roleName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				continue
			}
			if crb.GroupVersionKind().Kind == "" {
				crb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindClusterRoleBinding))
			}
			if cr.GroupVersionKind().Kind == "" {
				cr.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindClusterRole))
			}
			keep = append(keep, crb, cr)
		}
	}

	for i, roleRef := range ar.Spec.Token.RoleRefs {
		bindingName := fmt.Sprintf("openmcp:roleref:%s:%d", ctrlutils.NameHashSHAKE128Base32(r.rc.Environment(), r.rc.ProviderName(), ar.Namespace, ar.Name), i)
		if roleRef.Kind == KindRole {
			rb, err := clusteraccess.EnsureRoleBinding(ctx, gkeAccess.Client, bindingName, roleRef.Namespace, roleRef.Name, subjects, expectedLabels...)
			if err != nil {
				errs.Append(errutils.WithReason(fmt.Errorf("error ensuring RoleBinding '%s/%s': %w", roleRef.Namespace, bindingName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				continue
			}
			if rb.GroupVersionKind().Kind == "" {
				rb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindRoleBinding))
			}
			keep = append(keep, rb)
		} else {
			crb, err := clusteraccess.EnsureClusterRoleBinding(ctx, gkeAccess.Client, bindingName, roleRef.Name, subjects, expectedLabels...)
			if err != nil {
				errs.Append(errutils.WithReason(fmt.Errorf("error ensuring ClusterRoleBinding '%s': %w", bindingName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				continue
			}
			if crb.GroupVersionKind().Kind == "" {
				crb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindClusterRoleBinding))
			}
			keep = append(keep, crb)
		}
	}

	if err := errs.Aggregate(); err != nil {
		rr.ReconcileError = err
		return nil, rr
	}

	token, err := createTokenForServiceAccountWithAudience(ctx, gkeAccess.Client, sa, &DefaultTokenValidity, gkeAccess.TokenAudience)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error creating token for ServiceAccount '%s/%s': %w", saNamespace, saName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem)
		return nil, rr
	}
	rr.Result.RequeueAfter = time.Until(clusteraccess.ComputeTokenRenewalTimeWithRatio(token.CreationTimestamp, token.ExpirationTimestamp, RenewTokenAfterValidityPercentagePassed))

	kcfg, err := clusteraccess.CreateTokenKubeconfig(r.rc.ProviderName(), gkeAccess.RESTConfig.Host, gkeAccess.RESTConfig.CAData, token.Token)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error building kubeconfig: %w", err), providerv1alpha1.ReasonInternalError)
		return nil, rr
	}

	secretName := defaultSecretName(ar)
	sm := resources.NewSecretMutator(secretName, ar.Namespace, map[string][]byte{
		clustersv1alpha1.SecretKeyKubeconfig:          kcfg,
		clustersv1alpha1.SecretKeyExpirationTimestamp: []byte(strconv.FormatInt(token.ExpirationTimestamp.Unix(), 10)),
		clustersv1alpha1.SecretKeyCreationTimestamp:   []byte(strconv.FormatInt(token.CreationTimestamp.Unix(), 10)),
	}, corev1.SecretTypeOpaque)
	sm.MetadataMutator().WithOwnerReferences([]metav1.OwnerReference{{
		APIVersion: clustersv1alpha1.GroupVersion.String(),
		Kind:       "AccessRequest",
		Name:       ar.Name,
		UID:        ar.UID,
		Controller: ptr.To(true),
	}})
	if err := resources.CreateOrUpdateResource(ctx, r.platformCluster.Client(), sm); err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error creating/updating kubeconfig secret: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
		return nil, rr
	}
	rr.Object.Status.SecretRef = &commonapi.LocalObjectReference{Name: secretName}
	rr.Object.Status.Phase = clustersv1alpha1.REQUEST_GRANTED

	return keep, rr
}

// ensureOIDCAccess builds and stores an OIDC kubeconfig and the requested RBAC resources.
func (r *AccessRequestReconciler) ensureOIDCAccess(ctx context.Context, ar *clustersv1alpha1.AccessRequest, gkeAccess *shared.GKEAccess, rr ReconcileResult) ([]client.Object, ReconcileResult) {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Ensuring OIDC access")

	oidcCfg := ar.Spec.OIDC
	oidcCfg.OIDCProviderConfig.Default()
	expectedLabels := pairs.MapToPairs(managedLabels(ar))
	keep := []client.Object{}
	errs := errutils.NewReasonableErrorList()

	for i, roleDef := range oidcCfg.Roles {
		roleName := roleDef.Name
		if roleName == "" {
			roleName = fmt.Sprintf("openmcp:%s:%d", ctrlutils.NameHashSHAKE128Base32(r.rc.Environment(), r.rc.ProviderName(), ar.Namespace, ar.Name), i)
		}
		if roleDef.Namespace != "" {
			if !roleDef.DisableAutomaticNamespaceCreation {
				if _, err := clusteraccess.EnsureNamespace(ctx, gkeAccess.Client, roleDef.Namespace); err != nil {
					errs.Append(errutils.WithReason(fmt.Errorf("error ensuring namespace '%s': %w", roleDef.Namespace, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
					continue
				}
			}
			role, err := clusteraccess.EnsureRole(ctx, gkeAccess.Client, roleName, roleDef.Namespace, roleDef.Rules, expectedLabels...)
			if err != nil {
				errs.Append(errutils.WithReason(fmt.Errorf("error ensuring Role '%s/%s': %w", roleDef.Namespace, roleName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				continue
			}
			if role.GroupVersionKind().Kind == "" {
				role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindRole))
			}
			keep = append(keep, role)
		} else {
			cr, err := clusteraccess.EnsureClusterRole(ctx, gkeAccess.Client, roleName, roleDef.Rules, expectedLabels...)
			if err != nil {
				errs.Append(errutils.WithReason(fmt.Errorf("error ensuring ClusterRole '%s': %w", roleName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				continue
			}
			if cr.GroupVersionKind().Kind == "" {
				cr.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindClusterRole))
			}
			keep = append(keep, cr)
		}
	}

	for i, rb := range oidcCfg.RoleBindings {
		subjects := buildSubjectsWithPrefixes(rb.Subjects, &oidcCfg.OIDCProviderConfig)
		for j, roleRef := range rb.RoleRefs {
			bindingName := fmt.Sprintf("openmcp:%s:%d:%d", ctrlutils.NameHashSHAKE128Base32(r.rc.Environment(), r.rc.ProviderName(), ar.Namespace, ar.Name), i, j)
			if roleRef.Kind == KindRole {
				obj, err := clusteraccess.EnsureRoleBinding(ctx, gkeAccess.Client, bindingName, roleRef.Namespace, roleRef.Name, subjects, expectedLabels...)
				if err != nil {
					errs.Append(errutils.WithReason(fmt.Errorf("error ensuring RoleBinding '%s': %w", bindingName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
					continue
				}
				if obj.GroupVersionKind().Kind == "" {
					obj.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindRoleBinding))
				}
				keep = append(keep, obj)
			} else {
				obj, err := clusteraccess.EnsureClusterRoleBinding(ctx, gkeAccess.Client, bindingName, roleRef.Name, subjects, expectedLabels...)
				if err != nil {
					errs.Append(errutils.WithReason(fmt.Errorf("error ensuring ClusterRoleBinding '%s': %w", bindingName, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
					continue
				}
				if obj.GroupVersionKind().Kind == "" {
					obj.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(KindClusterRoleBinding))
				}
				keep = append(keep, obj)
			}
		}
	}
	if err := errs.Aggregate(); err != nil {
		rr.ReconcileError = err
		return nil, rr
	}

	kcfgOptions := []clusteraccess.CreateOIDCKubeconfigOption{
		clusteraccess.WithPKCEMethod(clusteraccess.PKCEMethodAuto),
		clusteraccess.WithClusterName(fmt.Sprintf("%s--%s", ar.Spec.ClusterRef.Namespace, ar.Spec.ClusterRef.Name)),
		clusteraccess.WithContextName(fmt.Sprintf("%s--%s--%s", ar.Spec.ClusterRef.Namespace, ar.Spec.ClusterRef.Name, oidcCfg.Name)),
	}
	for _, scope := range oidcCfg.ExtraScopes {
		kcfgOptions = append(kcfgOptions, clusteraccess.WithExtraScope(scope))
	}
	kcfg, err := clusteraccess.CreateOIDCKubeconfig(oidcCfg.Name, gkeAccess.RESTConfig.Host, gkeAccess.RESTConfig.CAData, oidcCfg.Issuer, oidcCfg.ClientID, kcfgOptions...)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error building OIDC kubeconfig: %w", err), providerv1alpha1.ReasonInternalError)
		return nil, rr
	}

	secretName := defaultSecretName(ar)
	sm := resources.NewSecretMutatorWithStringData(secretName, ar.Namespace, map[string]string{
		clustersv1alpha1.SecretKeyKubeconfig: string(kcfg),
	}, corev1.SecretTypeOpaque)
	sm.MetadataMutator().WithOwnerReferences([]metav1.OwnerReference{{
		APIVersion: clustersv1alpha1.GroupVersion.String(),
		Kind:       "AccessRequest",
		Name:       ar.Name,
		UID:        ar.UID,
		Controller: ptr.To(true),
	}})
	if err := resources.CreateOrUpdateResource(ctx, r.platformCluster.Client(), sm); err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error creating/updating kubeconfig secret: %w", err), clusterconst.ReasonPlatformClusterInteractionProblem)
		return nil, rr
	}
	rr.Object.Status.SecretRef = &commonapi.LocalObjectReference{Name: secretName}
	rr.Object.Status.Phase = clustersv1alpha1.REQUEST_GRANTED

	return keep, rr
}

func (r *AccessRequestReconciler) cleanupResources(ctx context.Context, gkeClient client.Client, keep []client.Object, labels map[string]string) errutils.ReasonableError {
	selector := client.MatchingLabels(labels)
	errs := errutils.NewReasonableErrorList()

	for _, listObj := range []client.ObjectList{
		&rbacv1.ClusterRoleBindingList{},
		&rbacv1.ClusterRoleList{},
		&rbacv1.RoleBindingList{},
		&rbacv1.RoleList{},
		&corev1.ServiceAccountList{},
	} {
		if err := gkeClient.List(ctx, listObj, selector); err != nil {
			errs.Append(errutils.WithReason(fmt.Errorf("error listing resources: %w", err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
			continue
		}
		deleteStale(ctx, gkeClient, listObj, keep, errs)
	}
	return errs.Aggregate()
}

func deleteStale(ctx context.Context, c client.Client, list client.ObjectList, keep []client.Object, errs *errutils.ReasonableErrorList) {
	switch l := list.(type) {
	case *rbacv1.ClusterRoleBindingList:
		for i := range l.Items {
			if !shouldKeep(&l.Items[i], keep) {
				if err := c.Delete(ctx, &l.Items[i]); err != nil && !apierrors.IsNotFound(err) {
					errs.Append(errutils.WithReason(fmt.Errorf("error deleting ClusterRoleBinding '%s': %w", l.Items[i].Name, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				}
			}
		}
	case *rbacv1.ClusterRoleList:
		for i := range l.Items {
			if !shouldKeep(&l.Items[i], keep) {
				if err := c.Delete(ctx, &l.Items[i]); err != nil && !apierrors.IsNotFound(err) {
					errs.Append(errutils.WithReason(fmt.Errorf("error deleting ClusterRole '%s': %w", l.Items[i].Name, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				}
			}
		}
	case *rbacv1.RoleBindingList:
		for i := range l.Items {
			if !shouldKeep(&l.Items[i], keep) {
				if err := c.Delete(ctx, &l.Items[i]); err != nil && !apierrors.IsNotFound(err) {
					errs.Append(errutils.WithReason(fmt.Errorf("error deleting RoleBinding '%s/%s': %w", l.Items[i].Namespace, l.Items[i].Name, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				}
			}
		}
	case *rbacv1.RoleList:
		for i := range l.Items {
			if !shouldKeep(&l.Items[i], keep) {
				if err := c.Delete(ctx, &l.Items[i]); err != nil && !apierrors.IsNotFound(err) {
					errs.Append(errutils.WithReason(fmt.Errorf("error deleting Role '%s/%s': %w", l.Items[i].Namespace, l.Items[i].Name, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				}
			}
		}
	case *corev1.ServiceAccountList:
		for i := range l.Items {
			if !shouldKeep(&l.Items[i], keep) {
				if err := c.Delete(ctx, &l.Items[i]); err != nil && !apierrors.IsNotFound(err) {
					errs.Append(errutils.WithReason(fmt.Errorf("error deleting ServiceAccount '%s/%s': %w", l.Items[i].Namespace, l.Items[i].Name, err), providerv1alpha1.ReasonGKEClusterInteractionProblem))
				}
			}
		}
	}
}

func shouldKeep(obj client.Object, keep []client.Object) bool {
	objKind := obj.GetObjectKind().GroupVersionKind().Kind
	for _, k := range keep {
		if k.GetName() != obj.GetName() || k.GetNamespace() != obj.GetNamespace() {
			continue
		}
		// controller-runtime does not populate GVK on items returned by List,
		// so skip the kind check if either side has an empty kind.
		keepKind := k.GetObjectKind().GroupVersionKind().Kind
		if keepKind == "" || objKind == "" || keepKind == objKind {
			return true
		}
	}
	return false
}

func (r *AccessRequestReconciler) resolveClusterAndProfile(ctx context.Context, ar *clustersv1alpha1.AccessRequest) (*clustersv1alpha1.Cluster, *shared.Profile, errutils.ReasonableError) {
	if ar.Spec.ClusterRef == nil {
		return nil, nil, errutils.WithReason(fmt.Errorf("spec.clusterRef is not set"), providerv1alpha1.ReasonConfigurationProblem)
	}
	c := &clustersv1alpha1.Cluster{}
	if err := r.platformCluster.Client().Get(ctx, client.ObjectKey{Name: ar.Spec.ClusterRef.Name, Namespace: ar.Spec.ClusterRef.Namespace}, c); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, errutils.WithReason(fmt.Errorf("cluster '%s/%s' not found", ar.Spec.ClusterRef.Namespace, ar.Spec.ClusterRef.Name), clusterconst.ReasonInvalidReference)
		}
		return nil, nil, errutils.WithReason(fmt.Errorf("error getting Cluster '%s/%s': %w", ar.Spec.ClusterRef.Namespace, ar.Spec.ClusterRef.Name, err), clusterconst.ReasonPlatformClusterInteractionProblem)
	}
	profile := r.rc.GetProfile(c.Spec.Profile)
	if profile == nil {
		return nil, nil, errutils.WithReason(fmt.Errorf("unknown profile '%s'", c.Spec.Profile), providerv1alpha1.ReasonUnknownProfile)
	}
	return c, profile, nil
}

func (r *AccessRequestReconciler) getGKEAccess(ctx context.Context, c *clustersv1alpha1.Cluster, profile *shared.Profile) (*shared.GKEAccess, errutils.ReasonableError) {
	pc := profile.ProviderConfig
	gkeClusterName := shared.GKEClusterName(c.Namespace, c.Name)
	clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", pc.Spec.ProjectID, pc.Spec.Region, gkeClusterName)

	access, err := shared.GetGKEAccess(ctx, profile.Credentials, clusterPath)
	if err != nil {
		return nil, errutils.WithReason(fmt.Errorf("error getting GKE cluster access for '%s': %w", clusterPath, err), providerv1alpha1.ReasonGKEClusterInteractionProblem)
	}
	return access, nil
}

func managedLabels(ar *clustersv1alpha1.AccessRequest) map[string]string {
	return map[string]string{
		providerv1alpha1.ManagedByNameLabel:      ar.Name,
		providerv1alpha1.ManagedByNamespaceLabel: ar.Namespace,
	}
}

func defaultSecretName(ar *clustersv1alpha1.AccessRequest) string {
	const suffix = ".kubeconfig"
	return ctrlutils.ShortenToXCharactersUnsafe(ar.Name, ctrlutils.K8sMaxNameLength-len(suffix)) + suffix
}

func buildSubjectsWithPrefixes(subjects []rbacv1.Subject, cfg *commonapi.OIDCProviderConfig) []rbacv1.Subject {
	result := make([]rbacv1.Subject, len(subjects))
	for i, s := range subjects {
		result[i] = s
		switch s.Kind {
		case rbacv1.UserKind:
			result[i].Name = cfg.GetUsernamePrefix() + s.Name
		case rbacv1.GroupKind:
			result[i].Name = cfg.GetGroupsPrefix() + s.Name
		}
	}
	return result
}

// createTokenForServiceAccountWithAudience requests a token with an explicit audience.
// On GKE, the default audience is the cluster's container.googleapis.com resource path,
// which the API server rejects. Setting the audience to the API server URL fixes this.
func createTokenForServiceAccountWithAudience(ctx context.Context, c client.Client, sa *corev1.ServiceAccount, desiredDuration *time.Duration, audience string) (*clusteraccess.ServiceAccountToken, error) {
	tr := &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			Audiences: []string{audience},
		},
	}
	if desiredDuration != nil {
		tr.Spec.ExpirationSeconds = ptr.To(int64(desiredDuration.Seconds()))
	}
	if err := c.SubResource("token").Create(ctx, sa, tr); err != nil {
		return nil, fmt.Errorf("error creating token for ServiceAccount '%s/%s': %w", sa.Namespace, sa.Name, err)
	}
	return &clusteraccess.ServiceAccountToken{
		Token:               tr.Status.Token,
		CreationTimestamp:   time.Now(),
		ExpirationTimestamp: tr.Status.ExpirationTimestamp.Time,
	}, nil
}

func (r *AccessRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clustersv1alpha1.AccessRequest{}, builder.WithPredicates(
			predicate.And(
				predicate.NewPredicateFuncs(func(obj client.Object) bool {
					ar, ok := obj.(*clustersv1alpha1.AccessRequest)
					if !ok {
						return false
					}
					return libutils.IsClusterProviderResponsibleForAccessRequest(ar, r.rc.ProviderName())
				}),
				predicate.Or(
					ctrlutils.DeletionTimestampChangedPredicate{},
					libutils.AccessRequestPhasePredicateUntyped(clustersv1alpha1.REQUEST_PENDING),
					ctrlutils.GotAnnotationPredicate(openmcpconst.OperationAnnotation, openmcpconst.OperationAnnotationValueReconcile),
					ctrlutils.LostAnnotationPredicate(openmcpconst.OperationAnnotation, openmcpconst.OperationAnnotationValueIgnore),
					ctrlutils.GotLabelPredicate(clustersv1alpha1.ProviderLabel, r.rc.ProviderName()),
					ctrlutils.GotLabelPredicate(clustersv1alpha1.ProfileLabel, ""),
				),
				predicate.Not(
					ctrlutils.HasAnnotationPredicate(openmcpconst.OperationAnnotation, openmcpconst.OperationAnnotationValueIgnore),
				),
			),
		)).
		Owns(&corev1.Secret{}).
		Complete(r)
}
