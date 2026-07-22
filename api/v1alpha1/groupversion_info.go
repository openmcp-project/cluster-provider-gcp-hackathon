// +kubebuilder:object:generate=true
// +groupName=gcp.cluster.open-control-plane.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	GroupVersion = schema.GroupVersion{Group: "gcp.cluster.open-control-plane.io", Version: "v1alpha1"}

	SchemeBuilder = runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
		metav1.AddToGroupVersion(s, GroupVersion)
		return nil
	})

	AddToScheme = SchemeBuilder.AddToScheme
)
