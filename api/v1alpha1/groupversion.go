// Package v1alpha1 defines the DstoreCluster API: a dstore cluster run as
// one Deployment per node, each with its own volumes, joined into one
// cluster by the operator.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the API group and version of this package.
var GroupVersion = schema.GroupVersion{Group: "dstore.amber-store.io", Version: "v1alpha1"}

// AddToScheme registers the types with a scheme.
func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &DstoreCluster{}, &DstoreClusterList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
