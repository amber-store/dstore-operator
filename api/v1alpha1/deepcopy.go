package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto copies the receiver into out.
func (in *StorageSpec) DeepCopyInto(out *StorageSpec) {
	*out = *in
	in.VolumeClaimTemplate.DeepCopyInto(&out.VolumeClaimTemplate)
	if in.PaxosVolumeClaimTemplate != nil {
		out.PaxosVolumeClaimTemplate = new(corev1.PersistentVolumeClaimSpec)
		in.PaxosVolumeClaimTemplate.DeepCopyInto(out.PaxosVolumeClaimTemplate)
	}
}

// DeepCopyInto copies the receiver into out.
func (in *DstoreClusterSpec) DeepCopyInto(out *DstoreClusterSpec) {
	*out = *in
	if in.ReplicationFactor != nil {
		v := *in.ReplicationFactor
		out.ReplicationFactor = &v
	}
	if in.MinReplicationFactor != nil {
		v := *in.MinReplicationFactor
		out.MinReplicationFactor = &v
	}
	in.Storage.DeepCopyInto(&out.Storage)
	if in.Zones != nil {
		out.Zones = append([]string{}, in.Zones...)
	}
	in.Resources.DeepCopyInto(&out.Resources)
	if in.NodeSelector != nil {
		out.NodeSelector = make(map[string]string, len(in.NodeSelector))
		for k, v := range in.NodeSelector {
			out.NodeSelector[k] = v
		}
	}
	if in.Tolerations != nil {
		out.Tolerations = make([]corev1.Toleration, len(in.Tolerations))
		for i := range in.Tolerations {
			in.Tolerations[i].DeepCopyInto(&out.Tolerations[i])
		}
	}
	if in.Affinity != nil {
		out.Affinity = in.Affinity.DeepCopy()
	}
	if in.ExtraArgs != nil {
		out.ExtraArgs = append([]string{}, in.ExtraArgs...)
	}
	if in.Env != nil {
		out.Env = make([]corev1.EnvVar, len(in.Env))
		for i := range in.Env {
			in.Env[i].DeepCopyInto(&out.Env[i])
		}
	}
}

// DeepCopyInto copies the receiver into out.
func (in *DstoreClusterStatus) DeepCopyInto(out *DstoreClusterStatus) {
	*out = *in
	if in.Nodes != nil {
		out.Nodes = append([]NodeStatus{}, in.Nodes...)
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

// DeepCopy returns a deep copy.
func (in *DstoreClusterStatus) DeepCopy() *DstoreClusterStatus {
	if in == nil {
		return nil
	}
	out := new(DstoreClusterStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *DstoreCluster) DeepCopyInto(out *DstoreCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy returns a deep copy.
func (in *DstoreCluster) DeepCopy() *DstoreCluster {
	if in == nil {
		return nil
	}
	out := new(DstoreCluster)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *DstoreCluster) DeepCopyObject() runtime.Object { return in.DeepCopy() }

// DeepCopyInto copies the receiver into out.
func (in *DstoreClusterList) DeepCopyInto(out *DstoreClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]DstoreCluster, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

// DeepCopy returns a deep copy.
func (in *DstoreClusterList) DeepCopy() *DstoreClusterList {
	if in == nil {
		return nil
	}
	out := new(DstoreClusterList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *DstoreClusterList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
