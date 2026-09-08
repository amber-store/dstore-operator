package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageSpec describes the volumes of a node.
type StorageSpec struct {
	// VolumeClaimTemplate is the claim created for each node's store
	// (packstore, meta, identity). Required.
	VolumeClaimTemplate corev1.PersistentVolumeClaimSpec `json:"volumeClaimTemplate"`
	// PaxosVolumeClaimTemplate, when set, gives each node a separate
	// claim for its acceptor state, as the design recommends.
	PaxosVolumeClaimTemplate *corev1.PersistentVolumeClaimSpec `json:"paxosVolumeClaimTemplate,omitempty"`
	// DeleteVolumesOnScaleDown removes a removed node's claims once the
	// cluster has moved its data. Default false: claims are kept.
	DeleteVolumesOnScaleDown bool `json:"deleteVolumesOnScaleDown,omitempty"`
}

// DstoreClusterSpec is the desired state of a cluster.
type DstoreClusterSpec struct {
	// Nodes is the number of dstore nodes. Minimum 1.
	Nodes int32 `json:"nodes"`
	// CapacityGiB is each node's placement weight in GiB. 0 derives it
	// from the volume claim template's storage request.
	CapacityGiB int32 `json:"capacityGiB,omitempty"`
	// Replicas is R, the owners per object. Default 3.
	Replicas *int32 `json:"replicas,omitempty"`
	// MinReplicas is the owners that must hold an object before a write
	// succeeds. Default max(R-1, 2).
	MinReplicas *int32 `json:"minReplicas,omitempty"`
	// Image is the dstore node image.
	Image string `json:"image"`
	// ImagePullPolicy of the node containers.
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// Port is the UDP port every node binds and advertises. Default 4433.
	Port int32 `json:"port,omitempty"`
	// Storage describes the nodes' volumes.
	Storage StorageSpec `json:"storage"`
	// Zones, when set, assigns node i the failure domain zones[i % len].
	Zones []string `json:"zones,omitempty"`
	// Resources of the node container.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// NodeSelector, Tolerations and Affinity are copied into every pod.
	NodeSelector map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity    `json:"affinity,omitempty"`
	// GCInterval is passed to the nodes (a Go duration, e.g. "4h").
	GCInterval string `json:"gcInterval,omitempty"`
	// ExtraArgs are appended to every dstore command.
	ExtraArgs []string `json:"extraArgs,omitempty"`
	// Env is added to the node container.
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// Node phases.
const (
	NodePending  = "Pending"
	NodeJoining  = "Joining"
	NodeMember   = "Member"
	NodeRemoving = "Removing"
)

// NodeStatus is the state of one node.
type NodeStatus struct {
	Index   int32  `json:"index"`
	ID      string `json:"id,omitempty"`
	Address string `json:"address,omitempty"`
	Phase   string `json:"phase"`
	Voter   bool   `json:"voter,omitempty"`
}

// Cluster phases.
const (
	PhaseBootstrapping = "Bootstrapping"
	PhaseJoining       = "Joining"
	PhaseReady         = "Ready"
	PhaseScalingDown   = "ScalingDown"
)

// DstoreClusterStatus is the observed state.
type DstoreClusterStatus struct {
	Phase              string             `json:"phase,omitempty"`
	Ticket             string             `json:"ticket,omitempty"`
	Epoch              uint64             `json:"epoch,omitempty"`
	Members            int32              `json:"members,omitempty"`
	Voters             int32              `json:"voters,omitempty"`
	Transition         string             `json:"transition,omitempty"`
	Nodes              []NodeStatus       `json:"nodes,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// DstoreCluster is a dstore cluster.
type DstoreCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DstoreClusterSpec   `json:"spec,omitempty"`
	Status DstoreClusterStatus `json:"status,omitempty"`
}

// DstoreClusterList is a list of clusters.
type DstoreClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DstoreCluster `json:"items"`
}

// ReplicasOrDefault returns R.
func (s *DstoreClusterSpec) ReplicasOrDefault() int32 {
	if s.Replicas != nil && *s.Replicas > 0 {
		return *s.Replicas
	}
	return 3
}

// PortOrDefault returns the node port.
func (s *DstoreClusterSpec) PortOrDefault() int32 {
	if s.Port > 0 {
		return s.Port
	}
	return 4433
}
