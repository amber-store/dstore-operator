package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	dstorev1 "github.com/amber-store/dstore-operator/api/v1alpha1"
)

// Labels and keys.
const (
	LabelCluster = "dstore.amber-store.io/cluster"
	LabelNode    = "dstore.amber-store.io/node"
	LabelApp     = "app.kubernetes.io/name"
	AppName      = "dstore"
	// AnnotationSpecHash records, on a node's Deployment, the hash of the
	// spec the operator last wrote; a different desired hash means a
	// rollout.
	AnnotationSpecHash = "dstore.amber-store.io/spec-hash"

	SecretKeyIdentity = "identity"
	SecretKeyToken    = "token"
	SecretKeySeed     = "seed"

	storeMount = "/data"
)

// Node roles the entrypoint understands.
const (
	RoleInit = "init"
	RoleJoin = "join"
)

// nodeRole is the role node i starts with on a fresh store: node 0
// creates the cluster, every other node joins it. A store that is
// already a member ignores the role and just serves.
func nodeRole(i int32) string {
	if i == 0 {
		return RoleInit
	}
	return RoleJoin
}

func nodeName(c *dstorev1.DstoreCluster, i int32) string {
	return fmt.Sprintf("%s-node-%d", c.Name, i)
}

func identitySecretName(c *dstorev1.DstoreCluster, i int32) string {
	return nodeName(c, i) + "-identity"
}

func joinSecretName(c *dstorev1.DstoreCluster, i int32) string {
	return nodeName(c, i) + "-join"
}

func pvcName(c *dstorev1.DstoreCluster, i int32) string { return nodeName(c, i) + "-store" }

func nodeLabels(c *dstorev1.DstoreCluster, i int32) map[string]string {
	return map[string]string{LabelApp: AppName, LabelCluster: c.Name, LabelNode: strconv.Itoa(int(i))}
}

// weightGiB returns a node's placement weight.
func weightGiB(c *dstorev1.DstoreCluster) int32 {
	if c.Spec.CapacityGiB > 0 {
		return c.Spec.CapacityGiB
	}
	if q, ok := c.Spec.Storage.VolumeClaimTemplate.Resources.Requests[corev1.ResourceStorage]; ok {
		gib := q.Value() >> 30
		if gib < 1 {
			gib = 1
		}
		return int32(gib)
	}
	return 1
}

func desiredPVC(c *dstorev1.DstoreCluster, i int32, name string, tmpl corev1.PersistentVolumeClaimSpec) *corev1.PersistentVolumeClaim {
	spec := *tmpl.DeepCopy()
	if len(spec.AccessModes) == 0 {
		spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	if spec.Resources.Requests == nil {
		spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: nodeLabels(c, i)},
		Spec:       spec,
	}
}

func desiredService(c *dstorev1.DstoreCluster, i int32) *corev1.Service {
	port := c.Spec.PortOrDefault()
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName(c, i), Namespace: c.Namespace, Labels: nodeLabels(c, i)},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: nodeLabels(c, i),
			Ports:    []corev1.ServicePort{{Name: "iroh", Protocol: corev1.ProtocolUDP, Port: port, TargetPort: intstr.FromInt32(port)}},
		},
	}
}

// desiredDeployment renders node i's Deployment. clusterIP is the node's
// Service address; with the host network the node advertises its host's
// IP instead, taken from the pod's status. role selects what the
// entrypoint does on a fresh store.
func desiredDeployment(c *dstorev1.DstoreCluster, i int32, clusterIP string, role string) *appsv1.Deployment {
	labels := nodeLabels(c, i)
	port := c.Spec.PortOrDefault()
	hostNet := c.Spec.HostNetworkOrDefault()
	one := int32(1)
	minR := int32(0)
	if c.Spec.MinReplicationFactor != nil {
		minR = *c.Spec.MinReplicationFactor
	}
	env := []corev1.EnvVar{
		{Name: "DSTORE_STORE", Value: storeMount},
		{Name: "DSTORE_PORT", Value: strconv.Itoa(int(port))},
	}
	if hostNet {
		env = append(env,
			corev1.EnvVar{Name: "DSTORE_HOST_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"}}},
			corev1.EnvVar{Name: "DSTORE_ADVERTISE", Value: fmt.Sprintf("$(DSTORE_HOST_IP):%d", port)},
		)
	} else {
		env = append(env, corev1.EnvVar{Name: "DSTORE_ADVERTISE", Value: fmt.Sprintf("%s:%d", clusterIP, port)})
	}
	env = append(env,
		corev1.EnvVar{Name: "DSTORE_ROLE", Value: role},
		corev1.EnvVar{Name: "DSTORE_WEIGHT", Value: strconv.Itoa(int(weightGiB(c)))},
		corev1.EnvVar{Name: "DSTORE_REPLICAS", Value: strconv.Itoa(int(c.Spec.ReplicationFactorOrDefault()))},
		corev1.EnvVar{Name: "DSTORE_MIN_REPLICAS", Value: strconv.Itoa(int(minR))},
		corev1.EnvVar{Name: "DSTORE_IDENTITY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: identitySecretName(c, i)}, Key: SecretKeyIdentity}}},
	)
	if len(c.Spec.Zones) > 0 {
		env = append(env, corev1.EnvVar{Name: "DSTORE_ZONE", Value: c.Spec.Zones[int(i)%len(c.Spec.Zones)]})
	}
	if c.Spec.GCInterval != "" {
		env = append(env, corev1.EnvVar{Name: "DSTORE_GC_INTERVAL", Value: c.Spec.GCInterval})
	}
	var args []string
	if hostNet {
		// Bind the advertised address rather than the wildcard. Bound to
		// 0.0.0.0 on the host network, the kernel picks the source address
		// of each reply by route, so a client on the same host (reached
		// over the CNI bridge) gets replies from the bridge's address and
		// QUIC path validation fails. The user's extra args come after, so
		// a --bind of their own still wins.
		args = append(args, "--bind", fmt.Sprintf("$(DSTORE_HOST_IP):%d", port))
	}
	args = append(args, c.Spec.ExtraArgs...)
	if len(args) > 0 {
		env = append(env, corev1.EnvVar{Name: "DSTORE_EXTRA_ARGS", Value: joinArgs(args)})
	}
	if role == RoleJoin {
		// The join secret is deleted once the node has joined; the
		// references are optional so that the member's pod can restart.
		optional := true
		env = append(env,
			corev1.EnvVar{Name: "DSTORE_SEED", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: joinSecretName(c, i)}, Key: SecretKeySeed, Optional: &optional}}},
			corev1.EnvVar{Name: "DSTORE_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: joinSecretName(c, i)}, Key: SecretKeyToken, Optional: &optional}}},
		)
	}
	env = append(env, c.Spec.Env...)

	volumes := []corev1.Volume{{Name: "store", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName(c, i)}}}}
	mounts := []corev1.VolumeMount{{Name: "store", MountPath: storeMount}}
	containerPort := corev1.ContainerPort{Name: "iroh", ContainerPort: port, Protocol: corev1.ProtocolUDP}
	podSpec := corev1.PodSpec{
		NodeSelector: c.Spec.NodeSelector,
		Tolerations:  c.Spec.Tolerations,
		Affinity:     c.Spec.Affinity,
		Volumes:      volumes,
	}
	if hostNet {
		// The port is bound on the host; declaring it as a hostPort makes
		// the scheduler keep two nodes of a cluster off the same host.
		containerPort.HostPort = port
		podSpec.HostNetwork = true
		podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
	podSpec.Containers = []corev1.Container{{
		Name:            "dstore",
		Image:           c.Spec.Image,
		ImagePullPolicy: c.Spec.ImagePullPolicy,
		Env:             env,
		Ports:           []corev1.ContainerPort{containerPort},
		VolumeMounts:    mounts,
		Resources:       c.Spec.Resources,
	}}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName(c, i), Namespace: c.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       podSpec,
			},
		},
	}
}

// specHash hashes the Deployment spec the operator wants, so that an
// unchanged spec costs no write and a changed one is a rollout.
func specHash(d *appsv1.Deployment) string {
	b, err := json.Marshal(d.Spec)
	if err != nil {
		panic(err) // a Deployment spec always marshals
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func joinArgs(args []string) string {
	out := ""
	for i, a := range args {
		if i > 0 {
			out += " "
		}
		out += a
	}
	return out
}
