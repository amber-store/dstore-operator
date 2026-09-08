package controllers

import (
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

	SecretKeyIdentity = "identity"
	SecretKeyToken    = "token"
	SecretKeySeed     = "seed"

	storeMount = "/data"
	paxosMount = "/paxos"
)

// Node roles the entrypoint understands.
const (
	RoleInit = "init"
	RoleJoin = "join"
)

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

func paxosPVCName(c *dstorev1.DstoreCluster, i int32) string { return nodeName(c, i) + "-paxos" }

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

// desiredDeployment renders node i's Deployment. advertise is the address
// peers dial (the node's Service ClusterIP); role selects what the
// entrypoint does on a fresh store.
func desiredDeployment(c *dstorev1.DstoreCluster, i int32, advertise string, role string) *appsv1.Deployment {
	labels := nodeLabels(c, i)
	port := c.Spec.PortOrDefault()
	one := int32(1)
	minR := int32(0)
	if c.Spec.MinReplicationFactor != nil {
		minR = *c.Spec.MinReplicationFactor
	}
	env := []corev1.EnvVar{
		{Name: "DSTORE_STORE", Value: storeMount},
		{Name: "DSTORE_PORT", Value: strconv.Itoa(int(port))},
		{Name: "DSTORE_ADVERTISE", Value: fmt.Sprintf("%s:%d", advertise, port)},
		{Name: "DSTORE_ROLE", Value: role},
		{Name: "DSTORE_WEIGHT", Value: strconv.Itoa(int(weightGiB(c)))},
		{Name: "DSTORE_REPLICAS", Value: strconv.Itoa(int(c.Spec.ReplicationFactorOrDefault()))},
		{Name: "DSTORE_MIN_REPLICAS", Value: strconv.Itoa(int(minR))},
		{Name: "DSTORE_IDENTITY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: identitySecretName(c, i)}, Key: SecretKeyIdentity}}},
	}
	if len(c.Spec.Zones) > 0 {
		env = append(env, corev1.EnvVar{Name: "DSTORE_ZONE", Value: c.Spec.Zones[int(i)%len(c.Spec.Zones)]})
	}
	if c.Spec.GCInterval != "" {
		env = append(env, corev1.EnvVar{Name: "DSTORE_GC_INTERVAL", Value: c.Spec.GCInterval})
	}
	if len(c.Spec.ExtraArgs) > 0 {
		env = append(env, corev1.EnvVar{Name: "DSTORE_EXTRA_ARGS", Value: joinArgs(c.Spec.ExtraArgs)})
	}
	if role == RoleJoin {
		optional := false
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
	if c.Spec.Storage.PaxosVolumeClaimTemplate != nil {
		volumes = append(volumes, corev1.Volume{Name: "paxos", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: paxosPVCName(c, i)}}})
		mounts = append(mounts, corev1.VolumeMount{Name: "paxos", MountPath: paxosMount})
		env = append(env, corev1.EnvVar{Name: "DSTORE_PAXOS_DIR", Value: paxosMount})
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName(c, i), Namespace: c.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					NodeSelector: c.Spec.NodeSelector,
					Tolerations:  c.Spec.Tolerations,
					Affinity:     c.Spec.Affinity,
					Volumes:      volumes,
					Containers: []corev1.Container{{
						Name:            "dstore",
						Image:           c.Spec.Image,
						ImagePullPolicy: c.Spec.ImagePullPolicy,
						Env:             env,
						Ports:           []corev1.ContainerPort{{Name: "iroh", ContainerPort: port, Protocol: corev1.ProtocolUDP}},
						VolumeMounts:    mounts,
						Resources:       c.Spec.Resources,
					}},
				},
			},
		},
	}
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
