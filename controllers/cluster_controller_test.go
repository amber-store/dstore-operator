package controllers

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	dstorev1 "github.com/amber-store/dstore-operator/api/v1alpha1"
	"github.com/amber-store/dstore/node"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/view"
)

// fakeCluster stands in for a running dstore cluster: the test moves nodes
// into and out of its view as the real cluster would after a join or a
// removal completes.
type fakeCluster struct {
	mu      sync.Mutex
	v       *view.View
	tokens  int
	removed []view.NodeID
}

func (f *fakeCluster) View() *view.View {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.v.Clone()
}
func (f *fakeCluster) Close() {}
func (f *fakeCluster) Admin(ctx context.Context, req node.AdminRequest) (node.AdminReply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch req.Op {
	case "token-create":
		f.tokens++
		tok := make([]byte, 32)
		rand.Read(tok)
		return node.AdminReply{Token: tok}, nil
	case "node-remove":
		id := view.NodeID(req.Node)
		f.removed = append(f.removed, id)
		// The removal transition runs: pending set now, committed by the
		// test through commitRemove.
		f.v.Pending = &view.Pending{ID: f.v.Epoch + 1, Reason: "remove"}
		return node.AdminReply{}, nil
	}
	return node.AdminReply{}, fmt.Errorf("unexpected admin op %s", req.Op)
}

func (f *fakeCluster) addMember(id view.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.v.Nodes = append(f.v.Nodes, view.Node{ID: id[:], Weight: 10, Writable: true})
	f.v.Voters = append(f.v.Voters, view.Voter{ID: id[:], Since: f.v.Epoch})
	f.v.Epoch++
	f.v.Pending = nil
}

func (f *fakeCluster) commitRemove() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.removed {
		var nodes []view.Node
		for _, nd := range f.v.Nodes {
			if nd.NID() != id {
				nodes = append(nodes, nd)
			}
		}
		f.v.Nodes = nodes
		var voters []view.Voter
		for _, vo := range f.v.Voters {
			if view.NodeID(vo.ID) != id {
				voters = append(voters, vo)
			}
		}
		f.v.Voters = voters
	}
	f.removed = nil
	f.v.Pending = nil
	f.v.Epoch++
}

type fakeDialer struct {
	cluster *fakeCluster
	dials   int
	fail    bool
}

func (d *fakeDialer) Dial(ctx context.Context, members []Member) (Cluster, error) {
	d.dials++
	if d.fail {
		return nil, fmt.Errorf("unreachable")
	}
	if d.cluster.v == nil {
		// First contact: node 0 initialised the cluster alone.
		d.cluster.v = &view.View{ClusterID: []byte("c"), Incarnation: 1, Epoch: 1, Replicas: 3, MinReplicas: 2,
			Nodes: []view.Node{{ID: members[0].ID[:], Weight: 10, Writable: true}}, Voters: []view.Voter{{ID: members[0].ID[:], Since: 1}}}
	}
	return d.cluster, nil
}

type harness struct {
	t   *testing.T
	c   client.Client
	r   *ClusterReconciler
	fd  *fakeDialer
	key types.NamespacedName
	ips int
}

func newHarness(t *testing.T, nodes int32) *harness {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := dstorev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &dstorev1.DstoreCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns", Generation: 1},
		Spec: dstorev1.DstoreClusterSpec{Nodes: nodes, Image: "dstore:test", CapacityGiB: 100, ReplicationFactor: ptr(int32(3)), MinReplicationFactor: ptr(int32(2)),
			Storage: dstorev1.StorageSpec{VolumeClaimTemplate: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("50Gi")}}}}},
	}
	// The API server bumps a Deployment's generation when its spec changes;
	// the fake client does not, so the interceptor does.
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&dstorev1.DstoreCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if d, ok := obj.(*appsv1.Deployment); ok {
					d.Generation = 1
				}
				return cl.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if d, ok := obj.(*appsv1.Deployment); ok {
					var cur appsv1.Deployment
					if err := cl.Get(ctx, client.ObjectKeyFromObject(d), &cur); err == nil {
						d.Generation = cur.Generation
						if !apiequality.Semantic.DeepEqual(cur.Spec, d.Spec) {
							d.Generation++
						}
					}
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).Build()
	fd := &fakeDialer{cluster: &fakeCluster{}}
	r := &ClusterReconciler{Client: c, Dialer: fd, Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))}
	return &harness{t: t, c: c, r: r, fd: fd, key: types.NamespacedName{Name: "demo", Namespace: "ns"}}
}

// step reconciles once and then plays the cluster's part: Services get
// ClusterIPs, and the Deployment controller runs. A new Deployment's pod
// is up within the step; a changed spec (a new generation) is observed
// first and its recreated pod becomes available only at the next step,
// so that within the reconcile the status still shows the old pod, as
// it does on a real cluster.
func (h *harness) step() {
	h.t.Helper()
	if _, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key}); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
	var svcs corev1.ServiceList
	_ = h.c.List(context.Background(), &svcs, client.InNamespace("ns"))
	for i := range svcs.Items {
		if svcs.Items[i].Spec.ClusterIP == "" {
			h.ips++
			svcs.Items[i].Spec.ClusterIP = fmt.Sprintf("10.0.0.%d", h.ips)
			_ = h.c.Update(context.Background(), &svcs.Items[i])
		}
	}
	var deps appsv1.DeploymentList
	_ = h.c.List(context.Background(), &deps, client.InNamespace("ns"))
	for i := range deps.Items {
		d := &deps.Items[i]
		st := &d.Status
		switch {
		case st.ObservedGeneration == 0:
			st.ObservedGeneration, st.Replicas, st.UpdatedReplicas, st.AvailableReplicas = d.Generation, 1, 1, 1
		case st.ObservedGeneration != d.Generation:
			st.ObservedGeneration, st.UpdatedReplicas, st.AvailableReplicas = d.Generation, 1, 0
		case st.AvailableReplicas == 0:
			st.AvailableReplicas = 1
		default:
			continue
		}
		_ = h.c.Status().Update(context.Background(), d)
	}
}

// rewrite edits a node's Deployment behind the operator's back and
// settles its status, as a Deployment written by an older operator would
// look.
func (h *harness) rewrite(i int32, fn func(d *appsv1.Deployment)) {
	h.t.Helper()
	d := h.deployment(i)
	fn(d)
	if err := h.c.Update(context.Background(), d); err != nil {
		h.t.Fatal(err)
	}
	d = h.deployment(i)
	d.Status.ObservedGeneration, d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.AvailableReplicas = d.Generation, 1, 1, 1
	if err := h.c.Status().Update(context.Background(), d); err != nil {
		h.t.Fatal(err)
	}
}

// ready drives a fresh cluster to Ready with every node a member.
func (h *harness) ready(nodes int32) {
	h.t.Helper()
	h.step()
	h.step()
	h.step()
	for i := int32(1); i < nodes; i++ {
		h.fd.cluster.addMember(h.nodeID(i))
		h.step()
	}
	if got := h.cluster().Status.Phase; got != dstorev1.PhaseReady {
		h.t.Fatalf("phase %q", got)
	}
}

func (h *harness) deployment(i int32) *appsv1.Deployment {
	h.t.Helper()
	var d appsv1.Deployment
	if err := h.c.Get(context.Background(), types.NamespacedName{Name: fmt.Sprintf("demo-node-%d", i), Namespace: "ns"}, &d); err != nil {
		h.t.Fatal(err)
	}
	return &d
}

func (h *harness) env(i int32) map[string]corev1.EnvVar {
	h.t.Helper()
	env := map[string]corev1.EnvVar{}
	for _, e := range h.deployment(i).Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e
	}
	return env
}

func (h *harness) image(i int32) string {
	h.t.Helper()
	return h.deployment(i).Spec.Template.Spec.Containers[0].Image
}

func (h *harness) cluster() *dstorev1.DstoreCluster {
	h.t.Helper()
	var c dstorev1.DstoreCluster
	if err := h.c.Get(context.Background(), h.key, &c); err != nil {
		h.t.Fatal(err)
	}
	return &c
}

func (h *harness) nodeID(i int32) view.NodeID {
	h.t.Helper()
	var s corev1.Secret
	if err := h.c.Get(context.Background(), types.NamespacedName{Name: fmt.Sprintf("demo-node-%d-identity", i), Namespace: "ns"}, &s); err != nil {
		h.t.Fatal(err)
	}
	id, err := IdentityID(string(s.Data[SecretKeyIdentity]))
	if err != nil {
		h.t.Fatal(err)
	}
	return id
}

func (h *harness) hasDeployment(i int32) bool {
	var d appsv1.Deployment
	return h.c.Get(context.Background(), types.NamespacedName{Name: fmt.Sprintf("demo-node-%d", i), Namespace: "ns"}, &d) == nil
}

func (h *harness) hasSecret(name string) bool {
	var s corev1.Secret
	return h.c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "ns"}, &s) == nil
}

func TestBootstrapAndJoins(t *testing.T) {
	h := newHarness(t, 3)

	// Identities and services come first; node 0 starts once its service
	// has an address; nothing else runs.
	h.step()
	if h.hasDeployment(0) {
		t.Fatal("node 0 started before its service had an address")
	}
	h.step()
	for i := int32(0); i < 3; i++ {
		if !h.hasSecret(fmt.Sprintf("demo-node-%d-identity", i)) {
			t.Fatalf("identity secret %d missing", i)
		}
	}
	if !h.hasDeployment(0) || h.hasDeployment(1) {
		t.Fatalf("only node 0 should run: %v %v", h.hasDeployment(0), h.hasDeployment(1))
	}
	if got := h.cluster().Status.Phase; got != dstorev1.PhaseBootstrapping {
		t.Fatalf("phase %q", got)
	}
	// Node 0 is up: the operator dials, then starts exactly one join.
	h.step()
	c := h.cluster()
	if c.Status.Phase != dstorev1.PhaseJoining {
		t.Fatalf("phase %q, ticket %q", c.Status.Phase, c.Status.Ticket)
	}
	if !h.hasDeployment(1) || h.hasDeployment(2) {
		t.Fatalf("one join at a time: node1=%v node2=%v", h.hasDeployment(1), h.hasDeployment(2))
	}
	if !h.hasSecret("demo-node-1-join") || h.fd.cluster.tokens != 1 {
		t.Fatalf("join secret/token: %v %d", h.hasSecret("demo-node-1-join"), h.fd.cluster.tokens)
	}
	var d appsv1.Deployment
	_ = h.c.Get(context.Background(), types.NamespacedName{Name: "demo-node-1", Namespace: "ns"}, &d)
	env := map[string]string{}
	for _, e := range d.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if env["DSTORE_ROLE"] != RoleJoin || env["DSTORE_WEIGHT"] != "100" || env["DSTORE_ADVERTISE"] == "" || env["DSTORE_REPLICAS"] != "3" || env["DSTORE_MIN_REPLICAS"] != "2" {
		t.Fatalf("env %v", env)
	}
	// Node 1 is still joining: no second join starts.
	h.step()
	if h.hasDeployment(2) {
		t.Fatal("second join started while the first was in progress")
	}
	// The join completes; node 2 follows.
	h.fd.cluster.addMember(h.nodeID(1))
	h.step()
	if h.hasSecret("demo-node-1-join") {
		t.Fatal("spent join secret not removed")
	}
	if !h.hasDeployment(2) {
		t.Fatal("node 2 join not started")
	}
	h.fd.cluster.addMember(h.nodeID(2))
	h.step()
	c = h.cluster()
	if c.Status.Phase != dstorev1.PhaseReady || c.Status.Members != 3 || c.Status.Voters != 3 {
		t.Fatalf("status %+v", c.Status)
	}
	if c.Status.Ticket == "" || len(c.Status.Nodes) != 3 || c.Status.Nodes[2].Phase != dstorev1.NodeMember {
		t.Fatalf("status %+v", c.Status)
	}
	if !h.hasSecret("demo-node-0-identity") {
		t.Fatal("identity secret gone")
	}
	// A PVC per node, from the template, retained (no owner reference).
	var pvc corev1.PersistentVolumeClaim
	if err := h.c.Get(context.Background(), types.NamespacedName{Name: "demo-node-2-store", Namespace: "ns"}, &pvc); err != nil {
		t.Fatal(err)
	}
	if q := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; q.String() != "50Gi" || len(pvc.OwnerReferences) != 0 {
		t.Fatalf("pvc %v owners %d", q.String(), len(pvc.OwnerReferences))
	}
}

func TestScaleDown(t *testing.T) {
	h := newHarness(t, 3)
	h.step()
	h.step()
	h.step()
	h.fd.cluster.addMember(h.nodeID(1))
	h.step()
	h.fd.cluster.addMember(h.nodeID(2))
	h.step()
	if h.cluster().Status.Phase != dstorev1.PhaseReady {
		t.Fatal("not ready")
	}
	// Scale to 2: node 2 is removed through the cluster first.
	c := h.cluster()
	c.Spec.Nodes = 2
	if err := h.c.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	h.step()
	if len(h.fd.cluster.removed) != 1 || h.fd.cluster.removed[0] != h.nodeID(2) {
		t.Fatalf("removal not requested: %v", h.fd.cluster.removed)
	}
	if !h.hasDeployment(2) {
		t.Fatal("deployment deleted before the removal committed")
	}
	if h.cluster().Status.Phase != dstorev1.PhaseScalingDown {
		t.Fatalf("phase %q", h.cluster().Status.Phase)
	}
	// Still pending: nothing is deleted.
	h.step()
	if !h.hasDeployment(2) {
		t.Fatal("deployment deleted while the transition ran")
	}
	h.fd.cluster.commitRemove()
	h.step()
	if h.hasDeployment(2) || h.hasSecret("demo-node-2-identity") {
		t.Fatal("node 2 resources not deleted")
	}
	var pvc corev1.PersistentVolumeClaim
	if err := h.c.Get(context.Background(), types.NamespacedName{Name: "demo-node-2-store", Namespace: "ns"}, &pvc); err != nil {
		t.Fatal("retained volume deleted")
	}
	h.step()
	if st := h.cluster().Status; st.Phase != dstorev1.PhaseReady || st.Members != 2 {
		t.Fatalf("status %+v", st)
	}
}

func TestUnreachableCluster(t *testing.T) {
	h := newHarness(t, 2)
	h.step()
	h.step()
	h.fd.fail = true
	h.step()
	c := h.cluster()
	if c.Status.Phase != dstorev1.PhaseBootstrapping || h.hasDeployment(1) {
		t.Fatalf("phase %q node1=%v", c.Status.Phase, h.hasDeployment(1))
	}
	h.fd.fail = false
	h.step()
	if !h.hasDeployment(1) {
		t.Fatal("join not started once reachable")
	}
}

func TestIdentityDerivation(t *testing.T) {
	secret, id, err := NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	again, err := IdentityID(secret + "\n")
	if err != nil || again != id {
		t.Fatalf("derivation not stable: %v", err)
	}
	tk := Ticket([]Member{{ID: id, Addrs: []string{"ip:192.168.1.10:4433", "ip:10.0.0.1:4433"}}})
	if len(tk.Members) != 1 || tk.Members[0].Addrs[0] != "ip:192.168.1.10:4433" || tk.Members[0].Addrs[1] != "ip:10.0.0.1:4433" {
		t.Fatalf("ticket %+v", tk)
	}
}

func ptr[T any](v T) *T { return &v }

func TestInvalidReplication(t *testing.T) {
	h := newHarness(t, 3)
	c := h.cluster()
	c.Spec.MinReplicationFactor = ptr(int32(4))
	if err := h.c.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	h.step()
	c = h.cluster()
	if len(c.Status.Conditions) != 1 || c.Status.Conditions[0].Type != "Valid" || c.Status.Conditions[0].Status != metav1.ConditionFalse {
		t.Fatalf("conditions %+v", c.Status.Conditions)
	}
	if h.hasDeployment(0) {
		t.Fatal("nodes created for an invalid spec")
	}
}

// TestHostNetwork checks the host-network pod shape and that a running
// pod's host IP becomes the node's advertised and dialed address.
func TestHostNetwork(t *testing.T) {
	h := newHarness(t, 2)
	h.step()
	h.step()
	var d appsv1.Deployment
	if err := h.c.Get(context.Background(), types.NamespacedName{Name: "demo-node-0", Namespace: "ns"}, &d); err != nil {
		t.Fatal(err)
	}
	ps := d.Spec.Template.Spec
	if !ps.HostNetwork || ps.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Fatalf("pod not on the host network: %+v", ps.HostNetwork)
	}
	if p := ps.Containers[0].Ports[0]; p.HostPort != 4433 || p.ContainerPort != 4433 || p.Protocol != corev1.ProtocolUDP {
		t.Fatalf("port %+v", p)
	}
	env := map[string]corev1.EnvVar{}
	for _, e := range ps.Containers[0].Env {
		env[e.Name] = e
	}
	if env["DSTORE_ADVERTISE"].Value != "$(DSTORE_HOST_IP):4433" || env["DSTORE_HOST_IP"].ValueFrom.FieldRef.FieldPath != "status.hostIP" {
		t.Fatalf("advertise env %+v %+v", env["DSTORE_ADVERTISE"], env["DSTORE_HOST_IP"])
	}
	if _, ok := env["DSTORE_EXTRA_ARGS"]; ok {
		t.Fatalf("extra args %q set without spec.extraArgs", env["DSTORE_EXTRA_ARGS"].Value)
	}
	// A running pod on host 192.168.1.10: the operator dials that first.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-node-0-abc", Namespace: "ns", Labels: nodeLabels(h.cluster(), 0)},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, HostIP: "192.168.1.10"}}
	if err := h.c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	h.step()
	c := h.cluster()
	if c.Status.Nodes[0].Address != "192.168.1.10" {
		t.Fatalf("status address %q", c.Status.Nodes[0].Address)
	}
	tk, err := ticket.Parse(c.Status.Ticket)
	if err != nil || len(tk.Members) != 1 || tk.Members[0].Addrs[0] != "ip:192.168.1.10:4433" || len(tk.Members[0].Addrs) != 2 {
		t.Fatalf("ticket %+v %v", tk, err)
	}
}

// TestClusterNetwork checks the opt-out: Service addresses only.
func TestClusterNetwork(t *testing.T) {
	h := newHarness(t, 2)
	c := h.cluster()
	off := false
	c.Spec.HostNetwork = &off
	if err := h.c.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	h.step()
	h.step()
	var d appsv1.Deployment
	if err := h.c.Get(context.Background(), types.NamespacedName{Name: "demo-node-0", Namespace: "ns"}, &d); err != nil {
		t.Fatal(err)
	}
	ps := d.Spec.Template.Spec
	if ps.HostNetwork || ps.Containers[0].Ports[0].HostPort != 0 {
		t.Fatalf("host network used: %+v", ps)
	}
	for _, e := range ps.Containers[0].Env {
		if e.Name == "DSTORE_ADVERTISE" && e.Value != "10.0.0.1:4433" {
			t.Fatalf("advertise %q", e.Value)
		}
		if e.Name == "DSTORE_EXTRA_ARGS" {
			t.Fatalf("extra args %q set without the host network", e.Value)
		}
	}
}

// TestExtraArgs checks that spec.extraArgs reach the node as given.
func TestExtraArgs(t *testing.T) {
	h := newHarness(t, 1)
	c := h.cluster()
	c.Spec.Port = 4434
	c.Spec.ExtraArgs = []string{"--bind", "127.0.0.1:4434"}
	if err := h.c.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	h.step()
	h.step()
	if got := h.env(0)["DSTORE_EXTRA_ARGS"].Value; got != "--bind 127.0.0.1:4434" {
		t.Fatalf("extra args %q", got)
	}
}

// TestRelay checks the relay knobs: by default the node image's own
// default applies (no relay env), spec.relay names a relay, and
// spec.noRelay turns relays off.
func TestRelay(t *testing.T) {
	h := newHarness(t, 1)
	h.step()
	h.step()
	env := h.env(0)
	if _, ok := env["DSTORE_RELAY"]; ok {
		t.Fatalf("DSTORE_RELAY set by default: %q", env["DSTORE_RELAY"].Value)
	}
	if _, ok := env["DSTORE_NO_RELAY"]; ok {
		t.Fatalf("DSTORE_NO_RELAY set by default: %q", env["DSTORE_NO_RELAY"].Value)
	}
	c := h.cluster()
	c.Spec.Relay = "https://relay.example.com"
	if err := h.c.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	h.step()
	if got := h.env(0)["DSTORE_RELAY"].Value; got != "https://relay.example.com" {
		t.Fatalf("DSTORE_RELAY %q", got)
	}
	c = h.cluster()
	c.Spec.Relay = ""
	c.Spec.NoRelay = true
	if err := h.c.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	h.step()
	h.step()
	env = h.env(0)
	if _, ok := env["DSTORE_RELAY"]; ok || env["DSTORE_NO_RELAY"].Value != "1" {
		t.Fatalf("relay env %+v %+v", env["DSTORE_RELAY"], env["DSTORE_NO_RELAY"])
	}
}

// TestUpgradeRollsOnlyChangedNodes covers an operator upgrade over a
// cluster whose Deployments were written without the spec hash (#4):
// a Deployment whose template already matches is annotated in place
// without a restart, the changed ones roll one at a time, and the next
// waits until the previous Deployment reports its new pod available,
// not on the stale status of the same pass.
func TestUpgradeRollsOnlyChangedNodes(t *testing.T) {
	h := newHarness(t, 3)
	h.ready(3)
	for i := int32(0); i < 3; i++ {
		h.rewrite(i, func(d *appsv1.Deployment) {
			delete(d.Annotations, AnnotationSpecHash)
			if i > 0 {
				d.Spec.Template.Spec.Containers[0].Image = "dstore:old"
			}
		})
	}
	gen0 := h.deployment(0).Generation
	h.step()
	d0, d1, d2 := h.deployment(0), h.deployment(1), h.deployment(2)
	if d0.Annotations[AnnotationSpecHash] == "" || d0.Generation != gen0 {
		t.Fatalf("node 0 with an unchanged template restarted: generation %d -> %d", gen0, d0.Generation)
	}
	if h.image(1) != "dstore:test" || d1.Annotations[AnnotationSpecHash] == "" {
		t.Fatalf("node 1 not rolled: %s", h.image(1))
	}
	if h.image(2) != "dstore:old" || d2.Annotations[AnnotationSpecHash] != "" {
		t.Fatalf("node 2 rolled in the same pass as node 1")
	}
	// Node 1's Deployment has observed the change but its pod is not
	// back: node 2 waits.
	h.step()
	if h.image(2) != "dstore:old" {
		t.Fatal("node 2 rolled while node 1's new pod was not available")
	}
	h.step()
	if h.image(2) != "dstore:test" {
		t.Fatal("node 2 not rolled once node 1 settled")
	}
}

// TestSpecChangeRollsNodes checks that a spec change reaches running
// nodes (#2), one node at a time, and that nothing is written when the
// spec has not changed.
func TestSpecChangeRollsNodes(t *testing.T) {
	h := newHarness(t, 3)
	h.ready(3)
	// A joined node's Deployment still names the spent, deleted join
	// secret: the references are optional so its pod can restart.
	for _, name := range []string{"DSTORE_SEED", "DSTORE_TOKEN"} {
		ref := h.env(1)[name].ValueFrom.SecretKeyRef
		if ref == nil || ref.Optional == nil || !*ref.Optional {
			t.Fatalf("%s reference not optional: %+v", name, ref)
		}
	}
	// Steady state: no Deployment or Service is written.
	rvs := map[string]string{}
	for i := int32(0); i < 3; i++ {
		rvs[fmt.Sprintf("d%d", i)] = h.deployment(i).ResourceVersion
	}
	h.step()
	h.step()
	for i := int32(0); i < 3; i++ {
		if got := h.deployment(i).ResourceVersion; got != rvs[fmt.Sprintf("d%d", i)] {
			t.Fatalf("node %d deployment rewritten without a spec change", i)
		}
	}
	// Change the image and the port: node 0 rolls first, alone.
	c := h.cluster()
	c.Spec.Image = "dstore:new"
	c.Spec.Port = 4434
	c.Spec.Env = []corev1.EnvVar{{Name: "EXTRA", Value: "1"}}
	if err := h.c.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	h.step()
	if h.image(0) != "dstore:new" || h.image(1) != "dstore:test" || h.image(2) != "dstore:test" {
		t.Fatalf("images after first step: %s %s %s", h.image(0), h.image(1), h.image(2))
	}
	if got := h.env(0)["EXTRA"].Value; got != "1" {
		t.Fatalf("env not updated: %q", got)
	}
	if h.deployment(0).Spec.Template.Spec.Containers[0].Ports[0].HostPort != 4434 {
		t.Fatal("port not updated")
	}
	// Node 0 is restarting: nothing else rolls until it is back.
	h.step()
	if h.image(1) != "dstore:test" || h.image(2) != "dstore:test" {
		t.Fatalf("next node rolled while node 0 was down: %s %s", h.image(1), h.image(2))
	}
	if st := h.cluster().Status; st.Phase != dstorev1.PhaseReady {
		t.Fatalf("phase %q while node 0 restarts with the others up", st.Phase)
	}
	h.step()
	if h.image(1) != "dstore:new" || h.image(2) != "dstore:test" {
		t.Fatalf("images after node 0 came back: %s %s", h.image(1), h.image(2))
	}
	h.step()
	h.step()
	if h.image(2) != "dstore:new" {
		t.Fatal("node 2 not rolled")
	}
	// The Services follow the port.
	var svc corev1.Service
	if err := h.c.Get(context.Background(), types.NamespacedName{Name: "demo-node-2", Namespace: "ns"}, &svc); err != nil {
		t.Fatal(err)
	}
	if svc.Spec.Ports[0].Port != 4434 || svc.Spec.Ports[0].TargetPort.IntValue() != 4434 {
		t.Fatalf("service ports %+v", svc.Spec.Ports)
	}
	h.step()
	if st := h.cluster().Status; st.Phase != dstorev1.PhaseReady || st.Members != 3 {
		t.Fatalf("status %+v", st)
	}
}

// TestRequeueWhileTransitionPending checks that a cluster whose members
// are all in but whose transition is still running is polled at the
// short interval, so status does not lag the nodes.
func TestRequeueWhileTransitionPending(t *testing.T) {
	h := newHarness(t, 2)
	h.ready(2)
	res, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key})
	if err != nil || res.RequeueAfter <= h.r.requeue().RequeueAfter {
		t.Fatalf("idle cluster polled every %v", res.RequeueAfter)
	}
	h.fd.cluster.v.Pending = &view.Pending{ID: h.fd.cluster.v.Epoch + 1, Reason: "join"}
	res, err = h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key})
	if err != nil || res.RequeueAfter != h.r.requeue().RequeueAfter {
		t.Fatalf("pending transition polled every %v", res.RequeueAfter)
	}
}
