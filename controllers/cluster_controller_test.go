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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dstorev1 "github.com/amber-store/dstore-operator/api/v1alpha1"
	"github.com/amber-store/dstore/node"
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
		Spec: dstorev1.DstoreClusterSpec{Nodes: nodes, Image: "dstore:test", CapacityGiB: 100,
			Storage: dstorev1.StorageSpec{VolumeClaimTemplate: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("50Gi")}}}}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&dstorev1.DstoreCluster{}).Build()
	fd := &fakeDialer{cluster: &fakeCluster{}}
	r := &ClusterReconciler{Client: c, Dialer: fd, Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))}
	return &harness{t: t, c: c, r: r, fd: fd, key: types.NamespacedName{Name: "demo", Namespace: "ns"}}
}

// step reconciles once and then plays the cluster's part: Services get
// ClusterIPs and Deployments become available.
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
		if deps.Items[i].Status.AvailableReplicas == 0 {
			deps.Items[i].Status.AvailableReplicas = 1
			_ = h.c.Status().Update(context.Background(), &deps.Items[i])
		}
	}
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
	if env["DSTORE_ROLE"] != RoleJoin || env["DSTORE_WEIGHT"] != "100" || env["DSTORE_ADVERTISE"] == "" {
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
	tk := Ticket([]Member{{ID: id, Addr: "ip:10.0.0.1:4433"}})
	if len(tk.Members) != 1 || tk.Members[0].Addrs[0] != "ip:10.0.0.1:4433" {
		t.Fatalf("ticket %+v", tk)
	}
}
