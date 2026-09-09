// Package controllers reconciles DstoreCluster objects into running,
// joined dstore clusters.
package controllers

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dstorev1 "github.com/amber-store/dstore-operator/api/v1alpha1"
	"github.com/amber-store/dstore/node"
	"github.com/amber-store/dstore/view"
)

// ClusterReconciler reconciles a DstoreCluster.
type ClusterReconciler struct {
	client.Client
	Dialer Dialer
	Log    *slog.Logger
	// Requeue is the polling interval while something is in progress.
	Requeue time.Duration
}

// SetupWithManager registers the reconciler.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dstorev1.DstoreCluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		// Node pods are owned by their Deployments' ReplicaSets, not by the
		// cluster; watch them by label so a pod landing on a host (and its
		// host IP) is seen at once.
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			name := o.GetLabels()[LabelCluster]
			if name == "" || o.GetLabels()[LabelApp] != AppName {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name, Namespace: o.GetNamespace()}}}
		})).
		Complete(r)
}

func (r *ClusterReconciler) requeue() ctrl.Result {
	d := r.Requeue
	if d == 0 {
		d = 15 * time.Second
	}
	return ctrl.Result{RequeueAfter: d}
}

// nodeInfo is what the reconciler knows about one node index.
type nodeInfo struct {
	index   int32
	id      view.NodeID
	addr    string // ClusterIP; empty until the Service has one
	hostIP  string // the running pod's host IP, with the host network
	ready   bool   // Deployment has an available replica
	settled bool   // Deployment has observed its spec and its new pod is available
	exists  bool   // Deployment exists
	joining bool   // a join Secret exists
}

// addrs returns the node's dial addresses, host IP first.
func (in *nodeInfo) addrs(port int32) []string {
	var out []string
	if in.hostIP != "" {
		out = append(out, fmt.Sprintf("ip:%s:%d", in.hostIP, port))
	}
	if in.addr != "" {
		out = append(out, fmt.Sprintf("ip:%s:%d", in.addr, port))
	}
	return out
}

// address is what status reports: the host IP with the host network,
// the Service address otherwise.
func (in *nodeInfo) address(hostNet bool) string {
	if hostNet && in.hostIP != "" {
		return in.hostIP
	}
	return in.addr
}

// Reconcile drives one cluster towards its spec (§8.1 of the design: one
// join at a time, each a voter change plus a placement transition).
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.With("cluster", req.NamespacedName.String())
	var c dstorev1.DstoreCluster
	if err := r.Get(ctx, req.NamespacedName, &c); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if c.Spec.Nodes < 1 {
		return ctrl.Result{}, r.setStatus(ctx, &c, func(s *dstorev1.DstoreClusterStatus) {
			setCondition(s, "Valid", metav1.ConditionFalse, "InvalidSpec", "spec.nodes must be at least 1")
		})
	}
	if c.Spec.Image == "" {
		return ctrl.Result{}, r.setStatus(ctx, &c, func(s *dstorev1.DstoreClusterStatus) {
			setCondition(s, "Valid", metav1.ConditionFalse, "InvalidSpec", "spec.image is required")
		})
	}
	if m := c.Spec.MinReplicationFactor; m != nil && *m > c.Spec.ReplicationFactorOrDefault() {
		return ctrl.Result{}, r.setStatus(ctx, &c, func(s *dstorev1.DstoreClusterStatus) {
			setCondition(s, "Valid", metav1.ConditionFalse, "InvalidSpec", "spec.minReplicationFactor must not exceed spec.replicationFactor")
		})
	}

	// Identities and Services for every desired node, so ids and addresses
	// are known before anything runs.
	infos := map[int32]*nodeInfo{}
	for i := int32(0); i < c.Spec.Nodes; i++ {
		id, err := r.ensureIdentity(ctx, &c, i)
		if err != nil {
			return ctrl.Result{}, err
		}
		addr, err := r.ensureService(ctx, &c, i)
		if err != nil {
			return ctrl.Result{}, err
		}
		infos[i] = &nodeInfo{index: i, id: id, addr: addr}
	}
	// Existing node resources beyond the spec (scale-down candidates).
	extra, err := r.extraNodes(ctx, &c)
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, i := range extra {
		id, err := r.identityOf(ctx, &c, i)
		if err != nil {
			continue
		}
		addr, _ := r.serviceAddr(ctx, &c, i)
		infos[i] = &nodeInfo{index: i, id: id, addr: addr}
	}
	for _, in := range infos {
		if err := r.observeDeployment(ctx, &c, in); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Node 0 bootstraps the cluster.
	n0 := infos[0]
	if n0.addr == "" {
		log.Info("waiting for node 0 service address")
		return r.requeue(), nil
	}
	if !n0.exists {
		if _, err := r.ensureNode(ctx, &c, n0, RoleInit); err != nil {
			return ctrl.Result{}, err
		}
	} else if err := r.rollout(ctx, &c, infos); err != nil {
		return ctrl.Result{}, err
	}

	// Dial the cluster through every node that is running.
	var members []Member
	for _, in := range sortedInfos(infos) {
		if in.ready && len(in.addrs(c.Spec.PortOrDefault())) > 0 {
			members = append(members, Member{ID: in.id, Addrs: in.addrs(c.Spec.PortOrDefault())})
		}
	}
	if len(members) == 0 {
		return result(r.requeue(), r.setStatus(ctx, &c, func(s *dstorev1.DstoreClusterStatus) {
			s.Phase = dstorev1.PhaseBootstrapping
			s.Nodes = nodeStatuses(&c, infos, nil)
			setCondition(s, "Ready", metav1.ConditionFalse, "Bootstrapping", "waiting for node 0 to start")
		}))
	}
	cl, err := r.Dialer.Dial(ctx, members)
	if err != nil {
		log.Info("cluster not reachable yet", "error", err)
		return result(r.requeue(), r.setStatus(ctx, &c, func(s *dstorev1.DstoreClusterStatus) {
			s.Phase = dstorev1.PhaseBootstrapping
			s.Nodes = nodeStatuses(&c, infos, nil)
			setCondition(s, "Ready", metav1.ConditionFalse, "Unreachable", "cluster not reachable: "+err.Error())
		}))
	}
	defer cl.Close()
	v := cl.View()
	if v == nil {
		return r.requeue(), nil
	}
	busy := v.Pending != nil || v.VoterSync == view.VoterSyncPending || len(v.Ramps) > 0
	phase := dstorev1.PhaseReady

	// Joins: one at a time, only when the cluster is idle.
	for _, in := range sortedInfos(infos) {
		if in.index >= c.Spec.Nodes {
			continue
		}
		if _, member := v.Node(in.id); member {
			if in.joining {
				// Joined: the token is spent; drop the join secret.
				_ = r.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: joinSecretName(&c, in.index), Namespace: c.Namespace}})
			}
			continue
		}
		if in.index == 0 {
			continue // node 0 bootstrapped the cluster; it is in the view or the view is stale
		}
		phase = dstorev1.PhaseJoining
		if in.exists {
			// Joining (or restarted before joining); wait.
			busy = true
			continue
		}
		if busy {
			continue
		}
		if in.addr == "" {
			continue
		}
		reply, err := cl.Admin(ctx, node.AdminRequest{Op: "token-create"})
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("token for node %d: %w", in.index, err)
		}
		seed := Ticket(members).Encode()
		if err := r.ensureJoinSecret(ctx, &c, in.index, hex.EncodeToString(reply.Token), seed); err != nil {
			return ctrl.Result{}, err
		}
		if _, err := r.ensureNode(ctx, &c, in, RoleJoin); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("join started", "node", in.index, "id", view.ShortID(in.id))
		busy = true
	}

	// Scale-down: remove members beyond the spec, one at a time, then
	// delete their resources once the view no longer lists them.
	for _, in := range sortedInfos(infos) {
		if in.index < c.Spec.Nodes {
			continue
		}
		phase = dstorev1.PhaseScalingDown
		if _, member := v.Node(in.id); member {
			if busy {
				continue
			}
			if _, err := cl.Admin(ctx, node.AdminRequest{Op: "node-remove", Node: in.id[:], AllowUnsafe: true}); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove node %d: %w", in.index, err)
			}
			log.Info("removal started", "node", in.index)
			busy = true
			continue
		}
		if v.IsVoter(in.id) || view.Contains(v.RemoveVoters, in.id) {
			continue // its vote is still being removed
		}
		if err := r.deleteNode(ctx, &c, in.index, c.Spec.Storage.DeleteVolumesOnScaleDown); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("node resources deleted", "node", in.index)
	}

	if err := r.setStatus(ctx, &c, func(s *dstorev1.DstoreClusterStatus) {
		s.Phase = phase
		s.Epoch = v.Epoch
		s.Members = int32(len(v.Nodes))
		s.Voters = int32(len(v.Voters))
		s.Ticket = Ticket(members).Encode()
		s.Transition = transitionText(v)
		s.Nodes = nodeStatuses(&c, infos, v)
		if phase == dstorev1.PhaseReady {
			setCondition(s, "Ready", metav1.ConditionTrue, "Ready", fmt.Sprintf("%d members, %d voters", len(v.Nodes), len(v.Voters)))
		} else {
			setCondition(s, "Ready", metav1.ConditionFalse, phase, transitionText(v))
		}
	}); err != nil {
		return ctrl.Result{}, err
	}
	// Poll while anything is in flight, a transition after the last join
	// included, so that status does not lag the nodes.
	if phase != dstorev1.PhaseReady || busy {
		return r.requeue(), nil
	}
	return ctrl.Result{RequeueAfter: 2 * time.Minute}, nil
}

// rollout brings the existing nodes' Deployments up to the spec, one
// node at a time: a spec change restarts the node, so the pass stops
// there, and later passes go past that node only once its Deployment
// has observed the change and reports the new pod available. The
// Deployment status read at the start of a pass is what gates it; the
// stale status right after an update is never trusted. Nodes beyond
// spec.nodes are on their way out and are left alone.
func (r *ClusterReconciler) rollout(ctx context.Context, c *dstorev1.DstoreCluster, infos map[int32]*nodeInfo) error {
	for _, in := range sortedInfos(infos) {
		if !in.exists || in.index >= c.Spec.Nodes {
			continue
		}
		changed, err := r.ensureNode(ctx, c, in, nodeRole(in.index))
		if err != nil {
			return err
		}
		if changed {
			r.Log.Info("node updated", "cluster", client.ObjectKeyFromObject(c).String(), "node", in.index)
			return nil
		}
		if !in.settled {
			return nil
		}
	}
	return nil
}

func transitionText(v *view.View) string {
	if v.Pending == nil {
		if v.VoterSync == view.VoterSyncPending {
			return "voter change in progress"
		}
		return ""
	}
	p := v.Pending
	return fmt.Sprintf("%s: %d/%d acked, %d/%d done", p.Reason, len(p.ParticipantsAck), len(v.AllMembers()), len(p.Done), len(p.Participants))
}

func sortedInfos(infos map[int32]*nodeInfo) []*nodeInfo {
	out := make([]*nodeInfo, 0, len(infos))
	for _, in := range infos {
		out = append(out, in)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].index < out[j].index })
	return out
}

func nodeStatuses(c *dstorev1.DstoreCluster, infos map[int32]*nodeInfo, v *view.View) []dstorev1.NodeStatus {
	var out []dstorev1.NodeStatus
	for _, in := range sortedInfos(infos) {
		st := dstorev1.NodeStatus{Index: in.index, ID: view.IDString(in.id), Address: in.address(c.Spec.HostNetworkOrDefault()), Phase: dstorev1.NodePending}
		if v != nil {
			if _, ok := v.Node(in.id); ok {
				st.Phase = dstorev1.NodeMember
				st.Voter = v.IsVoter(in.id)
			} else if in.exists {
				st.Phase = dstorev1.NodeJoining
			}
		}
		out = append(out, st)
	}
	return out
}

// ---- resources ----

func (r *ClusterReconciler) ensureIdentity(ctx context.Context, c *dstorev1.DstoreCluster, i int32) (view.NodeID, error) {
	var s corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: identitySecretName(c, i), Namespace: c.Namespace}, &s)
	if err == nil {
		return IdentityID(string(s.Data[SecretKeyIdentity]))
	}
	if !apierrors.IsNotFound(err) {
		return view.NodeID{}, err
	}
	secretHex, id, err := NewIdentity()
	if err != nil {
		return view.NodeID{}, err
	}
	s = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: identitySecretName(c, i), Namespace: c.Namespace, Labels: nodeLabels(c, i)},
		Data:       map[string][]byte{SecretKeyIdentity: []byte(secretHex + "\n")},
	}
	if err := controllerutil.SetControllerReference(c, &s, r.Scheme()); err != nil {
		return view.NodeID{}, err
	}
	if err := r.Create(ctx, &s); err != nil {
		return view.NodeID{}, err
	}
	return id, nil
}

func (r *ClusterReconciler) identityOf(ctx context.Context, c *dstorev1.DstoreCluster, i int32) (view.NodeID, error) {
	var s corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: identitySecretName(c, i), Namespace: c.Namespace}, &s); err != nil {
		return view.NodeID{}, err
	}
	return IdentityID(string(s.Data[SecretKeyIdentity]))
}

// ensureService creates or updates node i's Service and returns its
// ClusterIP, empty until one is allocated.
func (r *ClusterReconciler) ensureService(ctx context.Context, c *dstorev1.DstoreCluster, i int32) (string, error) {
	want := desiredService(c, i)
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: want.Name, Namespace: want.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		mergeLabels(svc, want.Labels)
		svc.Spec.Type = want.Spec.Type
		svc.Spec.Selector = want.Spec.Selector
		svc.Spec.Ports = want.Spec.Ports
		return controllerutil.SetControllerReference(c, svc, r.Scheme())
	})
	if err != nil {
		return "", err
	}
	if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == corev1.ClusterIPNone {
		return "", nil
	}
	return svc.Spec.ClusterIP, nil
}

func mergeLabels(o client.Object, labels map[string]string) {
	l := o.GetLabels()
	if l == nil {
		l = map[string]string{}
	}
	for k, v := range labels {
		l[k] = v
	}
	o.SetLabels(l)
}

func (r *ClusterReconciler) serviceAddr(ctx context.Context, c *dstorev1.DstoreCluster, i int32) (string, error) {
	var svc corev1.Service
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName(c, i), Namespace: c.Namespace}, &svc); err != nil {
		return "", err
	}
	return svc.Spec.ClusterIP, nil
}

func (r *ClusterReconciler) observeDeployment(ctx context.Context, c *dstorev1.DstoreCluster, in *nodeInfo) error {
	var d appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Name: nodeName(c, in.index), Namespace: c.Namespace}, &d)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	in.exists = true
	in.ready = d.Status.AvailableReplicas > 0
	replicas := int32(1)
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	in.settled = d.Status.ObservedGeneration >= d.Generation && d.Status.UpdatedReplicas == replicas && d.Status.AvailableReplicas == replicas
	if c.Spec.HostNetworkOrDefault() {
		var pods corev1.PodList
		if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels(nodeLabels(c, in.index))); err == nil {
			for _, p := range pods.Items {
				if p.Status.Phase == corev1.PodRunning && p.Status.HostIP != "" {
					in.hostIP = p.Status.HostIP
				}
			}
		}
	}
	var js corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: joinSecretName(c, in.index), Namespace: c.Namespace}, &js); err == nil {
		in.joining = true
	}
	return nil
}

func (r *ClusterReconciler) ensurePVC(ctx context.Context, c *dstorev1.DstoreCluster, i int32, name string, tmpl corev1.PersistentVolumeClaimSpec) error {
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: c.Namespace}, &pvc)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	d := desiredPVC(c, i, name, tmpl)
	// Retained claims carry no owner reference, so they outlive the
	// cluster object; deletable ones are garbage-collected with it.
	if c.Spec.Storage.DeleteVolumesOnScaleDown {
		if err := controllerutil.SetControllerReference(c, d, r.Scheme()); err != nil {
			return err
		}
	}
	return r.Create(ctx, d)
}

// ensureNode creates a node's claim and creates or updates its
// Deployment. It reports whether the Deployment's spec changed, which
// restarts the node (one replica, Recreate): a write that only adds the
// hash annotation to a matching template does not count. The spec the
// operator last wrote is recorded as a hash annotation, so an unchanged
// spec costs no write and server-side defaults never look like drift.
func (r *ClusterReconciler) ensureNode(ctx context.Context, c *dstorev1.DstoreCluster, in *nodeInfo, role string) (bool, error) {
	if err := r.ensurePVC(ctx, c, in.index, pvcName(c, in.index), c.Spec.Storage.VolumeClaimTemplate); err != nil {
		return false, err
	}
	want := desiredDeployment(c, in.index, role)
	hash := specHash(want)
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: want.Name, Namespace: want.Namespace}}
	var generation int64
	res, err := controllerutil.CreateOrUpdate(ctx, r.Client, d, func() error {
		generation = d.Generation
		if d.Annotations[AnnotationSpecHash] == hash {
			return nil
		}
		mergeLabels(d, want.Labels)
		if d.Annotations == nil {
			d.Annotations = map[string]string{}
		}
		d.Annotations[AnnotationSpecHash] = hash
		d.Spec.Replicas = want.Spec.Replicas
		d.Spec.Strategy = want.Spec.Strategy
		d.Spec.Template = want.Spec.Template
		if d.Spec.Selector == nil {
			d.Spec.Selector = want.Spec.Selector // immutable once set
		}
		return controllerutil.SetControllerReference(c, d, r.Scheme())
	})
	if err != nil {
		return false, err
	}
	in.exists = true
	// The API server bumps the generation only when the spec changed.
	return res == controllerutil.OperationResultCreated || d.Generation != generation, nil
}

func (r *ClusterReconciler) ensureJoinSecret(ctx context.Context, c *dstorev1.DstoreCluster, i int32, tokenHex, seed string) error {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: joinSecretName(c, i), Namespace: c.Namespace, Labels: nodeLabels(c, i)},
		Data:       map[string][]byte{SecretKeyToken: []byte(tokenHex), SecretKeySeed: []byte(seed)},
	}
	if err := controllerutil.SetControllerReference(c, s, r.Scheme()); err != nil {
		return err
	}
	if err := r.Create(ctx, s); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return r.Update(ctx, s)
		}
		return err
	}
	return nil
}

// extraNodes lists node indices with a Deployment beyond spec.nodes.
func (r *ClusterReconciler) extraNodes(ctx context.Context, c *dstorev1.DstoreCluster) ([]int32, error) {
	var list appsv1.DeploymentList
	if err := r.List(ctx, &list, client.InNamespace(c.Namespace), client.MatchingLabels{LabelCluster: c.Name, LabelApp: AppName}); err != nil {
		return nil, err
	}
	var out []int32
	for _, d := range list.Items {
		var i int32
		if _, err := fmt.Sscanf(d.Labels[LabelNode], "%d", &i); err != nil {
			continue
		}
		if i >= c.Spec.Nodes {
			out = append(out, i)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] > out[b] })
	return out, nil
}

// deleteNode removes a node's Deployment, Service and Secrets, and its
// claims when asked.
func (r *ClusterReconciler) deleteNode(ctx context.Context, c *dstorev1.DstoreCluster, i int32, volumes bool) error {
	objs := []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: nodeName(c, i), Namespace: c.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: nodeName(c, i), Namespace: c.Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: joinSecretName(c, i), Namespace: c.Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: identitySecretName(c, i), Namespace: c.Namespace}},
	}
	if volumes {
		objs = append(objs, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: pvcName(c, i), Namespace: c.Namespace}})
	}
	for _, o := range objs {
		if err := r.Delete(ctx, o); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// ---- status ----

// setStatus writes the status as a merge patch: Deployment and pod
// events enqueue the cluster again while a status write is in flight,
// and the next reconcile may read the cached copy from before it, so an
// update with that copy's resourceVersion would conflict.
func (r *ClusterReconciler) setStatus(ctx context.Context, c *dstorev1.DstoreCluster, fn func(s *dstorev1.DstoreClusterStatus)) error {
	orig := c.DeepCopy()
	fn(&c.Status)
	c.Status.ObservedGeneration = c.Generation
	if equalStatus(&orig.Status, &c.Status) {
		return nil
	}
	return r.Status().Patch(ctx, c, client.MergeFrom(orig))
}

// result pairs a requeue with a status write or another error: the error
// wins, so the reconciler never returns both a result and an error.
func result(res ctrl.Result, err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	return res, nil
}

func equalStatus(a, b *dstorev1.DstoreClusterStatus) bool {
	if a.Phase != b.Phase || a.Ticket != b.Ticket || a.Epoch != b.Epoch || a.Members != b.Members || a.Voters != b.Voters || a.Transition != b.Transition || a.ObservedGeneration != b.ObservedGeneration || len(a.Nodes) != len(b.Nodes) || len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Nodes {
		if a.Nodes[i] != b.Nodes[i] {
			return false
		}
	}
	for i := range a.Conditions {
		x, y := a.Conditions[i], b.Conditions[i]
		if x.Type != y.Type || x.Status != y.Status || x.Reason != y.Reason || x.Message != y.Message {
			return false
		}
	}
	return true
}

func setCondition(s *dstorev1.DstoreClusterStatus, typ string, status metav1.ConditionStatus, reason, msg string) {
	for i := range s.Conditions {
		if s.Conditions[i].Type == typ {
			if s.Conditions[i].Status != status {
				s.Conditions[i].LastTransitionTime = metav1.Now()
			}
			s.Conditions[i].Status, s.Conditions[i].Reason, s.Conditions[i].Message = status, reason, msg
			return
		}
	}
	s.Conditions = append(s.Conditions, metav1.Condition{Type: typ, Status: status, Reason: reason, Message: msg, LastTransitionTime: metav1.Now()})
}
