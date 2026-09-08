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
	ready   bool   // Deployment has an available replica
	exists  bool   // Deployment exists
	joining bool   // a join Secret exists
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
		if err := r.ensureNode(ctx, &c, n0, RoleInit); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !n0.ready {
		return r.requeue(), r.setStatus(ctx, &c, func(s *dstorev1.DstoreClusterStatus) {
			s.Phase = dstorev1.PhaseBootstrapping
			s.Nodes = nodeStatuses(infos, nil)
			setCondition(s, "Ready", metav1.ConditionFalse, "Bootstrapping", "waiting for node 0 to start")
		})
	}

	// Dial the cluster through every node that is running.
	var members []Member
	for _, in := range sortedInfos(infos) {
		if in.ready && in.addr != "" {
			members = append(members, Member{ID: in.id, Addr: fmt.Sprintf("ip:%s:%d", in.addr, c.Spec.PortOrDefault())})
		}
	}
	cl, err := r.Dialer.Dial(ctx, members)
	if err != nil {
		log.Info("cluster not reachable yet", "error", err)
		return r.requeue(), r.setStatus(ctx, &c, func(s *dstorev1.DstoreClusterStatus) {
			s.Phase = dstorev1.PhaseBootstrapping
			s.Nodes = nodeStatuses(infos, nil)
			setCondition(s, "Ready", metav1.ConditionFalse, "Unreachable", "cluster not reachable: "+err.Error())
		})
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
			return r.requeue(), fmt.Errorf("token for node %d: %w", in.index, err)
		}
		seed := Ticket(members).Encode()
		if err := r.ensureJoinSecret(ctx, &c, in.index, hex.EncodeToString(reply.Token), seed); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.ensureNode(ctx, &c, in, RoleJoin); err != nil {
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
				return r.requeue(), fmt.Errorf("remove node %d: %w", in.index, err)
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
		s.Nodes = nodeStatuses(infos, v)
		if phase == dstorev1.PhaseReady {
			setCondition(s, "Ready", metav1.ConditionTrue, "Ready", fmt.Sprintf("%d members, %d voters", len(v.Nodes), len(v.Voters)))
		} else {
			setCondition(s, "Ready", metav1.ConditionFalse, phase, transitionText(v))
		}
	}); err != nil {
		return ctrl.Result{}, err
	}
	if phase != dstorev1.PhaseReady {
		return r.requeue(), nil
	}
	return ctrl.Result{RequeueAfter: 2 * time.Minute}, nil
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

func nodeStatuses(infos map[int32]*nodeInfo, v *view.View) []dstorev1.NodeStatus {
	var out []dstorev1.NodeStatus
	for _, in := range sortedInfos(infos) {
		st := dstorev1.NodeStatus{Index: in.index, ID: view.IDString(in.id), Address: in.addr, Phase: dstorev1.NodePending}
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

func (r *ClusterReconciler) ensureService(ctx context.Context, c *dstorev1.DstoreCluster, i int32) (string, error) {
	var svc corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: nodeName(c, i), Namespace: c.Namespace}, &svc)
	if apierrors.IsNotFound(err) {
		d := desiredService(c, i)
		if err := controllerutil.SetControllerReference(c, d, r.Scheme()); err != nil {
			return "", err
		}
		if err := r.Create(ctx, d); err != nil {
			return "", err
		}
		return d.Spec.ClusterIP, nil
	}
	if err != nil {
		return "", err
	}
	if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == corev1.ClusterIPNone {
		return "", nil
	}
	return svc.Spec.ClusterIP, nil
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

// ensureNode creates a node's claims and Deployment.
func (r *ClusterReconciler) ensureNode(ctx context.Context, c *dstorev1.DstoreCluster, in *nodeInfo, role string) error {
	if err := r.ensurePVC(ctx, c, in.index, pvcName(c, in.index), c.Spec.Storage.VolumeClaimTemplate); err != nil {
		return err
	}
	if c.Spec.Storage.PaxosVolumeClaimTemplate != nil {
		if err := r.ensurePVC(ctx, c, in.index, paxosPVCName(c, in.index), *c.Spec.Storage.PaxosVolumeClaimTemplate); err != nil {
			return err
		}
	}
	d := desiredDeployment(c, in.index, in.addr, role)
	if err := controllerutil.SetControllerReference(c, d, r.Scheme()); err != nil {
		return err
	}
	if err := r.Create(ctx, d); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	in.exists = true
	return nil
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
		objs = append(objs,
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: pvcName(c, i), Namespace: c.Namespace}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: paxosPVCName(c, i), Namespace: c.Namespace}},
		)
	}
	for _, o := range objs {
		if err := r.Delete(ctx, o); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// ---- status ----

func (r *ClusterReconciler) setStatus(ctx context.Context, c *dstorev1.DstoreCluster, fn func(s *dstorev1.DstoreClusterStatus)) error {
	before := c.Status.DeepCopy()
	fn(&c.Status)
	c.Status.ObservedGeneration = c.Generation
	if equalStatus(before, &c.Status) {
		return nil
	}
	return r.Status().Update(ctx, c)
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
