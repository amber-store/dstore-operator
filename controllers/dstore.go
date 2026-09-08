package controllers

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/node"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	irohkey "github.com/tmc/go-iroh/key"
)

// Member is a node the operator can dial: its identity and addresses
// ("ip:host:port"), best first.
type Member struct {
	ID    view.NodeID
	Addrs []string
}

// Cluster is what the reconciler needs from a running dstore cluster.
type Cluster interface {
	// View returns the cluster's current view.
	View() *view.View
	// Admin runs an operator command on any node.
	Admin(ctx context.Context, req node.AdminRequest) (node.AdminReply, error)
	Close()
}

// Dialer opens a Cluster handle over the members it is given.
type Dialer interface {
	Dial(ctx context.Context, members []Member) (Cluster, error)
}

// IrohDialer dials real clusters over one iroh endpoint with an ephemeral
// identity and no relay: the operator runs inside the cluster network.
type IrohDialer struct {
	ep  *transport.IrohEndpoint
	log *slog.Logger
}

// NewIrohDialer binds the operator's endpoint.
func NewIrohDialer(ctx context.Context, log *slog.Logger) (*IrohDialer, error) {
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return nil, err
	}
	ep, err := transport.BindIroh(ctx, transport.IrohConfig{SecretKey: sk, DirectTimeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	return &IrohDialer{ep: ep, log: log}, nil
}

// Close shuts the endpoint down.
func (d *IrohDialer) Close() error { return d.ep.Close() }

// Ticket builds the bootstrap ticket naming the members.
func Ticket(members []Member) ticket.Ticket {
	t := ticket.Ticket{}
	for _, m := range members {
		id := m.ID
		t.Members = append(t.Members, ticket.Member{ID: id[:], Addrs: append([]string{}, m.Addrs...)})
	}
	return t
}

// Dial implements Dialer.
func (d *IrohDialer) Dial(ctx context.Context, members []Member) (Cluster, error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("no members to dial")
	}
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cl, err := client.Dial(dctx, client.Config{Endpoint: d.ep, Ticket: Ticket(members), Logger: d.log, RequestTimeout: 30 * time.Second})
	if err != nil {
		return nil, err
	}
	return &irohCluster{cl: cl}, nil
}

type irohCluster struct{ cl *client.Cluster }

func (c *irohCluster) View() *view.View { return c.cl.View() }
func (c *irohCluster) Close()           { c.cl.Close() }
func (c *irohCluster) Admin(ctx context.Context, req node.AdminRequest) (node.AdminReply, error) {
	b, err := c.cl.Admin(ctx, view.NodeID{}, req)
	if err != nil {
		return node.AdminReply{}, err
	}
	var r node.AdminReply
	if err := codec.Unmarshal(b, &r); err != nil {
		return r, err
	}
	return r, nil
}

// NewIdentity generates a node secret key in the hex form dstore stores
// in <store>/identity, and returns it with the node id it yields.
func NewIdentity() (secretHex string, id view.NodeID, err error) {
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return "", view.NodeID{}, err
	}
	seed := sk.Bytes()
	secretHex = hex.EncodeToString(seed[:])
	id, err = IdentityID(secretHex)
	return secretHex, id, err
}

// IdentityID derives the node id of a hex secret key.
func IdentityID(secretHex string) (view.NodeID, error) {
	sk, err := irohkey.ParseSecretKey(strings.TrimSpace(secretHex))
	if err != nil {
		return view.NodeID{}, err
	}
	return view.NodeID(sk.Public().EndpointID().Bytes()), nil
}
