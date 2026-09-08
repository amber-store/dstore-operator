// Command manager runs the dstore operator.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	dstorev1 "github.com/amber-store/dstore-operator/api/v1alpha1"
	"github.com/amber-store/dstore-operator/controllers"
)

func main() {
	var metricsAddr, probeAddr string
	var leaderElection bool
	var requeue time.Duration
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe endpoint")
	flag.BoolVar(&leaderElection, "leader-elect", false, "enable leader election")
	flag.DurationVar(&requeue, "requeue", 15*time.Second, "polling interval while a cluster is changing")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		log.Error("scheme", "error", err)
		os.Exit(1)
	}
	if err := dstorev1.AddToScheme(scheme); err != nil {
		log.Error("scheme", "error", err)
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElection,
		LeaderElectionID:       "dstore-operator.amber-store.io",
	})
	if err != nil {
		log.Error("manager", "error", err)
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()
	dialer, err := controllers.NewIrohDialer(context.Background(), log)
	if err != nil {
		log.Error("iroh endpoint", "error", err)
		os.Exit(1)
	}
	defer dialer.Close()

	rec := &controllers.ClusterReconciler{Client: mgr.GetClient(), Dialer: dialer, Log: log, Requeue: requeue}
	if err := rec.SetupWithManager(mgr); err != nil {
		log.Error("controller", "error", err)
		os.Exit(1)
	}
	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)
	log.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		log.Error("manager exited", "error", err)
		os.Exit(1)
	}
}
