/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sync

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/operations"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/syncutil"
	"github.com/open-policy-agent/gatekeeper/pkg/syncutil/cmt"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller").WithValues("metaKind", "Sync")

type Adder struct {
	CMT *cmt.CacheManagerTracker
	Events          <-chan event.GenericEvent
	Tracker         *readiness.Tracker
	ProcessExcluder *process.Excluder
}

// Add creates a new Sync Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func (a *Adder) Add(mgr manager.Manager) error {
	if !operations.HasValidationOperations() {
		return nil
	}
	reporter, err := syncutil.NewStatsReporter()
	if err != nil {
		log.Error(err, "Sync metrics reporter could not start")
		return err
	}

	r := newReconciler(mgr, *reporter, a.Tracker, a.ProcessExcluder, a.CMT)
	return add(mgr, r, a.Events)
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(
	mgr manager.Manager,
	reporter syncutil.Reporter,
	tracker *readiness.Tracker,
	processExcluder *process.Excluder,
	cmt *cmt.CacheManagerTracker,
) reconcile.Reconciler {
	return &ReconcileSync{
		reader:          mgr.GetCache(),
		scheme:          mgr.GetScheme(),
		log:             log,
		reporter:        reporter,
		tracker:         tracker,
		processExcluder: processExcluder,
		cmt: cmt,
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler, events <-chan event.GenericEvent) error {
	// Create a new controller
	c, err := controller.New("sync-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to the provided resource
	return c.Watch(
		&source.Channel{
			Source:         events,
			DestBufferSize: 1024,
		},
		handler.EnqueueRequestsFromMapFunc(util.EventPackerMapFunc()),
	)
}

var _ reconcile.Reconciler = &ReconcileSync{}

// ReconcileSync reconciles an arbitrary object described by Kind.
type ReconcileSync struct {
	reader client.Reader

	scheme          *runtime.Scheme
	log             logr.Logger
	reporter        syncutil.Reporter
	cmt *cmt.CacheManagerTracker
	tracker         *readiness.Tracker
	processExcluder *process.Excluder
}

// +kubebuilder:rbac:groups=constraints.gatekeeper.sh,resources=*,verbs=get;list;watch;create;update;patch;delete

// Reconcile reads that state of the cluster for an object and makes changes based on the state read
// and what is in the constraint.Spec.
func (r *ReconcileSync) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	timeStart := time.Now()

	gvk, unpackedRequest, err := util.UnpackRequest(request)
	if err != nil {
		// Unrecoverable, do not retry.
		// TODO(OREN) add metric
		log.Error(err, "unpacking request", "request", request)
		return reconcile.Result{}, nil
	}

	syncKey := r.cmt.SyncMetricsCache.GetSyncKey(unpackedRequest.Namespace, unpackedRequest.Name)
	reportMetrics := false
	defer func() {
		if reportMetrics {
			if err := r.reporter.ReportSyncDuration(time.Since(timeStart)); err != nil {
				log.Error(err, "failed to report sync duration")
			}

			r.cmt.SyncMetricsCache.ReportSync(&r.reporter, log)

			if err := r.reporter.ReportLastSync(); err != nil {
				log.Error(err, "failed to report last sync timestamp")
			}
		}
	}()

	instance := &unstructured.Unstructured{}
	instance.SetGroupVersionKind(gvk)

	if err := r.reader.Get(ctx, unpackedRequest.NamespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			// This is a deletion; remove the data
			instance.SetNamespace(unpackedRequest.Namespace)
			instance.SetName(unpackedRequest.Name)
			if _, err := r.cmt.RemoveData(ctx, instance); err != nil {
				return reconcile.Result{}, err
			}

			// cancel expectations
			t := r.tracker.ForData(instance.GroupVersionKind())
			t.CancelExpect(instance)

			r.cmt.SyncMetricsCache.DeleteObject(syncKey)
			reportMetrics = true
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// namespace is excluded from sync
	isExcludedNamespace, err := r.skipExcludedNamespace(instance)
	if err != nil {
		log.Error(err, "error while excluding namespaces")
	}

	if isExcludedNamespace {
		// cancel expectations
		t := r.tracker.ForData(instance.GroupVersionKind())
		t.CancelExpect(instance)
		return reconcile.Result{}, nil
	}

	if !instance.GetDeletionTimestamp().IsZero() {
		if _, err := r.cmt.RemoveData(ctx, instance); err != nil {
			return reconcile.Result{}, err
		}

		// cancel expectations
		t := r.tracker.ForData(instance.GroupVersionKind())
		t.CancelExpect(instance)

		r.cmt.SyncMetricsCache.DeleteObject(syncKey)
		reportMetrics = true
		return reconcile.Result{}, nil
	}

	r.log.V(logging.DebugLevel).Info(
		"data will be added",
		logging.ResourceAPIVersion, instance.GetAPIVersion(),
		logging.ResourceKind, instance.GetKind(),
		logging.ResourceNamespace, instance.GetNamespace(),
		logging.ResourceName, instance.GetName(),
	)

	if _, err := r.cmt.AddData(ctx, instance); err != nil {
		r.cmt.SyncMetricsCache.AddObject(syncKey, syncutil.Tags{
			Kind:   instance.GetKind(),
			Status: metrics.ErrorStatus,
		})
		reportMetrics = true

		return reconcile.Result{}, err
	}
	r.tracker.ForData(gvk).Observe(instance)
	log.V(1).Info("[readiness] observed data", "gvk", gvk, "namespace", instance.GetNamespace(), "name", instance.GetName())

	r.cmt.SyncMetricsCache.AddObject(syncKey, syncutil.Tags{
		Kind:   instance.GetKind(),
		Status: metrics.ActiveStatus,
	})

	r.cmt.SyncMetricsCache.AddKind(instance.GetKind())

	reportMetrics = true

	return reconcile.Result{}, nil
}

func (r *ReconcileSync) skipExcludedNamespace(obj *unstructured.Unstructured) (bool, error) {
	isNamespaceExcluded, err := r.processExcluder.IsNamespaceExcluded(process.Sync, obj)
	if err != nil {
		return false, err
	}

	return isNamespaceExcluded, err
}
