package cachemanager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/open-policy-agent/frameworks/constraint/pkg/types"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/cachemanager/aggregator"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/syncutil"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/target"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/watch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	log     = logf.Log.WithName("cache-manager")
	backoff = wait.Backoff{
		Duration: time.Second,
		Factor:   2,
		Jitter:   0.1,
		Steps:    3,
	}
)

type Config struct {
	Opa              OpaDataClient
	SyncMetricsCache *syncutil.MetricsCache
	Tracker          *readiness.Tracker
	ProcessExcluder  *process.Excluder
	Registrar        *watch.Registrar
	WatchedSet       *watch.Set
	GVKAggregator    *aggregator.GVKAgreggator
	Reader           client.Reader
}

type CacheManager struct {
	watchedSet            *watch.Set
	processExcluder       *process.Excluder
	gvksToSync            *aggregator.GVKAgreggator
	needToList            bool
	gvksToDeleteFromCache *watch.Set
	excluderChanged       bool
	// mu guards access to any of the fields above
	mu sync.RWMutex

	opa                        OpaDataClient
	syncMetricsCache           *syncutil.MetricsCache
	tracker                    *readiness.Tracker
	registrar                  *watch.Registrar
	backgroundManagementTicker time.Ticker
	reader                     client.Reader

	// stopChan is used to stop any list operations still in progress
	stopChan chan bool
}

// OpaDataClient is an interface for caching data.
type OpaDataClient interface {
	AddData(ctx context.Context, data interface{}) (*types.Responses, error)
	RemoveData(ctx context.Context, data interface{}) (*types.Responses, error)
}

func NewCacheManager(config *Config) (*CacheManager, error) {
	if config.WatchedSet == nil {
		return nil, fmt.Errorf("watchedSet must be non-nil")
	}
	if config.Registrar == nil {
		return nil, fmt.Errorf("registrar must be non-nil")
	}
	if config.ProcessExcluder == nil {
		return nil, fmt.Errorf("processExcluder must be non-nil")
	}
	if config.Tracker == nil {
		return nil, fmt.Errorf("tracker must be non-nil")
	}
	if config.Reader == nil {
		return nil, fmt.Errorf("reader must be non-nil")
	}

	if config.GVKAggregator == nil {
		config.GVKAggregator = aggregator.NewGVKAggregator()
	}

	cm := &CacheManager{
		opa:                        config.Opa,
		syncMetricsCache:           config.SyncMetricsCache,
		tracker:                    config.Tracker,
		processExcluder:            config.ProcessExcluder,
		registrar:                  config.Registrar,
		watchedSet:                 config.WatchedSet,
		reader:                     config.Reader,
		gvksToSync:                 config.GVKAggregator,
		backgroundManagementTicker: *time.NewTicker(3 * time.Second),
		gvksToDeleteFromCache:      watch.NewSet(),
		stopChan:                   make(chan bool, 1),
	}

	return cm, nil
}

func (c *CacheManager) Start(ctx context.Context) error {
	go c.manageCache(ctx)

	<-ctx.Done()
	return nil
}

// AddSource adjusts the watched set of gvks according to the newGVKs passed in
// for a given sourceKey.
// It errors out if there is an issue adding the Key internally or replacing the watches.
// Consumers are encouraged to retry on error.
func (c *CacheManager) AddSource(ctx context.Context, sourceKey aggregator.Key, newGVKs []schema.GroupVersionKind) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.gvksToSync.Upsert(sourceKey, newGVKs); err != nil {
		return fmt.Errorf("internal error adding source: %w", err)
	}
	// as a result of upserting the new gvks for the source key, some gvks
	// may become unreferenced and need to be deleted; this will be handled async
	// in the manageCache loop.

	// make changes to the watches
	if err := c.replaceWatchSet(ctx); err != nil {
		return fmt.Errorf("error watching new gvks: %w", err)
	}

	return nil
}

// replaceWatchSet looks at the specifiedGVKs and makes changes to the registrar's watch set.
// assumes caller has lock.
// replaceWatchSet may error out and that error is retryable.
func (c *CacheManager) replaceWatchSet(ctx context.Context) error {
	newWatchSet := watch.NewSet()
	newWatchSet.Add(c.gvksToSync.GVKs()...)

	// record any gvks that need to be deleted from the opa cache.
	c.gvksToDeleteFromCache.AddSet(c.watchedSet.Difference(newWatchSet))

	var innerError error
	c.watchedSet.Replace(newWatchSet, func() {
		// *Note the following steps are not transactional with respect to admission control

		// Important: dynamic watches update must happen *after* updating our watchSet.
		// Otherwise, the sync controller will drop events for the newly watched kinds.
		innerError = c.registrar.ReplaceWatch(ctx, newWatchSet.Items())
	})

	return innerError
}

// RemoveSource removes the watches of the GVKs for a given aggregator.Key.
// It errors out if there is an issue removing the Key internally or replacing the watches.
// Consumers are encouraged to retry on error.
func (c *CacheManager) RemoveSource(ctx context.Context, sourceKey aggregator.Key) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.gvksToSync.Remove(sourceKey); err != nil {
		return fmt.Errorf("internal error removing source: %w", err)
	}

	// make changes to the watches
	if err := c.replaceWatchSet(ctx); err != nil {
		return fmt.Errorf("error removing watches for source %v: %w", sourceKey, err)
	}

	return nil
}

// ExcludeProcesses swaps the current process excluder with the new *process.Excluder.
// It's a no-op if the two excluders are equal.
func (c *CacheManager) ExcludeProcesses(newExcluder *process.Excluder) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.processExcluder.Equals(newExcluder) {
		return
	}

	c.processExcluder.Replace(newExcluder)
	// there is a new excluder which means we need to schedule a wipe for any
	// previously watched GVKs to be re-added to get a chance to be evaluated
	// for this new process excluder.
	c.excluderChanged = true
}

func (c *CacheManager) AddObject(ctx context.Context, instance *unstructured.Unstructured) error {
	gvk := instance.GroupVersionKind()

	isNamespaceExcluded, err := c.processExcluder.IsNamespaceExcluded(process.Sync, instance)
	if err != nil {
		return fmt.Errorf("error while excluding namespaces for gvk: %+v: %w", gvk, err)
	}

	// bail because it means we should not be
	// syncing this gvk's objects as it is namespace excluded.
	if isNamespaceExcluded {
		c.tracker.ForData(instance.GroupVersionKind()).CancelExpect(instance)
		return nil
	}

	syncKey := syncutil.GetKeyForSyncMetrics(instance.GetNamespace(), instance.GetName())
	if c.watchedSet.Contains(gvk) {
		_, err = c.opa.AddData(ctx, instance)
		if err != nil {
			c.syncMetricsCache.AddObject(
				syncKey,
				syncutil.Tags{
					Kind:   instance.GetKind(),
					Status: metrics.ErrorStatus,
				},
			)

			return err
		}
	}

	c.tracker.ForData(instance.GroupVersionKind()).Observe(instance)

	c.syncMetricsCache.AddObject(syncKey, syncutil.Tags{
		Kind:   instance.GetKind(),
		Status: metrics.ActiveStatus,
	})
	c.syncMetricsCache.AddKind(instance.GetKind())

	return err
}

func (c *CacheManager) RemoveObject(ctx context.Context, instance *unstructured.Unstructured) error {
	gvk := instance.GroupVersionKind()

	if c.watchedSet.Contains(gvk) {
		if _, err := c.opa.RemoveData(ctx, instance); err != nil {
			return err
		}
	}

	// only delete from metrics map if the data removal was succcesful
	c.syncMetricsCache.DeleteObject(syncutil.GetKeyForSyncMetrics(instance.GetNamespace(), instance.GetName()))
	c.tracker.ForData(instance.GroupVersionKind()).CancelExpect(instance)

	return nil
}

func (c *CacheManager) wipeData(ctx context.Context) error {
	if _, err := c.opa.RemoveData(ctx, target.WipeData()); err != nil {
		return err
	}

	// reset sync cache before sending the metric
	c.syncMetricsCache.ResetCache()
	c.syncMetricsCache.ReportSync()

	return nil
}

func (c *CacheManager) ReportSyncMetrics() {
	c.syncMetricsCache.ReportSync()
}

func (c *CacheManager) syncGVK(ctx context.Context, gvk schema.GroupVersionKind) error {
	u := &unstructured.UnstructuredList{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	})

	var err error
	c.mu.Lock()
	if !c.watchedSet.Contains(gvk) {
		// we are not actually watching this gvk anymore
		// so don't list instances for it.
		err = nil
	} else {
		err = c.reader.List(ctx, u)
	}
	c.mu.Unlock()

	if err != nil {
		return fmt.Errorf("replaying data for %+v: %w", gvk, err)
	}

	for i := range u.Items {
		if err := c.AddObject(ctx, &u.Items[i]); err != nil {
			return fmt.Errorf("adding data for %+v: %w", gvk, err)
		}
	}

	return nil
}

func (c *CacheManager) manageCache(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(c.stopChan)
			return
		case <-c.backgroundManagementTicker.C:
			c.mu.Lock()
			c.wipeCacheIfNeeded(ctx)

			// spin up new goroutines to relist gvks as there has been a wipe
			if c.needToList {
				// stop any goroutines that were relisting before
				// as we may no longer be interested in those gvks
				c.stopChan <- true

				// assume all gvks need to be relisted
				gvksToRelist := c.gvksToSync.GVKs()

				// clean state
				c.needToList = false
				c.stopChan = make(chan bool, 1)

				go c.replayGVKs(ctx, gvksToRelist)
			}
			c.mu.Unlock()
		}
	}
}

func (c *CacheManager) replayGVKs(ctx context.Context, gvksToRelist []schema.GroupVersionKind) {
	gvksSet := watch.NewSet()
	gvksSet.Add(gvksToRelist...)

	for gvksSet.Size() != 0 {
		gvkItems := gvksSet.Items()

		for _, gvk := range gvkItems {
			select {
			case <-ctx.Done():
				return
			case <-c.stopChan:
				return
			default:
				operation := func() (bool, error) {
					if err := c.syncGVK(ctx, gvk); err != nil {
						return false, err
					}
					return true, nil
				}

				if err := wait.ExponentialBackoff(backoff, operation); err != nil {
					log.Error(err, "internal: error listings gvk cache data", "gvk", gvk)
				} else {
					gvksSet.Remove(gvk)
				}
			}
		}

		c.ReportSyncMetrics()
	}
}

// wipeCacheIfNeeded performs a cache wipe if there are any gvks needing to be removed
// from the cache or if the excluder has changed. It also marks which gvks need to be
// re listed again in the opa cache after the wipe.
// assumes the caller has lock.
func (c *CacheManager) wipeCacheIfNeeded(ctx context.Context) {
	// remove any gvks not needing to be synced anymore
	// or re evaluate all if the excluder changed.
	if c.gvksToDeleteFromCache.Size() > 0 || c.excluderChanged {
		if err := c.wipeData(ctx); err != nil {
			log.Error(err, "internal: error wiping cache")
		} else {
			c.gvksToDeleteFromCache = watch.NewSet()
			c.excluderChanged = false
			c.needToList = true
		}
	}
}
