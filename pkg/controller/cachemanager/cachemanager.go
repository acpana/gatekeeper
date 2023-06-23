package cachemanager

import (
	"context"
	"fmt"
	"time"

	"github.com/open-policy-agent/gatekeeper/v3/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/syncutil"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/syncutil/aggregator"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/target"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/watch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("cache-manager")

type CacheManagerConfig struct {
	Opa              syncutil.OpaDataClient
	SyncMetricsCache *syncutil.MetricsCache
	Tracker          *readiness.Tracker
	ProcessExcluder  *process.Excluder
	Registrar        *watch.Registrar
	WatchedSet       *watch.Set
	GVKAggregator    *aggregator.GVKAgreggator
	Reader           client.Reader
}

type CacheManager struct {
	opa              syncutil.OpaDataClient
	syncMetricsCache *syncutil.MetricsCache
	tracker          *readiness.Tracker
	processExcluder  *process.Excluder
	Registrar        *watch.Registrar
	watchedSet       *watch.Set
	gvkToWatchAgg    *aggregator.GVKAgreggator
	gvkToRemoveAgg   *aggregator.GVKAgreggator
	replayErrChan    chan error
	replayTicker     time.Ticker
	reader           client.Reader
}

func NewCacheManager(ctx context.Context, config *CacheManagerConfig) *CacheManager {
	cm := &CacheManager{
		opa:              config.Opa,
		syncMetricsCache: config.SyncMetricsCache,
		tracker:          config.Tracker,
		processExcluder:  config.ProcessExcluder,
		Registrar:        config.Registrar,
		watchedSet:       config.WatchedSet,
		reader:           config.Reader,
	}

	cm.gvkToWatchAgg = aggregator.NewGVKAggregator()
	cm.gvkToRemoveAgg = aggregator.NewGVKAggregator()
	cm.replayTicker = *time.NewTicker(3 * time.Second)

	if ctx == nil {
		ctx = context.TODO()
	}

	go cm.cacheCleanupProcess(ctx)

	return cm
}

func (c *CacheManager) WatchGVKsToSync(ctx context.Context, newGVKs []schema.GroupVersionKind, newExcluder *process.Excluder, syncSourceType, syncSourceName string) error {
	netNewGVKs := []schema.GroupVersionKind{}
	for _, gvk := range newGVKs {
		if !c.gvkToWatchAgg.IsPresent(gvk) {
			netNewGVKs = append(netNewGVKs, gvk)
		}
	}

	opKey := aggregator.Key{Source: syncSourceType, ID: syncSourceName}
	currentGVKsForKey := c.gvkToWatchAgg.List(opKey)

	if len(newGVKs) == 0 {
		// we are not syncing anything for this key anymore
		if err := c.gvkToWatchAgg.Remove(opKey); err != nil {
			return fmt.Errorf("internal error removing gvks for aggregation: %w", err)
		}
	} else {
		if err := c.gvkToWatchAgg.Upsert(opKey, newGVKs); err != nil {
			return fmt.Errorf("internal error upserting gvks for aggregation: %w", err)
		}
	}

	gvksToDelete := getGVKsToDelete(newGVKs, currentGVKsForKey)
	if len(gvksToDelete) > 0 {
		// Remove expectations for resources we no longer watch.
		for _, gvk := range gvksToDelete {
			c.watchedSet.Remove(gvk)
			c.tracker.CancelData(gvk)
		}

		if err := c.gvkToRemoveAgg.Upsert(opKey, gvksToDelete); err != nil {
			return fmt.Errorf("internal error marking which gvks to unsync: %w", err)
		}
	}

	// the new watch set is the current watch set, less the gvks to delete
	// plus the net new gvks we are adding.
	newGvkWatchSet := watch.NewSet()
	newGvkWatchSet.AddSet(c.watchedSet)
	for _, gvk := range netNewGVKs {
		newGvkWatchSet.Add(gvk)
	}

	// If the watch set has not changed, we're done here.
	if c.watchedSet.Equals(newGvkWatchSet) && (newExcluder != nil && c.processExcluder.Equals(newExcluder)) {
		return nil
	}

	// Start watching the newly added gvks set
	var innerError error
	c.watchedSet.Replace(newGvkWatchSet, func() {
		// swapping with the new excluder
		if newExcluder != nil {
			c.ReplaceExcluder(newExcluder)
		}

		// *Note the following steps are not transactional with respect to admission control*

		// Important: dynamic watches update must happen *after* updating our watchSet.
		// Otherwise, the sync controller will drop events for the newly watched kinds.
		// Defer error handling so object re-sync happens even if the watch is hard
		// errored due to a missing GVK in the watch set.
		innerError = c.Registrar.ReplaceWatch(ctx, newGvkWatchSet.Items())
	})
	if innerError != nil {
		return innerError
	}

	return nil
}

// returns GVKs that are in currentGVKsForKey but not in newGVKs.
func getGVKsToDelete(newGVKs []schema.GroupVersionKind, currentGVKsForKey map[schema.GroupVersionKind]struct{}) []schema.GroupVersionKind {
	newGVKSet := make(map[schema.GroupVersionKind]struct{})
	for _, gvk := range newGVKs {
		newGVKSet[gvk] = struct{}{}
	}

	var toDelete []schema.GroupVersionKind
	for gvk := range currentGVKsForKey {
		if _, found := newGVKSet[gvk]; !found {
			toDelete = append(toDelete, gvk)
		}
	}

	return toDelete
}

func (c *CacheManager) AddObject(ctx context.Context, instance *unstructured.Unstructured) error {
	isNamespaceExcluded, err := c.processExcluder.IsNamespaceExcluded(process.Sync, instance)
	if err != nil {
		return fmt.Errorf("error while excluding namespaces: %w", err)
	}

	// bail because it means we should not be
	// syncing this gvk
	if isNamespaceExcluded {
		c.tracker.ForData(instance.GroupVersionKind()).CancelExpect(instance)
		return nil
	}

	syncKey := syncutil.GetKeyForSyncMetrics(instance.GetNamespace(), instance.GetName())
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

	c.tracker.ForData(instance.GroupVersionKind()).Observe(instance)

	c.syncMetricsCache.AddObject(syncKey, syncutil.Tags{
		Kind:   instance.GetKind(),
		Status: metrics.ActiveStatus,
	})
	c.syncMetricsCache.AddKind(instance.GetKind())

	return err
}

func (c *CacheManager) RemoveObject(ctx context.Context, instance *unstructured.Unstructured) error {
	if _, err := c.opa.RemoveData(ctx, instance); err != nil {
		return err
	}

	// only delete from metrics map if the data removal was succcesful
	c.syncMetricsCache.DeleteObject(syncutil.GetKeyForSyncMetrics(instance.GetNamespace(), instance.GetName()))
	c.tracker.ForData(instance.GroupVersionKind()).CancelExpect(instance)

	return nil
}

func (c *CacheManager) WipeData(ctx context.Context) error {
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

func (c *CacheManager) ReplaceExcluder(p *process.Excluder) {
	c.processExcluder.Replace(p)
}

// replayData replays all watched and cached data into Opa following a config set change.
// In the future we can rework this to avoid the full opa data cache wipe.
func (c *CacheManager) replayData(ctx context.Context, needsReplaySet *watch.Set, reader client.Reader) error {
	if needsReplaySet == nil {
		return nil
	}

	for _, gvk := range needsReplaySet.Items() {
		u := &unstructured.UnstructuredList{}
		u.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   gvk.Group,
			Version: gvk.Version,
			Kind:    gvk.Kind + "List",
		})

		err := reader.List(ctx, u)
		if err != nil {
			return fmt.Errorf("replaying data for %+v: %w", gvk, err)
		}

		defer c.ReportSyncMetrics()

		for i := range u.Items {
			if err := c.AddObject(ctx, &u.Items[i]); err != nil {
				return fmt.Errorf("adding data for %+v: %w", gvk, err)
			}
		}
		needsReplaySet.Remove(gvk)
	}

	return nil
}

// cacheCleanupProcess performs a conditional wipe followed by a replay if necsessary.
func (c *CacheManager) cacheCleanupProcess(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.replayTicker.C:
			gvksToDelete := c.gvkToRemoveAgg.ListAllGVKsWithKeys()
			if len(gvksToDelete) > 0 {
				c.gvkToRemoveAgg.Clear()
				// "checkpoint save" what needs to be replayed
				gvksToReplaySet := watch.NewSet()
				for _, gvk := range c.gvkToWatchAgg.ListAllGVKs() {
					gvksToReplaySet.Add(gvk)
				}

				if err := c.WipeData(ctx); err != nil {
					log.Error(err, "internal: error wiping cache")
				} else {
					// previously replayData() would be inherently triggered by
					// the reconciler event's and we would effectively get retries for free.
					// but now we have to use the ticker's periodicity instead.
					// So need to add all these gvksToDelete back to try to replayData again next time.
					err := c.replayData(ctx, gvksToReplaySet, c.reader)
					if err != nil {
						log.Error(err, "internal: error replay cache data")

						for key, gvks := range gvksToDelete {
							gvksList := []schema.GroupVersionKind{}
							for gvk := range gvks {
								gvksList = append(gvksList, gvk)
							}
							c.gvkToRemoveAgg.Upsert(key, gvksList)
						}
					}
				}
			}
		}
	}
}
