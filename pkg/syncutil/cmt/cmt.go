package cmt

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/open-policy-agent/frameworks/constraint/pkg/types"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/syncutil"

	//	syncc "github.com/open-policy-agent/gatekeeper/pkg/controller/sync"
	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type CacheManagerTracker struct {
	// mutex


	opa              syncutil.OpaDataClient
	SyncMetricsCache *syncutil.MetricsCache

	// GVKAggregator
}

func NewCacheManager(opa syncutil.OpaDataClient, syncMetricsCache *syncutil.MetricsCache) *CacheManagerTracker {
	return &CacheManagerTracker{
		opa:              opa,
		SyncMetricsCache: syncMetricsCache,
	}
}

func (c *CacheManagerTracker) WipeCache(ctx context.Context, log logr.Logger) error {
	if _, err := c.opa.RemoveData(ctx, target.WipeData()); err != nil {
		return err
	}
	c.SyncMetricsCache.ResetCache()
	c.SyncMetricsCache.ReportSync(&syncutil.Reporter{}, log)

	return nil
}

func (c *CacheManagerTracker) ReplayData(ctx context.Context, reader client.Reader, needsReplay *watch.Set, processExcluder *process.Excluder, log logr.Logger) error {
	if needsReplay == nil {
		return nil
	}
	for _, gvk := range needsReplay.Items() {
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

		defer c.SyncMetricsCache.ReportSync(&syncutil.Reporter{}, log)

		for i := range u.Items {
			syncKey := c.SyncMetricsCache.GetSyncKey(u.Items[i].GetNamespace(), u.Items[i].GetName())

			isExcludedNamespace, err := processExcluder.IsNamespaceExcluded(process.Sync, &u.Items[i])
			if err != nil {
				return fmt.Errorf("checking if namespace is excluded: %w", err)
			}
			if isExcludedNamespace {
				continue
			}

			if _, err := c.opa.AddData(ctx, &u.Items[i]); err != nil {
				return fmt.Errorf("adding data for %+v: %w", gvk, err)
			}
			c.SyncMetricsCache.AddObject(syncKey, syncutil.Tags{
				Kind:   u.Items[i].GetKind(),
				Status: metrics.ActiveStatus,
			})
		}
	}
	return nil
}

func  (c *CacheManagerTracker) AddData(ctx context.Context, data interface{}) (*types.Responses, error) {
	return c.opa.AddData(ctx, data)
}

func  (c *CacheManagerTracker) RemoveData(ctx context.Context, data interface{}) (*types.Responses, error) {
	return c.opa.RemoveData(ctx, data)
}
