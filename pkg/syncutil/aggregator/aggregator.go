package aggregator

import (
	"fmt"
	gosync "sync"

	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Key struct {
	Source string
	ID     string
}

func NewGVKAggregator(allowedKeyKinds []string) *GVKAgreggator {
	return &GVKAgreggator{
		store:        make(map[Key]map[schema.GroupVersionKind]struct{}),
		reverseStore: make(map[schema.GroupVersionKind]map[Key]struct{}),
	}
}

// todo comments.
type GVKAgreggator struct {
	mu gosync.RWMutex

	store        map[Key]map[schema.GroupVersionKind]struct{}
	reverseStore map[schema.GroupVersionKind]map[Key]struct{}
}

func (b *GVKAgreggator) ListGVKs() []schema.GroupVersionKind {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Keys(b.reverseStore)
}

func (b *GVKAgreggator) Remove(k Key) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	gvks, found := b.store[k]
	if !found {
		return nil
	}

	for gvk := range gvks {
		keySet, found := b.reverseStore[gvk]
		if !found {
			// this should not happen if we keep the two maps are well defined
			// but let's be defensive nonetheless

			return fmt.Errorf("internal aggregator error: gvks stores are corrupted for key: %s", k)
		}

		delete(keySet, k)

		b.reverseStore[gvk] = keySet
	}

	delete(b.store, k)
	return nil
}

func (b *GVKAgreggator) Upsert(k Key, gvks []schema.GroupVersionKind) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.store[k]; !ok {
		b.store[k] = make(map[schema.GroupVersionKind]struct{})
	}

	for _, gvk := range gvks {
		b.store[k][gvk] = struct{}{}
		if _, found := b.reverseStore[gvk]; !found {
			b.reverseStore[gvk] = make(map[Key]struct{})
		}
		b.reverseStore[gvk][k] = struct{}{}
	}
}
