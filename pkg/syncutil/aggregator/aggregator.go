package aggregator

import (
	"fmt"
	gosync "sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Key defines a type, identifier tuple to store
// in the GVKAggregator.
type Key struct {
	// Source specifies the type or where this Key comes from.
	Source string
	// ID specifies the name of the type that this Key is.
	ID string
}

func NewGVKAggregator() *GVKAgreggator {
	return &GVKAgreggator{
		store:        make(map[Key]map[schema.GroupVersionKind]struct{}),
		reverseStore: make(map[schema.GroupVersionKind]map[Key]struct{}),
	}
}

type GVKAgreggator struct {
	mu gosync.RWMutex

	// store keeps track of associations between a Key type and a set of GVKs.
	store map[Key]map[schema.GroupVersionKind]struct{}
	// reverseStore keeps track of associations between a GVK and the set of Key types
	// that references the GVK in the store map above. It is useful to have reverseStore
	// in order for IsPresent() and ListGVKs() to run in optimal time.
	reverseStore map[schema.GroupVersionKind]map[Key]struct{}
}

// IsPresent returns true if the given gvk is present in the GVKAggregator.
func (b *GVKAgreggator) IsPresent(gvk schema.GroupVersionKind) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, found := b.reverseStore[gvk]
	return found
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
			// this should not happen if we keep the two maps well defined
			// but let's be defensive nonetheless.
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
