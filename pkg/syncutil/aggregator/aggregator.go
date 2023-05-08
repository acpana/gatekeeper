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

// GVKAgreggator is an implementation of a bi directional map
// that stores associations between Key K and GVKs and reverse associations
// between GVK g and Keys.
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

// Remove deletes the any associations that Key k has in the GVKAggregator.
// For any GVK in the association k --> [GVKs], we also delete any associations
// between the GVK and the Key k stored in the reverse map.
func (b *GVKAgreggator) Remove(k Key) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	gvks, found := b.store[k]
	if !found {
		return nil
	}

	if err := b.pruneReverseStore(gvks, k); err != nil {
		return err
	}

	delete(b.store, k)
	return nil
}

// Upsert stores an association between Key k and the list of GVKs
// and also the reverse associatoin between each GVK passed in and Key k.
// Any old associations are dropped, unless they are included in the new list of
// GVKs.
// It errors out if there is an internal issue with remove the reverse Key links
// for any GVKs that are being dropped as part of this Upsert call.
func (b *GVKAgreggator) Upsert(k Key, gvks []schema.GroupVersionKind) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// if any oldOGVKs exist, we deal with 3 flavors of GVKs:
	// - A. GVKs that are net new
	// - B. GVKs that are both new and old (intersection)
	// - C. GVKs that shouold be marked to be pruned because they are no longer
	// referenced by Key k.
	//
	// It is sufficient in this block to determine C and prune GVKs both from
	// b.store and b.reverseStore. Flavors A & B will be taken care of
	// in the for loop below.
	if oldGvks, found := b.store[k]; found {
		gvksToPrune := b.markOldGVKsToPrune(gvks, oldGvks)

		// prune store
		for gkvToPrune := range gvksToPrune {
			delete(oldGvks, gkvToPrune)
		}

		if err := b.pruneReverseStore(gvksToPrune, k); err != nil {
			return fmt.Errorf("failed to prune entries: %w", err)
		}

		if len(oldGvks) == 0 {
			b.store[k] = make(map[schema.GroupVersionKind]struct{})
		} else {
			b.store[k] = oldGvks
		}
	} else {
		b.store[k] = make(map[schema.GroupVersionKind]struct{})
	}

	// Flavor A: add new GVKs
	// Flavor B: no op for interesection GVKs since they are already present in the stores
	for _, gvk := range gvks {
		b.store[k][gvk] = struct{}{}
		if _, found := b.reverseStore[gvk]; !found {
			b.reverseStore[gvk] = make(map[Key]struct{})
		}
		b.reverseStore[gvk][k] = struct{}{}
	}

	return nil
}

func (b *GVKAgreggator) markOldGVKsToPrune(newGVKs []schema.GroupVersionKind, oldGVKs map[schema.GroupVersionKind]struct{}) map[schema.GroupVersionKind]struct{} {
	// deep copy oldGVKs
	oldGVKsCpy := make(map[schema.GroupVersionKind]struct{}, len(oldGVKs))
	for k, v := range oldGVKs {
		oldGVKsCpy[k] = v
	}

	// intersection: exclude the oldGVKs that are present in the new GVKs as well.
	for _, newGVK := range newGVKs {
		// don't prune what is being already added
		delete(oldGVKsCpy, newGVK)
	}

	return oldGVKsCpy
}

func (b *GVKAgreggator) pruneReverseStore(gvks map[schema.GroupVersionKind]struct{}, k Key) error {
	for gvk := range gvks {
		keySet, found := b.reverseStore[gvk]
		if !found || len(keySet) == 0 {
			// this should not happen if we keep the two maps well defined
			// but let's be defensive nonetheless.
			return fmt.Errorf("internal aggregator error: gvks stores are corrupted for key: %s", k)
		}

		delete(keySet, k)

		// remove GVK from reverseStore if it's not referenced by any Ket anymore.
		if len(keySet) == 0 {
			delete(b.reverseStore, gvk)
		} else {
			b.reverseStore[gvk] = keySet
		}
	}

	return nil
}
