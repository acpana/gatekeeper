package cachemanager

import (
	"context"
	"testing"

	configv1alpha1 "github.com/open-policy-agent/gatekeeper/v3/apis/config/v1alpha1"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/fakes"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/syncutil"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/util"
	"github.com/open-policy-agent/gatekeeper/v3/test/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

var cfg *rest.Config

func TestMain(m *testing.M) {
	testutils.StartControlPlane(m, &cfg, 3)
}

// TestCacheManager_AddObject_RemoveObject tests that we can add/ remove objects in the cache.
func TestCacheManager_AddObject_RemoveObject(t *testing.T) {
	mgr, _ := testutils.SetupManager(t, cfg)
	opaClient := &fakes.FakeOpa{}

	tracker, err := readiness.SetupTracker(mgr, false, false, false)
	assert.NoError(t, err)

	processExcluder := process.Get()
	cm := NewCacheManager(opaClient, syncutil.NewMetricsCache(), tracker, processExcluder)
	ctx := context.Background()

	pod := fakes.Pod(
		fakes.WithNamespace("test-ns"),
		fakes.WithName("test-name"),
	)
	unstructuredPod, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	require.NoError(t, err)

	require.NoError(t, cm.AddObject(ctx, &unstructured.Unstructured{Object: unstructuredPod}))

	// test that pod is cache managed
	require.True(t, opaClient.HasGVK(pod.GroupVersionKind()))

	// now remove the object and verify it's removed
	require.NoError(t, cm.RemoveObject(ctx, &unstructured.Unstructured{Object: unstructuredPod}))
	require.False(t, opaClient.HasGVK(pod.GroupVersionKind()))
}

// TestCacheManager_processExclusion makes sure that we don't add objects that are process excluded
// and remove any objects that were previously not process excluded but have become so now.
func TestCacheManager_processExclusion(t *testing.T) {
	mgr, _ := testutils.SetupManager(t, cfg)
	opaClient := &fakes.FakeOpa{}

	tracker, err := readiness.SetupTracker(mgr, false, false, false)
	assert.NoError(t, err)

	// exclude "test-ns-excluded" namespace as excluded
	processExcluder := process.Get()
	processExcluder.Add([]configv1alpha1.MatchEntry{
		{
			ExcludedNamespaces: []util.Wildcard{"test-ns-excluded"},
			Processes:          []string{"sync"},
		},
	})

	cm := NewCacheManager(opaClient, syncutil.NewMetricsCache(), tracker, processExcluder)
	ctx := context.Background()

	pod := fakes.Pod(
		fakes.WithNamespace("test-ns-excluded"),
		fakes.WithName("test-name"),
	)
	unstructuredPod, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	require.NoError(t, err)
	require.NoError(t, cm.AddObject(ctx, &unstructured.Unstructured{Object: unstructuredPod}))

	// test that pod from excluded namespace is not cache managed
	require.False(t, opaClient.HasGVK(pod.GroupVersionKind()))

	// now add a new pod that IS NOT name space excluded at the time
	// of addition but becomes exlcluded later.
	pod2 := fakes.Pod(
		fakes.WithNamespace("test-ns-excluded-2"),
		fakes.WithName("test-name-2"),
	)
	unstructuredPod2, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod2)
	require.NoError(t, err)
	require.NoError(t, cm.AddObject(ctx, &unstructured.Unstructured{Object: unstructuredPod2}))

	// test that pod is cache managed
	require.True(t, opaClient.HasGVK(pod2.GroupVersionKind()))

	// now namespace exclude and attempt to re-add, this sequence should
	// remove the object from the cache altogether.
	processExcluder.Add([]configv1alpha1.MatchEntry{
		{
			ExcludedNamespaces: []util.Wildcard{"test-ns-excluded-2"},
			Processes:          []string{"sync"},
		},
	})
	require.NoError(t, cm.AddObject(ctx, &unstructured.Unstructured{Object: unstructuredPod2}))

	// test that pod is now NOT cache managed
	require.False(t, opaClient.HasGVK(pod2.GroupVersionKind()))
}
