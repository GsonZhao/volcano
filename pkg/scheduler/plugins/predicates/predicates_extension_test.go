/*
Copyright 2026 The Volcano Authors.

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

package predicates

import (
	"context"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/dynamic-resource-allocation/structured"
	fwk "k8s.io/kube-scheduler/framework"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/k8s"
)

type interfaceStatus struct {
	supported bool
	ignored   bool
}

func TestUpstreamPluginExtensions(t *testing.T) {
	// Initialize predicates plugin to get instances of VolumeBinding and DynamicResources
	ResetVolumeBindingPluginForTest()
	pp := New(nil).(*PredicatesPlugin)
	pp.enabledPredicates.volumeBindingEnable = true
	pp.enabledPredicates.dynamicResourceAllocationEnable = true

	nodeMap := map[string]fwk.NodeInfo{}
	client := k8sfake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	pp.Handle = k8s.NewFramework(
		nodeMap,
		k8s.WithClientSet(client),
		k8s.WithInformerFactory(informerFactory),
	)
	pp.InitPlugin()

	// Retrieve the instantiated plugin wrappers
	vbPlugin, exists := pp.FilterPlugins["VolumeBinding"]
	if !exists {
		t.Fatalf("VolumeBinding plugin was not initialized")
	}

	draPlugin, exists := pp.FilterPlugins["DynamicResources"]
	if !exists {
		t.Fatalf("DynamicResources plugin was not initialized")
	}

	// 1. Declare all known scheduling framework extension interfaces
	interfaces := map[string]reflect.Type{
		"PreFilterPlugin":  reflect.TypeOf((*fwk.PreFilterPlugin)(nil)).Elem(),
		"FilterPlugin":     reflect.TypeOf((*fwk.FilterPlugin)(nil)).Elem(),
		"PostFilterPlugin": reflect.TypeOf((*fwk.PostFilterPlugin)(nil)).Elem(),
		"PreScorePlugin":   reflect.TypeOf((*fwk.PreScorePlugin)(nil)).Elem(),
		"ScorePlugin":      reflect.TypeOf((*fwk.ScorePlugin)(nil)).Elem(),
		"ReservePlugin":    reflect.TypeOf((*fwk.ReservePlugin)(nil)).Elem(),
		"PermitPlugin":     reflect.TypeOf((*fwk.PermitPlugin)(nil)).Elem(),
		"PreBindPlugin":    reflect.TypeOf((*fwk.PreBindPlugin)(nil)).Elem(),
		"BindPlugin":       reflect.TypeOf((*fwk.BindPlugin)(nil)).Elem(),
		"PostBindPlugin":   reflect.TypeOf((*fwk.PostBindPlugin)(nil)).Elem(),
		"QueueSortPlugin":  reflect.TypeOf((*fwk.QueueSortPlugin)(nil)).Elem(),
		"PreEnqueuePlugin": reflect.TypeOf((*fwk.PreEnqueuePlugin)(nil)).Elem(),
		"SignPlugin":       reflect.TypeOf((*fwk.SignPlugin)(nil)).Elem(),
	}

	// 2. Define static whitelists for each plugin.
	// Every interface implemented by the plugin must be marked as supported or ignored.
	vbWhitelist := map[string]interfaceStatus{
		"PreFilterPlugin": {supported: true},
		"FilterPlugin":    {supported: true},
		"PreScorePlugin":  {supported: true},
		"ScorePlugin":     {supported: true},
		"ReservePlugin":   {supported: true},
		"PreBindPlugin":   {supported: true},
		"SignPlugin":      {ignored: true},
	}

	draWhitelist := map[string]interfaceStatus{
		"PreFilterPlugin":  {supported: true},
		"FilterPlugin":     {supported: true},
		"PostFilterPlugin": {ignored: true},
		"ScorePlugin":      {supported: true},
		"ReservePlugin":    {supported: true},
		"PreBindPlugin":    {supported: true},
		"PreEnqueuePlugin": {ignored: true},
		"SignPlugin":       {ignored: true},
	}

	// 3. Programmatically inspect VolumeBinding using reflection
	t.Run("VolumeBinding Extensions Guardrail", func(t *testing.T) {
		inspectPluginExtensions(t, vbPlugin, interfaces, vbWhitelist)
	})

	// 4. Programmatically inspect DynamicResources using reflection
	t.Run("DynamicResources Extensions Guardrail", func(t *testing.T) {
		inspectPluginExtensions(t, draPlugin, interfaces, draWhitelist)
	})
}

func inspectPluginExtensions(
	t *testing.T,
	plugin interface{},
	interfaces map[string]reflect.Type,
	whitelist map[string]interfaceStatus,
) {
	pluginType := reflect.TypeOf(plugin)

	// Check each registered interface
	for name, interfaceType := range interfaces {
		implements := pluginType.Implements(interfaceType)
		status, inWhitelist := whitelist[name]

		if implements {
			if !inWhitelist {
				t.Errorf("Upstream plugin %T implements framework interface %s which is NOT in the whitelist. Please verify if Volcano needs to adapt and support this interface, and update the whitelist accordingly.", plugin, name)
			} else if !status.supported && !status.ignored {
				t.Errorf("Upstream plugin %T implements framework interface %s, but its status in the whitelist is neither supported nor ignored.", plugin, name)
			}
		} else {
			if inWhitelist && status.supported {
				t.Errorf("Upstream plugin %T NO LONGER implements framework interface %s (marked as supported). This indicates a breaking change in the upstream dependency!", plugin, name)
			}
		}
	}
}

// mockScorePlugin implements only fwk.ScorePlugin (no PreScorePlugin)
type mockScorePlugin struct {
	name string
}

func (m *mockScorePlugin) Name() string {
	return m.name
}

func (m *mockScorePlugin) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	return 10, nil
}

func (m *mockScorePlugin) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// mockPreScoreAndScorePlugin implements both fwk.PreScorePlugin and fwk.ScorePlugin
type mockPreScoreAndScorePlugin struct {
	*mockScorePlugin
	preScoreCalled bool
	preScoreStatus *fwk.Status
}

func (m *mockPreScoreAndScorePlugin) PreScore(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	m.preScoreCalled = true
	return m.preScoreStatus
}

func TestScorePluginAdapter_PreScore(t *testing.T) {
	t.Run("ScorePlugin without PreScorePlugin", func(t *testing.T) {
		mock := &mockScorePlugin{name: "mock-plugin"}
		adapter := &scorePluginAdapter{ScorePlugin: mock}
		status := adapter.PreScore(context.Background(), nil, nil, nil)
		if status != nil {
			t.Errorf("Expected nil status, got %v", status)
		}
	})

	t.Run("ScorePlugin with PreScorePlugin", func(t *testing.T) {
		expectedStatus := fwk.NewStatus(fwk.Success, "custom status")
		mock := &mockPreScoreAndScorePlugin{
			mockScorePlugin: &mockScorePlugin{name: "mock-prescore-plugin"},
			preScoreStatus:  expectedStatus,
		}
		adapter := &scorePluginAdapter{ScorePlugin: mock}
		status := adapter.PreScore(context.Background(), nil, nil, nil)
		if status != expectedStatus {
			t.Errorf("Expected status %v, got %v", expectedStatus, status)
		}
		if !mock.preScoreCalled {
			t.Errorf("Expected PreScore to be called on the mock plugin")
		}
	})
}

// --- Mock types for DRA scarcity scoring test ---

type mockSharedDRAManager struct {
	slices []*resourcev1.ResourceSlice
}

func (m *mockSharedDRAManager) ResourceClaims() fwk.ResourceClaimTracker { return &mockClaimTracker{} }
func (m *mockSharedDRAManager) ResourceSlices() fwk.ResourceSliceLister {
	return &mockSliceLister{slices: m.slices}
}
func (m *mockSharedDRAManager) DeviceClasses() fwk.DeviceClassLister { return &mockDeviceClassLister{} }
func (m *mockSharedDRAManager) DeviceClassResolver() fwk.DeviceClassResolver {
	return &mockDeviceClassResolver{}
}

type mockClaimTracker struct{}

func (m *mockClaimTracker) List() ([]*resourcev1.ResourceClaim, error) { return nil, nil }
func (m *mockClaimTracker) Get(namespace, claimName string) (*resourcev1.ResourceClaim, error) {
	return nil, nil
}
func (m *mockClaimTracker) ListAllAllocatedDevices() (sets.Set[structured.DeviceID], error) {
	return nil, nil
}
func (m *mockClaimTracker) GatherAllocatedState() (*structured.AllocatedState, error) {
	return nil, nil
}
func (m *mockClaimTracker) AssumeClaimAfterAPICall(claim *resourcev1.ResourceClaim) error {
	return nil
}
func (m *mockClaimTracker) SignalClaimPendingAllocation(claimUID types.UID, allocatedClaim *resourcev1.ResourceClaim) error {
	return nil
}
func (m *mockClaimTracker) RemoveClaimPendingAllocation(claimUID types.UID) (deleted bool) {
	return false
}
func (m *mockClaimTracker) ClaimHasPendingAllocation(claimUID types.UID) bool { return false }
func (m *mockClaimTracker) AssumedClaimRestore(namespace, claimName string)   {}

type mockSliceLister struct {
	slices []*resourcev1.ResourceSlice
}

func (m *mockSliceLister) ListWithDeviceTaintRules() ([]*resourcev1.ResourceSlice, error) {
	return m.slices, nil
}

type mockDeviceClassLister struct{}

func (m *mockDeviceClassLister) List() ([]*resourcev1.DeviceClass, error) { return nil, nil }
func (m *mockDeviceClassLister) Get(className string) (*resourcev1.DeviceClass, error) {
	return nil, nil
}

type mockDeviceClassResolver struct{}

func (m *mockDeviceClassResolver) GetDeviceClass(resourceName v1.ResourceName) *resourcev1.DeviceClass {
	return nil
}

func TestDRAScarcityPenalty(t *testing.T) {
	nodeWithGPU := "lynxi-116"
	nodeWithAPU := "lynxi-161"
	nodeWithLink := "lynxi-178"
	nodeNoDRA := "master-230"

	// Build ResourceSlices: 3 nodes have DRA devices, 1 does not
	resourceSlices := []*resourcev1.ResourceSlice{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "gpu-slice"},
			Spec: resourcev1.ResourceSliceSpec{
				NodeName: &nodeWithGPU,
				Devices:  []resourcev1.Device{{Name: "gpu-0"}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "apu-slice"},
			Spec: resourcev1.ResourceSliceSpec{
				NodeName: &nodeWithAPU,
				Devices:  []resourcev1.Device{{Name: "apu-0"}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "link-slice"},
			Spec: resourcev1.ResourceSliceSpec{
				NodeName: &nodeWithLink,
				Devices:  []resourcev1.Device{{Name: "link-0"}},
			},
		},
		// master-230 has NO ResourceSlice → not a DRA node
	}

	// Build fwk.NodeInfo list
	buildNodeInfo := func(name string) fwk.NodeInfo {
		node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		ni := k8sframework.NewNodeInfo()
		ni.SetNode(node)
		return ni
	}
	nodes := []fwk.NodeInfo{
		buildNodeInfo(nodeWithGPU),
		buildNodeInfo(nodeWithAPU),
		buildNodeInfo(nodeWithLink),
		buildNodeInfo(nodeNoDRA),
	}

	tests := []struct {
		name          string
		task          *api.TaskInfo
		expectedScore map[string]float64 // nodeName → expected score
		draEnabled    bool
	}{
		{
			name: "non-DRA task: DRA nodes get 0, non-DRA node gets MaxNodeScore*weight",
			task: &api.TaskInfo{
				Pod:       &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "decoder-0", Namespace: "default"}},
				DRAResreq: nil, // no DRA requirements
			},
			draEnabled: true,
			expectedScore: map[string]float64{
				nodeWithGPU:  0,
				nodeWithAPU:  0,
				nodeWithLink: 0,
				nodeNoDRA:    200, // MaxNodeScore(100) * defaultWeight(2)
			},
		},
		{
			name: "DRA task gets no scarcity scoring",
			task: &api.TaskInfo{
				Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "encoder-0", Namespace: "default"}},
				DRAResreq: map[string]*api.DRAResource{
					"gpu.example.com": {Count: 1},
				},
			},
			draEnabled: true,
			expectedScore: map[string]float64{
				nodeWithGPU:  0,
				nodeWithAPU:  0,
				nodeWithLink: 0,
				nodeNoDRA:    0,
			},
		},
		{
			name: "non-DRA task with DRA disabled gets no scarcity scoring",
			task: &api.TaskInfo{
				Pod:       &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "default"}},
				DRAResreq: nil,
			},
			draEnabled: false,
			expectedScore: map[string]float64{
				nodeWithGPU:  0,
				nodeWithAPU:  0,
				nodeWithLink: 0,
				nodeNoDRA:    0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pp := &PredicatesPlugin{
				pluginArguments: framework.Arguments{},
				enabledPredicates: predicateEnable{
					dynamicResourceAllocationEnable: tt.draEnabled,
				},
			}
			nodeMap := map[string]fwk.NodeInfo{}
			client := k8sfake.NewSimpleClientset()
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			pp.Handle = k8s.NewFramework(
				nodeMap,
				k8s.WithSharedDRAManager(&mockSharedDRAManager{slices: resourceSlices}),
				k8s.WithClientSet(client),
				k8s.WithInformerFactory(informerFactory),
			)

			scores := pp.draScarcityPenalty(tt.task, nodes)

			for nodeName, expected := range tt.expectedScore {
				got := 0.0
				if s, ok := scores[nodeName]; ok {
					got = s
				}
				if got != expected {
					t.Errorf("node %s: expected score %v, got %v", nodeName, expected, got)
				}
			}
		})
	}
}
