/*
Copyright 2022 The Kubernetes Authors.
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

package defaultevictor

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/informers"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
	utilptr "k8s.io/utils/ptr"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworkfake "sigs.k8s.io/descheduler/pkg/framework/fake"
	testutil "sigs.k8s.io/descheduler/test"
)

func TestShouldHandlePDB_SingleReplicaDeployment(t *testing.T) {
	ctx := context.Background()

	// Build a single-replica deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "single-replica-app",
			Namespace: "default",
			UID:       "deployment-uid-123",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "single-replica-app",
				"app.kubernetes.io/component": "core",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: utilptr.To[int32](1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":      "single-replica-app",
					"app.kubernetes.io/component": "core",
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":      "single-replica-app",
						"app.kubernetes.io/component": "core",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "app",
							Image: "nginx:latest",
						},
					},
				},
			},
		},
	}

	// Build a pod from the single-replica deployment
	pod := testutil.BuildTestPod("single-replica-app-xyz123-abc45", 400, 0, "node1", func(pod *v1.Pod) {
		pod.Labels = map[string]string{
			"app.kubernetes.io/name":      "single-replica-app",
			"app.kubernetes.io/component": "core",
		}
		pod.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "single-replica-app",
				UID:        "deployment-uid-123",
			},
		}
	})

	// Build a PDB that protects the pod (manually created, not operator-managed)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "single-replica-app-min-replica-pdb",
			Namespace: "default",
			Labels: map[string]string{
				"app": "single-replica-app",
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: utilptr.To[intstr.IntOrString](intstr.FromInt32(0)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":      "single-replica-app",
					"app.kubernetes.io/component": "core",
				},
			},
		},
	}

	// Create fake client with deployment, pod, and PDB
	objs := []runtime.Object{deployment, pod, pdb}
	fakeClient := fakeclient.NewSimpleClientset(objs...)
	sharedInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	// Start informers
	sharedInformerFactory.Start(ctx.Done())
	sharedInformerFactory.WaitForCacheSync(ctx.Done())

	// Initialize DefaultEvictor plugin
	evictorArgs := &DefaultEvictorArgs{
		DeletePDBsForSingleReplicaDeployments: true,
	}

	podInformer := sharedInformerFactory.Core().V1().Pods().Informer()
	getPodsAssignedToNode, err := podutil.BuildGetPodsAssignedToNodeFunc(podInformer)
	if err != nil {
		t.Fatalf("Unable to build GetPodsAssignedToNodeFunc: %v", err)
	}

	evictorPlugin, err := New(ctx, evictorArgs, &frameworkfake.HandleImpl{
		ClientsetImpl:                 fakeClient,
		GetPodsAssignedToNodeFuncImpl: getPodsAssignedToNode,
		SharedInformerFactoryImpl:     sharedInformerFactory,
	})
	if err != nil {
		t.Fatalf("Unable to initialize the plugin: %v", err)
	}

	defaultEvictor, ok := evictorPlugin.(*DefaultEvictor)
	if !ok {
		t.Fatalf("Unable to cast plugin to DefaultEvictor")
	}

	logger := klog.FromContext(ctx)

	// Test shouldHandlePDB with single-replica deployment
	action, reason := defaultEvictor.shouldHandlePDB(ctx, pdb, []*v1.Pod{pod}, []*v1.Pod{pod}, logger)

	if action != PDBActionDelete {
		t.Errorf("Expected action PDBActionDelete for single-replica deployment, got %s (reason: %s)", action, reason)
	}
}

func TestShouldHandlePDB_MultiReplicaDeployment_AllOnNode(t *testing.T) {
	ctx := context.Background()

	// Build a multi-replica deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multi-replica-gateway",
			Namespace: "networking",
			UID:       "deployment-uid-456",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "multi-replica-gateway",
				"app.kubernetes.io/component": "gateway",
				"region":                      "us-west",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: utilptr.To[int32](3),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":      "multi-replica-gateway",
					"app.kubernetes.io/component": "gateway",
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":      "multi-replica-gateway",
						"app.kubernetes.io/component": "gateway",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "gateway",
							Image: "gateway:latest",
						},
					},
				},
			},
		},
	}

	// Build pods from the multi-replica deployment (all on node1)
	pods := make([]*v1.Pod, 3)
	podNames := []string{"multi-replica-gateway-abc12-def34", "multi-replica-gateway-abc12-def35", "multi-replica-gateway-abc12-def36"}
	for i := 0; i < 3; i++ {
		pods[i] = testutil.BuildTestPod(podNames[i], 400, 0, "node1", func(pod *v1.Pod) {
			pod.Namespace = "networking"
			pod.Labels = map[string]string{
				"app.kubernetes.io/name":      "multi-replica-gateway",
				"app.kubernetes.io/component": "gateway",
			}
			pod.OwnerReferences = []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "multi-replica-gateway",
					UID:        "deployment-uid-456",
				},
			}
		})
	}

	// Build a PDB that protects the pods (Kyverno-generated style with 50% minAvailable)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multi-replica-gateway-pdb",
			Namespace: "networking",
			Labels: map[string]string{
				"app.kubernetes.io/managed-by":     "kyverno",
				"generate.kyverno.io/policy-name":  "zone-affinity-pdb-generator",
				"generate.kyverno.io/rule-name":    "zone-affinity-pdb",
				"generate.kyverno.io/trigger-kind": "Deployment",
				"generate.kyverno.io/trigger-name": "multi-replica-gateway",
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: utilptr.To[intstr.IntOrString](intstr.FromInt32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":      "multi-replica-gateway",
					"app.kubernetes.io/component": "gateway",
				},
			},
		},
	}

	// Create fake client with deployment, pods, and PDB
	objs := []runtime.Object{deployment, pdb}
	for _, pod := range pods {
		objs = append(objs, pod)
	}
	fakeClient := fakeclient.NewSimpleClientset(objs...)
	sharedInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	// Start informers
	sharedInformerFactory.Start(ctx.Done())
	sharedInformerFactory.WaitForCacheSync(ctx.Done())

	// Initialize DefaultEvictor plugin
	evictorArgs := &DefaultEvictorArgs{
		DeletePDBsForSingleReplicaDeployments: true,
	}

	podInformer := sharedInformerFactory.Core().V1().Pods().Informer()
	getPodsAssignedToNode, err := podutil.BuildGetPodsAssignedToNodeFunc(podInformer)
	if err != nil {
		t.Fatalf("Unable to build GetPodsAssignedToNodeFunc: %v", err)
	}

	evictorPlugin, err := New(ctx, evictorArgs, &frameworkfake.HandleImpl{
		ClientsetImpl:                 fakeClient,
		GetPodsAssignedToNodeFuncImpl: getPodsAssignedToNode,
		SharedInformerFactoryImpl:     sharedInformerFactory,
	})
	if err != nil {
		t.Fatalf("Unable to initialize the plugin: %v", err)
	}

	defaultEvictor, ok := evictorPlugin.(*DefaultEvictor)
	if !ok {
		t.Fatalf("Unable to cast plugin to DefaultEvictor")
	}

	logger := klog.FromContext(ctx)

	// Test shouldHandlePDB with all multi-replica pods on target node
	// Since all 3 pods are on the node and it's a multi-replica deployment,
	// it should MODIFY the PDB (all pods of PDB on single node)
	action, reason := defaultEvictor.shouldHandlePDB(ctx, pdb, pods, pods, logger)

	if action != PDBActionModify {
		t.Errorf("Expected action PDBActionModify for multi-replica with all on node, got %s (reason: %s)", action, reason)
	}
}

func TestShouldHandlePDB_SingleReplicaWithMultipleNamesOnNode(t *testing.T) {
	ctx := context.Background()

	// Build a single-replica deployment matching the real issue scenario (Calyptia logging pipeline)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs",
			Namespace: "calyptia",
			UID:       "1b8e450d-6dc9-4fc5-8363-01f894227cdd",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "gamma-cism-v1-0-0",
				"app.kubernetes.io/component": "calyptia-core",
				"core-pipeline":               "gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: utilptr.To[int32](1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/component": "calyptia-core",
					"core-pipeline":               "gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs",
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/component": "calyptia-core",
						"core-pipeline":               "gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "calyptia",
							Image: "calyptia/core:latest",
						},
					},
				},
			},
		},
	}

	// Build a pod from the single-replica deployment (matching the issue scenario exactly)
	pod := testutil.BuildTestPod("gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs-abc123-def456", 400, 0, "node1", func(pod *v1.Pod) {
		pod.Labels = map[string]string{
			"app.kubernetes.io/component": "calyptia-core",
			"core-pipeline":               "gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs",
		}
		pod.Namespace = "calyptia"
		pod.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs",
				UID:        "1b8e450d-6dc9-4fc5-8363-01f894227cdd",
			},
		}
	})

	// Build a PDB with Kyverno-generated metadata (this is the key test case)
	// The old code would fail here because the PDB name doesn't contain "single-replica"
	// or "min-replica" in the name itself - it's named after the deployment
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs-pdb",
			Namespace: "calyptia",
			UID:       "e9ca4f5a-79f6-4e8f-9f04-dbcc63953896",
			Labels: map[string]string{
				"app": "gamma-cism",
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: utilptr.To[intstr.IntOrString](intstr.FromInt32(0)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/component": "calyptia-core",
					"core-pipeline":               "gamma-cism-v1-0-0-gamma-cism-v1-0-0-logs",
				},
			},
		},
	}

	// Create fake client
	objs := []runtime.Object{deployment, pod, pdb}
	fakeClient := fakeclient.NewSimpleClientset(objs...)
	sharedInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	// Start informers
	sharedInformerFactory.Start(ctx.Done())
	sharedInformerFactory.WaitForCacheSync(ctx.Done())

	// Initialize DefaultEvictor plugin
	evictorArgs := &DefaultEvictorArgs{
		DeletePDBsForSingleReplicaDeployments: true,
	}

	podInformer := sharedInformerFactory.Core().V1().Pods().Informer()
	getPodsAssignedToNode, err := podutil.BuildGetPodsAssignedToNodeFunc(podInformer)
	if err != nil {
		t.Fatalf("Unable to build GetPodsAssignedToNodeFunc: %v", err)
	}

	evictorPlugin, err := New(ctx, evictorArgs, &frameworkfake.HandleImpl{
		ClientsetImpl:                 fakeClient,
		GetPodsAssignedToNodeFuncImpl: getPodsAssignedToNode,
		SharedInformerFactoryImpl:     sharedInformerFactory,
	})
	if err != nil {
		t.Fatalf("Unable to initialize the plugin: %v", err)
	}

	defaultEvictor, ok := evictorPlugin.(*DefaultEvictor)
	if !ok {
		t.Fatalf("Unable to cast plugin to DefaultEvictor")
	}

	logger := klog.FromContext(ctx)

	// Test shouldHandlePDB - this is the critical test
	// The old code would return PDBActionNone because the PDB name doesn't contain
	// "min-replica" or "single-replica", even though all pods are from single-replica deployments
	// The fixed code should return PDBActionDelete
	action, reason := defaultEvictor.shouldHandlePDB(ctx, pdb, []*v1.Pod{pod}, []*v1.Pod{pod}, logger)

	if action != PDBActionDelete {
		t.Errorf("Expected action PDBActionDelete for single-replica deployment with generic PDB name, got %s (reason: %s)", action, reason)
	}
}
