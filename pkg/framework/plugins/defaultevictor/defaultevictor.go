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
	"errors"
	"fmt"
	"maps"
	"slices"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	evictionutils "sigs.k8s.io/descheduler/pkg/descheduler/evictions/utils"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
	"sigs.k8s.io/descheduler/pkg/utils"
)

const (
	PluginName                 = "DefaultEvictor"
	evictPodAnnotationKey      = "descheduler.alpha.kubernetes.io/evict"
	namespaceWithLabelSelector = "namespaceWithLabelSelector-"
)

var _ frameworktypes.EvictorPlugin = &DefaultEvictor{}

type constraint func(pod *v1.Pod) error

// DefaultEvictor is the first EvictorPlugin, which defines the default extension points of the
// pre-baked evictor that is shipped.
// Even though we name this plugin DefaultEvictor, it does not actually evict anything,
// This plugin is only meant to customize other actions (extension points) of the evictor,
// like filtering, sorting, and other ones that might be relevant in the future
type DefaultEvictor struct {
	logger      klog.Logger
	args        *DefaultEvictorArgs
	constraints []constraint
	handle      frameworktypes.Handle
}

// IsPodEvictableBasedOnPriority checks if the given pod is evictable based on priority resolved from pod Spec.
func IsPodEvictableBasedOnPriority(pod *v1.Pod, priority int32) bool {
	return pod.Spec.Priority == nil || *pod.Spec.Priority < priority
}

// HaveEvictAnnotation checks if the pod have evict annotation
func HaveEvictAnnotation(pod *v1.Pod) bool {
	_, found := pod.ObjectMeta.Annotations[evictPodAnnotationKey]
	return found
}

// New builds plugin from its arguments while passing a handle
// nolint: gocyclo
func New(ctx context.Context, args runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	defaultEvictorArgs, ok := args.(*DefaultEvictorArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type defaultEvictorFilterArgs, got %T", args)
	}
	logger := klog.FromContext(ctx).WithValues("plugin", PluginName)

	ev := &DefaultEvictor{
		logger: logger,
		handle: handle,
		args:   defaultEvictorArgs,
	}
	// add constraints
	err := ev.addAllConstraints(logger, handle)
	if err != nil {
		return nil, err
	}

	if ev.args.NamespaceLabelSelector != nil && (len(ev.args.NamespaceLabelSelector.MatchLabels) > 0 || len(ev.args.NamespaceLabelSelector.MatchExpressions) > 0) {
		selector, nslErr := metav1.LabelSelectorAsSelector(ev.args.NamespaceLabelSelector)
		if nslErr != nil {
			return nil, fmt.Errorf("unable to convert namespaceLabelSelector to label selector: %w", nslErr)
		}
		indexName := namespaceWithLabelSelector + ev.handle.PluginInstanceID()
		if nslErr := addNamespaceLabelSelectorIndexer(ev.handle.SharedInformerFactory().Core().V1().Namespaces().Informer(), indexName, selector); nslErr != nil {
			return nil, fmt.Errorf("failed to add namespace label selector indexer: %w", nslErr)
		}
	}
	return ev, nil
}

func addNamespaceLabelSelectorIndexer(informer cache.SharedIndexInformer, indexName string, selector labels.Selector) error {
	indexer := informer.GetIndexer()
	for name := range indexer.GetIndexers() {
		if name == indexName {
			return nil
		}
	}
	return informer.AddIndexers(cache.Indexers{
		indexName: func(obj interface{}) ([]string, error) {
			ns, ok := obj.(*v1.Namespace)
			if !ok {
				return []string{}, errors.New("unexpected object")
			}
			if !selector.Empty() {
				if !selector.Matches(labels.Set(ns.Labels)) {
					return []string{}, nil
				}
			}
			return []string{ns.GetName()}, nil
		},
	})
}

func (d *DefaultEvictor) addAllConstraints(logger klog.Logger, handle frameworktypes.Handle) error {
	args := d.args
	// Determine effective protected policies based on the provided arguments.
	effectivePodProtections := getEffectivePodProtections(args)

	if err := applyEffectivePodProtections(d, effectivePodProtections, handle); err != nil {
		return fmt.Errorf("failed to apply effective protected policies: %w", err)
	}
	if constraints, err := evictionConstraintsForLabelSelector(logger, args.LabelSelector); err != nil {
		return err
	} else {
		d.constraints = append(d.constraints, constraints...)
	}
	if constraints, err := evictionConstraintsForMinReplicas(logger, args.MinReplicas, handle); err != nil {
		return err
	} else {
		d.constraints = append(d.constraints, constraints...)
	}
	d.constraints = append(d.constraints, evictionConstraintsForMinPodAge(args.MinPodAge)...)
	return nil
}

// applyEffectivePodProtections configures the evictor with specified Pod protection.
func applyEffectivePodProtections(d *DefaultEvictor, podProtections []PodProtection, handle frameworktypes.Handle) error {
	protectionMap := make(map[PodProtection]bool, len(podProtections))
	for _, protection := range podProtections {
		protectionMap[protection] = true
	}

	// Apply protections
	if err := applySystemCriticalPodsProtection(d, protectionMap, handle); err != nil {
		return err
	}
	applyFailedBarePodsProtection(d, protectionMap)
	applyLocalStoragePodsProtection(d, protectionMap)
	applyDaemonSetPodsProtection(d, protectionMap)
	applyPVCPodsProtection(d, protectionMap)
	applyPodsWithoutPDBProtection(d, protectionMap, handle)
	applyPodsWithResourceClaimsProtection(d, protectionMap)

	return nil
}

// protectedPVCStorageClasses returns the list of storage classes that should
// be protected from eviction. If the list is empty or nil then all storage
// classes are protected (assuming PodsWithPVC protection is enabled).
func protectedPVCStorageClasses(d *DefaultEvictor) []ProtectedStorageClass {
	protcfg := d.args.PodProtections.Config
	if protcfg == nil {
		return nil
	}
	scconfig := protcfg.PodsWithPVC
	if scconfig == nil {
		return nil
	}
	return scconfig.ProtectedStorageClasses
}

// podStorageClasses returns a list of storage classes referred by a pod. We
// need this when assessing if a pod should be protected because it refers to a
// protected storage class.
func podStorageClasses(inf informers.SharedInformerFactory, pod *v1.Pod) ([]string, error) {
	lister := inf.Core().V1().PersistentVolumeClaims().Lister().PersistentVolumeClaims(
		pod.Namespace,
	)

	referred := map[string]bool{}
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}

		claim, err := lister.Get(vol.PersistentVolumeClaim.ClaimName)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to get persistent volume claim %q/%q: %w",
				pod.Namespace, vol.PersistentVolumeClaim.ClaimName, err,
			)
		}

		// this should never happen as once a pvc is created with a nil
		// storageClass it is automatically picked up by the default
		// storage class. By returning an error here we make the pod
		// protected from eviction.
		if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName == "" {
			return nil, fmt.Errorf(
				"failed to resolve storage class for pod %q/%q",
				pod.Namespace, claim.Name,
			)
		}

		referred[*claim.Spec.StorageClassName] = true
	}

	return slices.Collect(maps.Keys(referred)), nil
}

func applyFailedBarePodsProtection(d *DefaultEvictor, protectionMap map[PodProtection]bool) {
	isProtectionEnabled := protectionMap[FailedBarePods]
	if !isProtectionEnabled {
		d.logger.V(1).Info("Warning: EvictFailedBarePods is set to True. This could cause eviction of pods without ownerReferences.")
		d.constraints = append(d.constraints, func(pod *v1.Pod) error {
			ownerRefList := podutil.OwnerRef(pod)
			if len(ownerRefList) == 0 && pod.Status.Phase != v1.PodFailed {
				return fmt.Errorf("pod does not have any ownerRefs and is not in failed phase")
			}
			return nil
		})
	} else {
		d.constraints = append(d.constraints, func(pod *v1.Pod) error {
			if len(podutil.OwnerRef(pod)) == 0 {
				return fmt.Errorf("pod does not have any ownerRefs")
			}
			return nil
		})
	}
}

func applySystemCriticalPodsProtection(d *DefaultEvictor, protectionMap map[PodProtection]bool, handle frameworktypes.Handle) error {
	isProtectionEnabled := protectionMap[SystemCriticalPods]
	if !isProtectionEnabled {
		d.logger.V(1).Info("Warning: System critical pod protection is disabled. This could cause eviction of Kubernetes system pods.")
		return nil
	}

	d.constraints = append(d.constraints, func(pod *v1.Pod) error {
		if utils.IsCriticalPriorityPod(pod) {
			return fmt.Errorf("pod has system critical priority and is protected against eviction")
		}
		return nil
	})

	priorityThreshold := d.args.PriorityThreshold
	if priorityThreshold != nil && (priorityThreshold.Value != nil || len(priorityThreshold.Name) > 0) {
		thresholdPriority, err := utils.GetPriorityValueFromPriorityThreshold(context.TODO(), handle.ClientSet(), priorityThreshold)
		if err != nil {
			d.logger.Error(err, "failed to get priority threshold")
			return err
		}
		d.constraints = append(d.constraints, func(pod *v1.Pod) error {
			if !IsPodEvictableBasedOnPriority(pod, thresholdPriority) {
				return fmt.Errorf("pod has higher priority than specified priority class threshold")
			}
			return nil
		})
	}
	return nil
}

func applyLocalStoragePodsProtection(d *DefaultEvictor, protectionMap map[PodProtection]bool) {
	isProtectionEnabled := protectionMap[PodsWithLocalStorage]
	if isProtectionEnabled {
		d.constraints = append(d.constraints, func(pod *v1.Pod) error {
			if utils.IsPodWithLocalStorage(pod) {
				return fmt.Errorf("pod has local storage and is protected against eviction")
			}
			return nil
		})
	}
}

func applyDaemonSetPodsProtection(d *DefaultEvictor, protectionMap map[PodProtection]bool) {
	isProtectionEnabled := protectionMap[DaemonSetPods]
	if isProtectionEnabled {
		d.constraints = append(d.constraints, func(pod *v1.Pod) error {
			ownerRefList := podutil.OwnerRef(pod)
			if utils.IsDaemonsetPod(ownerRefList) {
				return fmt.Errorf("daemonset pods are protected against eviction")
			}
			return nil
		})
	}
}

// applyPVCPodsProtection protects pods that refer to a PVC from eviction. If
// the user has specified a list of storage classes to protect then only pods
// referring to PVCs of those storage classes are protected.
func applyPVCPodsProtection(d *DefaultEvictor, enabledProtections map[PodProtection]bool) {
	if !enabledProtections[PodsWithPVC] {
		return
	}

	// if the user isn't filtering by storage classes we protect all pods
	// referring to a PVC.
	protected := protectedPVCStorageClasses(d)
	if len(protected) == 0 {
		d.constraints = append(
			d.constraints,
			func(pod *v1.Pod) error {
				if utils.IsPodWithPVC(pod) {
					return fmt.Errorf("pod with PVC is protected against eviction")
				}
				return nil
			},
		)
		return
	}

	protectedsc := map[string]bool{}
	for _, class := range protected {
		protectedsc[class.Name] = true
	}

	d.constraints = append(
		d.constraints, func(pod *v1.Pod) error {
			classes, err := podStorageClasses(d.handle.SharedInformerFactory(), pod)
			if err != nil {
				return err
			}
			for _, class := range classes {
				if !protectedsc[class] {
					continue
				}
				return fmt.Errorf("pod using protected storage class %q", class)
			}
			return nil
		},
	)
}

func applyPodsWithoutPDBProtection(d *DefaultEvictor, protectionMap map[PodProtection]bool, handle frameworktypes.Handle) {
	isProtectionEnabled := protectionMap[PodsWithoutPDB]
	if isProtectionEnabled {
		d.constraints = append(d.constraints, func(pod *v1.Pod) error {
			hasPdb, err := utils.IsPodCoveredByPDB(pod, handle.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister())
			if err != nil {
				return fmt.Errorf("unable to check if pod is covered by PodDisruptionBudget: %w", err)
			}
			if !hasPdb {
				return fmt.Errorf("pod does not have a PodDisruptionBudget and is protected against eviction")
			}
			return nil
		})
	}
}

func applyPodsWithResourceClaimsProtection(d *DefaultEvictor, protectionMap map[PodProtection]bool) {
	isProtectionEnabled := protectionMap[PodsWithResourceClaims]
	if isProtectionEnabled {
		d.constraints = append(d.constraints, func(pod *v1.Pod) error {
			if utils.IsPodWithResourceClaims(pod) {
				return fmt.Errorf("pod has ResourceClaims and descheduler is configured to protect ResourceClaims pods")
			}
			return nil
		})
	}
}

// getEffectivePodProtections determines which policies are currently active.
// It supports both new-style (PodProtections) and legacy-style flags.
func getEffectivePodProtections(args *DefaultEvictorArgs) []PodProtection {
	// determine whether to use PodProtections config
	useNewConfig := len(args.PodProtections.DefaultDisabled) > 0 || len(args.PodProtections.ExtraEnabled) > 0

	if !useNewConfig {
		// fall back to the Deprecated config
		return legacyGetPodProtections(args)
	}

	// effective is the final list of active protection.
	effective := make([]PodProtection, 0)
	effective = append(effective, defaultPodProtections...)

	// Remove PodProtections that are in the DefaultDisabled list.
	effective = slices.DeleteFunc(effective, func(protection PodProtection) bool {
		return slices.Contains(args.PodProtections.DefaultDisabled, protection)
	})

	// Add extra enabled in PodProtections
	effective = append(effective, args.PodProtections.ExtraEnabled...)

	return effective
}

// legacyGetPodProtections returns protections using deprecated boolean flags.
func legacyGetPodProtections(args *DefaultEvictorArgs) []PodProtection {
	var protections []PodProtection

	// defaultDisabled
	if !args.EvictLocalStoragePods {
		protections = append(protections, PodsWithLocalStorage)
	}
	if !args.EvictDaemonSetPods {
		protections = append(protections, DaemonSetPods)
	}
	if !args.EvictSystemCriticalPods {
		protections = append(protections, SystemCriticalPods)
	}
	if !args.EvictFailedBarePods {
		protections = append(protections, FailedBarePods)
	}

	// extraEnabled
	if args.IgnorePvcPods {
		protections = append(protections, PodsWithPVC)
	}
	if args.IgnorePodsWithoutPDB {
		protections = append(protections, PodsWithoutPDB)
	}
	return protections
}

// Name retrieves the plugin name
func (d *DefaultEvictor) Name() string {
	return PluginName
}

func (d *DefaultEvictor) PreEvictionFilter(pod *v1.Pod) bool {
	logger := d.logger.WithValues("ExtensionPoint", frameworktypes.PreEvictionFilterExtensionPoint)
	if d.args.NodeFit {
		// Skip nodeFit check for pods in excluded namespaces
		if d.isNamespaceExcludedFromNodeFit(pod.Namespace) {
			logger.V(3).Info("pod is in excluded namespace, skipping nodeFit check", "pod", klog.KObj(pod), "namespace", pod.Namespace)
			return true
		}

		// Skip nodeFit check for pods matching the label selector (they are exempt from fit checking)
		if d.args.LabelSelector != nil {
			selector, err := metav1.LabelSelectorAsSelector(d.args.LabelSelector)
			if err == nil && selector.Matches(labels.Set(pod.Labels)) {
				logger.V(3).Info("pod matches nodeFit exemption label selector, skipping nodeFit check", "pod", klog.KObj(pod))
				return true
			}
		}

		nodes, err := nodeutil.ReadyNodes(context.TODO(), d.handle.ClientSet(), d.handle.SharedInformerFactory().Core().V1().Nodes().Lister(), d.args.NodeSelector)
		if err != nil {
			logger.Error(err, "unable to list ready nodes", "pod", klog.KObj(pod))
			return false
		}
		if !nodeutil.PodFitsAnyOtherNode(d.handle.GetPodsAssignedToNodeFunc(), pod, nodes) {
			logger.V(3).Info("pod does not fit on any other node because of nodeSelector(s), Taint(s), or nodes marked as unschedulable", "pod", klog.KObj(pod))
			return false
		}
	}

	if d.args.NamespaceLabelSelector == nil || (len(d.args.NamespaceLabelSelector.MatchLabels) == 0 && len(d.args.NamespaceLabelSelector.MatchExpressions) == 0) {
		return true
	}
	indexName := namespaceWithLabelSelector + d.handle.PluginInstanceID()
	objs, err := d.handle.SharedInformerFactory().Core().V1().Namespaces().Informer().GetIndexer().ByIndex(indexName, pod.Namespace)
	if err != nil {
		logger.Error(err, "unable to list namespaces for namespaceLabelSelector filter in the policy parameter", "pod", klog.KObj(pod))
		return false
	}
	if len(objs) == 0 {
		logger.Info("pod namespace do not match the namespaceLabelSelector filter in the policy parameter", "pod", klog.KObj(pod))
		return false
	}
	return true
}

func (d *DefaultEvictor) Filter(pod *v1.Pod) bool {
	logger := d.logger.WithValues("ExtensionPoint", frameworktypes.FilterExtensionPoint)
	checkErrs := []error{}

	if HaveEvictAnnotation(pod) {
		return true
	}

	if d.args.NoEvictionPolicy == MandatoryNoEvictionPolicy && evictionutils.HaveNoEvictionAnnotation(pod) {
		return false
	}

	if utils.IsMirrorPod(pod) {
		checkErrs = append(checkErrs, fmt.Errorf("pod is a mirror pod"))
	}

	if utils.IsStaticPod(pod) {
		checkErrs = append(checkErrs, fmt.Errorf("pod is a static pod"))
	}

	if utils.IsPodTerminating(pod) {
		checkErrs = append(checkErrs, fmt.Errorf("pod is terminating"))
	}

	for _, c := range d.constraints {
		if err := c(pod); err != nil {
			checkErrs = append(checkErrs, err)
		}
	}

	if len(checkErrs) > 0 {
		logger.V(4).Info("Pod fails the following checks", "pod", klog.KObj(pod), "checks", utilerrors.NewAggregate(checkErrs).Error())
		return false
	}

	return true
}

func getPodIndexerByOwnerRefs(indexName string, handle frameworktypes.Handle) (cache.Indexer, error) {
	podInformer := handle.SharedInformerFactory().Core().V1().Pods().Informer()
	indexer := podInformer.GetIndexer()

	// do not reinitialize the indexer, if it's been defined already
	for name := range indexer.GetIndexers() {
		if name == indexName {
			return indexer, nil
		}
	}

	if err := podInformer.AddIndexers(cache.Indexers{
		indexName: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return []string{}, errors.New("unexpected object")
			}

			return podutil.OwnerRefUIDs(pod), nil
		},
	}); err != nil {
		return nil, err
	}

	return indexer, nil
}

// PDB action types
const (
	PDBActionDelete = "delete"
	PDBActionModify = "modify"
	PDBActionNone   = "none"
)

// shouldHandlePDB determines what action to take with a PDB to allow evictions during node drain
// It returns (action, reason) where action is one of: "delete", "modify", or "none"
func (d *DefaultEvictor) shouldHandlePDB(ctx context.Context, pdb *policyv1.PodDisruptionBudget, allPods, podsOnNode []*v1.Pod, logger klog.Logger) (string, string) {
	if len(allPods) == 0 {
		return PDBActionNone, "PDB has no pods"
	}

	// Scenario 1: Single-replica deployments - DELETE these PDBs (safe to delete)
	// Check if ALL pods covered by this PDB are from single-replica deployments
	// (not just pods on the current node - check all pods in allPods)
	allAreSingleReplica := true
	for _, pod := range allPods {
		if !d.isSingleReplicaDeploymentPod(ctx, pod) {
			allAreSingleReplica = false
			break
		}
	}
	if allAreSingleReplica && len(allPods) > 0 {
		// For single-replica deployments, if the PDB has minAvailable: 1 and there's only
		// 1 pod, the PDB is overly restrictive. For these, MODIFY instead of DELETE because:
		// 1. Some operators (OTEL, Kyverno) auto-recreate deleted PDBs
		// 2. Modifying allows temporary eviction during node drain
		// 3. It's safer than deletion which might fail to recreate in time
		if d.shouldModifyInsteadOfDelete(pdb) {
			return PDBActionModify, "single-replica-deployment-restrictive"
		}
		// For non-restrictive single-replica PDBs, DELETE is safe because there's only 1 pod
		return PDBActionDelete, "single-replica-deployment"
	}

	// Scenario 2: All pods of a PDB are on a single node - MODIFY PDB during node drain
	// This handles cases like zone-specific gateways where all replicas for a zone happen to land on one node
	// Only modify if there's more than 1 replica (single-replica PDBs are handled by scenario 1)
	if len(podsOnNode) == len(allPods) && len(podsOnNode) > 1 {
		return PDBActionModify, fmt.Sprintf("all-pods-on-target-node: PDB has no pods on other nodes")
	}

	// Scenario 3: Underreplicated deployment - when enabled, modify PDBs for deployments running
	// at significantly reduced capacity compared to their configuration
	if d.args.DeletePDBsForUnderreplicatedDeployments {
		if isDeploymentUnderreplicated(allPods) {
			return PDBActionModify, fmt.Sprintf("underreplicated-deployment: pods distributed poorly, enabling eviction")
		}
	}

	// Scenario 4: Deployment running at minimum replica threshold - MODIFY PDB to allow draining
	// If current pods equal minAvailable (or close to it), we're at the minimum and can't evict safely
	// This prevents node drain from getting stuck on underreplicated applications
	minRequired := calculateMinRequiredPods(pdb, len(allPods))
	if len(allPods) <= minRequired {
		return PDBActionModify, fmt.Sprintf("at-minimum-replicas: %d pods meet minimum requirement of %d, allowing eviction", len(allPods), minRequired)
	}

	// Scenario 5: Poorly distributed workload - all or nearly all pods concentrated on very few nodes
	// Calculate which nodes have pods covered by this PDB
	nodeDistribution := make(map[string]int)
	for _, pod := range allPods {
		nodeDistribution[pod.Spec.NodeName]++
	}

	// If pods are concentrated on 1-2 nodes out of many, the PDB isn't achieving distribution
	// This covers zone-specific PDBs where all replicas ended up in the same zone
	nodesWithPods := len(nodeDistribution)
	if nodesWithPods <= 2 && len(allPods) > 2 {
		// Calculate how concentrated the pods are
		maxPodsOnSingleNode := 0
		for _, count := range nodeDistribution {
			if count > maxPodsOnSingleNode {
				maxPodsOnSingleNode = count
			}
		}
		// If >=80% of pods are on a single node, the PDB is failing to distribute
		// Calculate: (maxPodsOnSingleNode * 100) >= (len(allPods) * 80)
		if (maxPodsOnSingleNode * 100) >= (len(allPods) * 80) {
			return PDBActionModify, fmt.Sprintf("poorly-distributed-across-nodes: %d/%d pods on %d nodes", len(podsOnNode), len(allPods), nodesWithPods)
		}
	}

	return PDBActionNone, fmt.Sprintf("adequate-distribution: %d pods across %d nodes", len(allPods), nodesWithPods)
}

// isNamespaceExcludedFromNodeFit checks if a namespace is in the nodeFitExcludedNamespaces list
func (d *DefaultEvictor) isNamespaceExcludedFromNodeFit(namespace string) bool {
	if len(d.args.NodeFitExcludedNamespaces) == 0 {
		return false
	}
	for _, excludedNs := range d.args.NodeFitExcludedNamespaces {
		if excludedNs == namespace {
			return true
		}
	}
	return false
}

// calculateMinRequiredPods calculates the minimum number of pods that must remain available
// based on the PDB's minAvailable and maxUnavailable constraints
func calculateMinRequiredPods(pdb *policyv1.PodDisruptionBudget, totalPods int) int {
	if pdb.Spec.MaxUnavailable != nil {
		// maxUnavailable: how many can be unavailable
		// minRequired = total - maxUnavailable
		maxUnavailableVal := pdb.Spec.MaxUnavailable
		intVal := maxUnavailableVal.IntValue()
		return totalPods - intVal
	}

	if pdb.Spec.MinAvailable != nil {
		// minAvailable: how many must remain available
		minAvailableVal := pdb.Spec.MinAvailable
		intVal := minAvailableVal.IntValue()
		return intVal
	}

	// Default: at least 1 pod must remain (conservative default)
	return 1
}

// isDeploymentUnderreplicated checks if a deployment appears to be running at reduced capacity
// This is useful for identifying PDBs that are unnecessarily restrictive given actual distribution
func isDeploymentUnderreplicated(pods []*v1.Pod) bool {
	if len(pods) < 3 {
		return false
	}

	// Count how many nodes the pods are spread across
	nodeDistribution := make(map[string]int)
	for _, pod := range pods {
		nodeDistribution[pod.Spec.NodeName]++
	}

	// If very few pods are spread across multiple nodes (less than 2 per node on average),
	// the deployment is likely underutilized or poorly distributed
	nodesWithPods := len(nodeDistribution)
	avgPodsPerNode := len(pods) / nodesWithPods

	// Consider underreplicated if:
	// - pods are spread across 3+ nodes but only 2-3 pods average per node
	// - or pods are concentrated on very few nodes despite having replicas
	return nodesWithPods >= 3 && avgPodsPerNode <= 2
}

// DeletePDBsForNode relaxes PodDisruptionBudgets to allow eviction during node drain
// by either deleting them (for safe cases) or modifying them (for operator-managed PDBs).
// For operator-managed PDBs (Kyverno, OTEL, etc), modification is preferred since they
// auto-recreate if deleted. Only processes PDBs with pods on the target node to avoid
// unnecessary cluster-wide checks.
func (d *DefaultEvictor) DeletePDBsForNode(ctx context.Context, node *v1.Node, logger klog.Logger) {
	if !d.args.DeletePDBsForSingleReplicaDeployments {
		return
	}

	logger.V(1).Info("Starting PDB relaxation for single-replica deployments on node", "node", node.Name)

	// List all PDBs in all namespaces
	pdbList, err := d.handle.ClientSet().PolicyV1().PodDisruptionBudgets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		logger.Error(err, "failed to list PodDisruptionBudgets")
		return
	}

	if len(pdbList.Items) == 0 {
		logger.V(2).Info("No PodDisruptionBudgets found")
		return
	}

	logger.V(2).Info("Found PodDisruptionBudgets", "count", len(pdbList.Items))

	relaxedCount := 0
	for i := range pdbList.Items {
		pdb := &pdbList.Items[i]
		logger.V(3).Info("Checking PDB", "pdb", klog.KObj(pdb))

		// Get pods covered by this PDB
		pods, err := d.getPodsForPDB(ctx, pdb)
		if err != nil {
			logger.V(2).Error(err, "failed to get pods for PDB", "pdb", klog.KObj(pdb))
			continue
		}

		if len(pods) == 0 {
			logger.V(3).Info("PDB has no matching pods", "pdb", klog.KObj(pdb))
			continue
		}

		// Filter pods to only those on the target node
		var podsOnNode []*v1.Pod
		for _, pod := range pods {
			if pod.Spec.NodeName == node.Name {
				podsOnNode = append(podsOnNode, pod)
			}
		}

		if len(podsOnNode) == 0 {
			logger.V(3).Info("PDB has no pods on target node", "pdb", klog.KObj(pdb), "node", node.Name)
			continue
		}

		// Determine what action to take with this PDB
		action, reason := d.shouldHandlePDB(ctx, pdb, pods, podsOnNode, logger)

		switch action {
		case PDBActionDelete:
			if len(podsOnNode) > 0 {
				logger.V(1).Info("Deleting PDB for single-replica deployment", "pdb", klog.KObj(pdb), "node", node.Name, "reason", reason, "podCount", len(podsOnNode))
				err := d.handle.ClientSet().PolicyV1().PodDisruptionBudgets(pdb.Namespace).Delete(ctx, pdb.Name, metav1.DeleteOptions{})
				if err != nil {
					logger.Error(err, "failed to delete PDB", "pdb", klog.KObj(pdb))
				} else {
					logger.V(1).Info("Successfully deleted PDB", "pdb", klog.KObj(pdb))
					relaxedCount++
				}
			}

		case PDBActionModify:
			if len(podsOnNode) > 0 {
				logger.V(1).Info("Modifying PDB to allow evictions", "pdb", klog.KObj(pdb), "node", node.Name, "reason", reason, "podCount", len(podsOnNode))

				// Create a copy and relax the PDB to allow all disruptions
				pdbCopy := pdb.DeepCopy()

				// Set maxUnavailable to allow all pods to be unavailable (100%)
				maxUnavailable := intstr.FromString("100%")
				pdbCopy.Spec.MaxUnavailable = &maxUnavailable
				pdbCopy.Spec.MinAvailable = nil

				_, err := d.handle.ClientSet().PolicyV1().PodDisruptionBudgets(pdb.Namespace).Update(ctx, pdbCopy, metav1.UpdateOptions{})
				if err != nil {
					logger.Error(err, "failed to modify PDB", "pdb", klog.KObj(pdb))
				} else {
					logger.V(1).Info("Successfully modified PDB", "pdb", klog.KObj(pdb), "maxUnavailable", "100%")
					relaxedCount++
				}
			}

		case PDBActionNone:
			if len(podsOnNode) > 0 {
				logger.V(2).Info("Skipping PDB action", "pdb", klog.KObj(pdb), "node", node.Name, "reason", reason, "podsOnNode", len(podsOnNode), "totalPods", len(pods))
			}
		}
	}

	if relaxedCount > 0 {
		logger.V(1).Info("Completed PDB relaxation for single-replica deployments", "node", node.Name, "relaxedCount", relaxedCount)
	}
}

// getPodsForPDB returns pods that match the PDB's selector
func (d *DefaultEvictor) getPodsForPDB(ctx context.Context, pdb *policyv1.PodDisruptionBudget) ([]*v1.Pod, error) {
	var pods []*v1.Pod

	// If PDB has no selector, it matches nothing in our logic
	if pdb.Spec.Selector == nil {
		return pods, nil
	}

	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PDB label selector: %w", err)
	}

	// List pods in the PDB's namespace
	podList, err := d.handle.ClientSet().CoreV1().Pods(pdb.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods for PDB: %w", err)
	}

	for i := range podList.Items {
		pods = append(pods, &podList.Items[i])
	}

	return pods, nil
}

// isSingleReplicaDeploymentPod checks if a pod belongs to a single-replica deployment
func (d *DefaultEvictor) isSingleReplicaDeploymentPod(ctx context.Context, pod *v1.Pod) bool {
	ownerRefs := podutil.OwnerRef(pod)
	if len(ownerRefs) == 0 {
		return false
	}

	for _, ownerRef := range ownerRefs {
		if ownerRef.Kind == "Deployment" {
			// Try to get Deployment from API client
			dep, err := d.handle.ClientSet().AppsV1().Deployments(pod.Namespace).Get(ctx, ownerRef.Name, metav1.GetOptions{})
			if err == nil && dep != nil && utils.OwnerHasSingleReplica([]metav1.OwnerReference{ownerRef}, dep) {
				return true
			}
		} else if ownerRef.Kind == "StatefulSet" {
			// Try to get StatefulSet from API client
			sts, err := d.handle.ClientSet().AppsV1().StatefulSets(pod.Namespace).Get(ctx, ownerRef.Name, metav1.GetOptions{})
			if err == nil && sts != nil && utils.OwnerHasSingleReplica([]metav1.OwnerReference{ownerRef}, sts) {
				return true
			}
		} else if ownerRef.Kind == "ReplicaSet" {
			// For ReplicaSets, follow the chain to the owning Deployment
			rs, err := d.handle.ClientSet().AppsV1().ReplicaSets(pod.Namespace).Get(ctx, ownerRef.Name, metav1.GetOptions{})
			if err != nil || rs == nil {
				continue
			}
			// Check if ReplicaSet has a Deployment owner
			for _, rsOwnerRef := range rs.GetOwnerReferences() {
				if rsOwnerRef.Kind == "Deployment" {
					dep, err := d.handle.ClientSet().AppsV1().Deployments(pod.Namespace).Get(ctx, rsOwnerRef.Name, metav1.GetOptions{})
					if err == nil && dep != nil && utils.OwnerHasSingleReplica([]metav1.OwnerReference{rsOwnerRef}, dep) {
						return true
					}
				}
			}
		}
	}

	return false
}

// shouldModifyInsteadOfDelete determines if a PDB should be MODIFIED instead of DELETED
// during node drain. This is appropriate for PDBs that:
// 1. Have minAvailable: 1 with single-replica workloads (overly restrictive)
// 2. May be auto-recreated by operators if deleted
// Modification (setting minAvailable to 0) is safer and allows temporary eviction
func (d *DefaultEvictor) shouldModifyInsteadOfDelete(pdb *policyv1.PodDisruptionBudget) bool {
	// Check if PDB has minAvailable set to 1 (the most restrictive for single-replica)
	if pdb.Spec.MinAvailable != nil && pdb.Spec.MinAvailable.IntValue() == 1 {
		return true
	}

	// Check for ownerReferences which indicate the PDB is managed by a controller
	if len(pdb.OwnerReferences) > 0 {
		return true
	}

	return false
}
