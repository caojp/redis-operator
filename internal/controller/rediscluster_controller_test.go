/*
Copyright 2026.

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

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cachev1 "github.com/caojp/redis-operator/api/v1"
)

const testNamespace = "default"

var _ = Describe("RedisCluster Controller", func() {
	ctx := context.Background()

	// newCluster 建一个 CR 并返回它的 key。
	newCluster := func(name string, mutate func(*cachev1.RedisCluster)) types.NamespacedName {
		rc := &cachev1.RedisCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec:       cachev1.RedisClusterSpec{Replicas: 3},
		}
		if mutate != nil {
			mutate(rc)
		}
		Expect(k8sClient.Create(ctx, rc)).To(Succeed())
		return types.NamespacedName{Name: name, Namespace: testNamespace}
	}

	// doReconcile 直接调一次 Reconcile，不跑 Manager，用例更可控。
	doReconcile := func(key types.NamespacedName) (ctrl.Result, error) {
		reconciler := &RedisClusterReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(32),
		}
		return reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	}

	AfterEach(func() {
		var clusters cachev1.RedisClusterList
		Expect(k8sClient.List(ctx, &clusters)).To(Succeed())
		for i := range clusters.Items {
			_ = k8sClient.Delete(ctx, &clusters.Items[i])
		}

		var stsList appsv1.StatefulSetList
		Expect(k8sClient.List(ctx, &stsList)).To(Succeed())
		for i := range stsList.Items {
			_ = k8sClient.Delete(ctx, &stsList.Items[i])
		}

		var podList corev1.PodList
		Expect(k8sClient.List(ctx, &podList)).To(Succeed())
		for i := range podList.Items {
			_ = k8sClient.Delete(ctx, &podList.Items[i])
		}
	})

	It("按照 CR 创建 Service 和 StatefulSet", func() {
		key := newCluster("cache-basic", nil)

		_, err := doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		headless := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "cache-basic-headless", Namespace: testNamespace,
		}, headless)).To(Succeed())
		Expect(headless.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		Expect(headless.Spec.PublishNotReadyAddresses).To(BeTrue())

		master := &corev1.Service{}
		Expect(k8sClient.Get(ctx, key, master)).To(Succeed())
		Expect(master.Spec.Selector).To(HaveKeyWithValue(
			"statefulset.kubernetes.io/pod-name", "cache-basic-0"))

		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
		Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
		Expect(sts.Spec.ServiceName).To(Equal("cache-basic-headless"))
		Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.OrderedReadyPodManagement))
		// image 没写在 CR 里，由 CRD 的 default 补成 redis:7.4.12
		Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal("redis:7.4.12"))
		Expect(sts.OwnerReferences).To(HaveLen(1))
		Expect(sts.OwnerReferences[0].Kind).To(Equal("RedisCluster"))
		// 没写 storage，走 emptyDir
		Expect(sts.Spec.VolumeClaimTemplates).To(BeEmpty())
		Expect(sts.Spec.Template.Spec.Volumes).To(HaveLen(1))
		Expect(sts.Spec.Template.Spec.Volumes[0].EmptyDir).NotTo(BeNil())
	})

	It("重复调谐不会改动已存在的 StatefulSet", func() {
		key := newCluster("cache-idempotent", nil)

		_, err := doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		first := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, first)).To(Succeed())

		for i := 0; i < 3; i++ {
			_, err := doReconcile(key)
			Expect(err).NotTo(HaveOccurred())
		}

		second := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, second)).To(Succeed())
		Expect(second.ResourceVersion).To(Equal(first.ResourceVersion))
	})

	It("保留 API Server 填充的默认字段，不会被 merge patch 抹掉", func() {
		key := newCluster("cache-defaults", nil)
		_, err := doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		// 模拟 API Server 的默认填充：这个字段控制器从来不管。
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
		sts.Spec.Template.Spec.Containers[0].TerminationMessagePath = "/dev/termination-log"
		sts.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
		Expect(k8sClient.Update(ctx, sts)).To(Succeed())

		_, err = doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		after := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, after)).To(Succeed())
		Expect(after.Spec.Template.Spec.Containers[0].TerminationMessagePath).
			To(Equal("/dev/termination-log"))
		Expect(after.Spec.Template.Spec.Containers[0].TerminationMessagePolicy).
			To(Equal(corev1.TerminationMessageReadFile))
	})

	It("改 replicas 会把变更同步到 StatefulSet", func() {
		key := newCluster("cache-scale", nil)
		_, err := doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		rc := &cachev1.RedisCluster{}
		Expect(k8sClient.Get(ctx, key, rc)).To(Succeed())
		rc.Spec.Replicas = 5
		Expect(k8sClient.Update(ctx, rc)).To(Succeed())

		_, err = doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
		Expect(*sts.Spec.Replicas).To(Equal(int32(5)))
	})

	It("配置 storage 时生成 PVC 模板", func() {
		className := "standard"
		key := newCluster("cache-storage", func(rc *cachev1.RedisCluster) {
			rc.Spec.Storage = &cachev1.StorageSpec{Size: "2Gi", ClassName: &className}
		})

		_, err := doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
		Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(1))
		claim := sts.Spec.VolumeClaimTemplates[0]
		Expect(*claim.Spec.StorageClassName).To(Equal("standard"))
		Expect(claim.Spec.Resources.Requests.Storage().String()).To(Equal("2Gi"))
		// 用 PVC 模板时不应该再挂 emptyDir
		Expect(sts.Spec.Template.Spec.Volumes).To(BeEmpty())
	})

	It("根据 Ready 的 Pod 回写 status", func() {
		key := newCluster("cache-status", nil)
		_, err := doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		for _, name := range []string{"cache-status-0", "cache-status-1", "cache-status-2"} {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: testNamespace,
					Labels: map[string]string{
						"app.kubernetes.io/name":     "redis",
						"app.kubernetes.io/instance": key.Name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "redis", Image: "redis:7.4.12"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				LastProbeTime:      metav1.Now(),
			}}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}

		_, err = doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		rc := &cachev1.RedisCluster{}
		Expect(k8sClient.Get(ctx, key, rc)).To(Succeed())
		Expect(rc.Status.ReadyReplicas).To(Equal(int32(3)))
		Expect(rc.Status.Master).To(Equal("cache-status-0"))
		Expect(rc.Status.ObservedGeneration).To(Equal(rc.Generation))

		available := findCondition(rc.Status.Conditions, "Available")
		Expect(available).NotTo(BeNil())
		Expect(available.Status).To(Equal(metav1.ConditionTrue))

		// 测试环境里没有真实 Redis 可连，条件应该是 Unknown 而不是崩掉
		replication := findCondition(rc.Status.Conditions, "ReplicationHealthy")
		Expect(replication).NotTo(BeNil())
		Expect(replication.Status).To(Equal(metav1.ConditionUnknown))
	})

	It("CR 删除后调谐直接返回，不报错", func() {
		key := newCluster("cache-gone", nil)
		_, err := doReconcile(key)
		Expect(err).NotTo(HaveOccurred())

		rc := &cachev1.RedisCluster{}
		Expect(k8sClient.Get(ctx, key, rc)).To(Succeed())
		Expect(k8sClient.Delete(ctx, rc)).To(Succeed())

		_, err = doReconcile(key)
		Expect(err).NotTo(HaveOccurred())
	})

	It("CRD 校验会拦掉非法字段", func() {
		// 用 unstructured 提交，才能把 replicas: 0 原样送到 API Server。
		// 如果用 Go 结构体，`omitempty` 会把 0 当空值省略，
		// 请求里就没有 replicas 字段，API Server 反而补上了默认值 3。
		newRaw := func(name string, spec map[string]interface{}) *unstructured.Unstructured {
			return &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "cache.example.com/v1",
				"kind":       "RedisCluster",
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": testNamespace,
				},
				"spec": spec,
			}}
		}

		err := k8sClient.Create(ctx, newRaw("cache-invalid", map[string]interface{}{
			"replicas": int64(0),
		}))
		Expect(err).To(HaveOccurred(), "replicas 小于下限应该被拒")
		Expect(apierrors.IsInvalid(err)).To(BeTrue())

		err = k8sClient.Create(ctx, newRaw("cache-too-many", map[string]interface{}{
			"replicas": int64(9),
		}))
		Expect(err).To(HaveOccurred(), "replicas 超过上限应该被拒")
		Expect(apierrors.IsInvalid(err)).To(BeTrue())

		err = k8sClient.Create(ctx, newRaw("cache-bad-storage", map[string]interface{}{
			"replicas": int64(3),
			"storage":  map[string]interface{}{"size": "1G"},
		}))
		Expect(err).To(HaveOccurred(), "storage.size 不符合 pattern 应该被拒")
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
	})
})

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
