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
	"fmt"
	"net"
	"reflect"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1 "github.com/caojp/redis-operator/api/v1"
	"github.com/caojp/redis-operator/internal/redisinfo"
)

const (
	// redisPort 是容器内 Redis 监听端口。
	redisPort int32 = 6379
	// dataVolumeName 是数据盘在 Pod 里的卷名，PVC 模板和 emptyDir 都用它。
	dataVolumeName = "data"
	// dataMountPath 要与启动脚本里的 --dir 保持一致。
	dataMountPath = "/data"
	// headlessSuffix 是 headless Service 的后缀，为每个 Pod 提供稳定 DNS。
	headlessSuffix = "-headless"
	// containerName 是 Redis 容器名，exec 探针和日志排查都依赖它。
	containerName = "redis"
	// requeueInterval 是集群未就绪时的重查间隔。
	requeueInterval = 10 * time.Second
	// redisTimeout 是探测 Redis 的超时时间，宁可探不到也不能拖住工作队列。
	redisTimeout = 2 * time.Second
)

// startupScript 由 Pod 序号决定自己当主还是当从。
// StatefulSet 的 Pod 名字固定为 <name>-<ordinal>，序号可以稳定地当作角色依据。
const startupScript = `set -eu
ORDINAL="${HOSTNAME##*-}"
FLAGS="--port 6379 --dir /data --protected-mode no --appendonly yes --replica-announce-ip ${POD_IP:-127.0.0.1}"
if [ -n "${REDIS_PASSWORD:-}" ]; then
  FLAGS="$FLAGS --requirepass ${REDIS_PASSWORD} --masterauth ${REDIS_PASSWORD}"
fi
if [ "${ORDINAL}" = "0" ]; then
  echo "[entrypoint] 以主节点身份启动"
  exec redis-server $FLAGS
fi
echo "[entrypoint] 以从节点身份启动，跟随 ${MASTER_HOST}:6379"
exec redis-server $FLAGS --replicaof "${MASTER_HOST}" 6379
`

// pingScript 是存活/就绪探针用的脚本，兼容开了密码的场景。
const pingScript = `if [ -n "${REDIS_PASSWORD:-}" ]; then
  redis-cli --no-auth-warning -a "${REDIS_PASSWORD}" -h 127.0.0.1 -p 6379 ping
else
  redis-cli -h 127.0.0.1 -p 6379 ping
fi
`

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=cache.example.com,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.example.com,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.example.com,resources=redisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile 把集群的实际状态拉向 RedisCluster 里声明的期望状态。
// 每一步都先读再写，重复执行不会产生额外副作用。
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. 读 CR。读不到通常意味着已被删除，直接结束即可。
	var rc cachev1.RedisCluster
	if err := r.Get(ctx, req.NamespacedName, &rc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. 逐个调谐子资源。顺序有意义：Service 先于 Pod 存在，
	//    StatefulSet 拉起 Pod 时才能解析到自己的稳定域名。
	if err := r.reconcileHeadlessService(ctx, &rc); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileMasterService(ctx, &rc); err != nil {
		return ctrl.Result{}, err
	}
	sts, err := r.reconcileStatefulSet(ctx, &rc)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. 观测：就绪 Pod 数、主节点连了几个从节点。
	observed, err := r.observe(ctx, &rc)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 4. 回写 status。只有内容真的变了才写，避免自我触发的无限循环。
	if err := r.updateStatus(ctx, &rc, observed); err != nil {
		return ctrl.Result{}, err
	}

	// 5. 没到期望副本数就主动回来再看一眼；到齐了就完全交给事件驱动。
	if observed.ReadyReplicas < rc.Spec.Replicas || sts.Status.ReadyReplicas < rc.Spec.Replicas {
		log.V(1).Info("集群尚未就绪，稍后再查", "ready", observed.ReadyReplicas, "desired", rc.Spec.Replicas)
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Named("rediscluster").
		Complete(r)
}

// ---------------------------------------------------------------------------
// 子资源调谐
// ---------------------------------------------------------------------------

// reconcileHeadlessService 维护给 StatefulSet 用的 headless Service。
// clusterIP: None 之后，Pod 就能通过 <pod>.<svc>.<ns>.svc.cluster.local 互相找到。
func (r *RedisClusterReconciler) reconcileHeadlessService(ctx context.Context, rc *cachev1.RedisCluster) error {
	log := logf.FromContext(ctx)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: headlessName(rc), Namespace: rc.Namespace},
	}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = objectLabels(rc)
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.Selector = selectorLabels(rc)
		// 未就绪也发布 DNS 记录，否则从节点启动时解析不到主节点。
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "redis",
			Port:       redisPort,
			TargetPort: intstr.FromInt32(redisPort),
			Protocol:   corev1.ProtocolTCP,
		}}
		return controllerutil.SetControllerReference(rc, svc, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("调谐 headless Service %s 失败: %w", svc.Name, err)
	}
	if op != controllerutil.OperationResultNone {
		log.Info("headless Service 已同步", "service", svc.Name, "operation", op)
	}
	return nil
}

// reconcileMasterService 维护业务用的读写入口。
// 选择器锁定到 pod-0，客户端永远连当前主节点，不用自己维护地址。
func (r *RedisClusterReconciler) reconcileMasterService(ctx context.Context, rc *cachev1.RedisCluster) error {
	log := logf.FromContext(ctx)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: rc.Name, Namespace: rc.Namespace},
	}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = objectLabels(rc)
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.Selector = withPodName(selectorLabels(rc), masterPodName(rc))
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "redis",
			Port:       redisPort,
			TargetPort: intstr.FromInt32(redisPort),
			Protocol:   corev1.ProtocolTCP,
		}}
		return controllerutil.SetControllerReference(rc, svc, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("调谐读写 Service %s 失败: %w", svc.Name, err)
	}
	if op != controllerutil.OperationResultNone {
		log.Info("读写 Service 已同步", "service", svc.Name, "operation", op)
	}
	return nil
}

// reconcileStatefulSet 维护 Redis 实例本身，并把最新的对象返回给上层做观测。
func (r *RedisClusterReconciler) reconcileStatefulSet(
	ctx context.Context, rc *cachev1.RedisCluster,
) (*appsv1.StatefulSet, error) {
	log := logf.FromContext(ctx)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: rc.Name, Namespace: rc.Namespace},
	}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		r.mutateStatefulSet(rc, sts)
		return controllerutil.SetControllerReference(rc, sts, r.Scheme)
	})
	if err != nil {
		return nil, fmt.Errorf("调谐 StatefulSet %s 失败: %w", sts.Name, err)
	}
	if op != controllerutil.OperationResultNone {
		log.Info("StatefulSet 已同步", "statefulset", sts.Name, "operation", op)
		r.event(rc, corev1.EventTypeNormal, "StatefulSetSynced",
			fmt.Sprintf("StatefulSet %s %s", sts.Name, op))
	}
	return sts, nil
}

// mutateStatefulSet 只负责把对象改成"期望的样子"。
// CreateOrUpdate 会先读出已存在的对象，所以写法要同时适用于新建和更新。
func (r *RedisClusterReconciler) mutateStatefulSet(rc *cachev1.RedisCluster, sts *appsv1.StatefulSet) {
	replicas := rc.Spec.Replicas
	gracePeriod := int64(30)

	sts.Labels = objectLabels(rc)
	sts.Spec.Replicas = &replicas
	sts.Spec.ServiceName = headlessName(rc)
	sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels(rc)}
	// 逐个启动：pod-0 起好之后才拉 pod-1，从节点才不会空转重连。
	sts.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
	sts.Spec.MinReadySeconds = 3

	sts.Spec.Template.ObjectMeta.Labels = objectLabels(rc)
	sts.Spec.Template.Spec.TerminationGracePeriodSeconds = &gracePeriod
	r.mergeRedisContainer(rc, &sts.Spec.Template.Spec)

	// VolumeClaimTemplates 在 StatefulSet 创建之后不可变，
	// 所以只在这个对象还没落库（CreationTimestamp 为空）时下发。
	if sts.CreationTimestamp.IsZero() {
		if claim := r.dataClaimTemplate(rc); claim != nil {
			sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{*claim}
			sts.Spec.Template.Spec.Volumes = nil
		} else {
			sts.Spec.VolumeClaimTemplates = nil
			sts.Spec.Template.Spec.Volumes = []corev1.Volume{{
				Name:         dataVolumeName,
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}}
		}
	}
}

// redisContainer 生成 Redis 容器定义。
func (r *RedisClusterReconciler) redisContainer(rc *cachev1.RedisCluster) corev1.Container {
	env := []corev1.EnvVar{
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					// 显式写上 apiVersion，避免 API Server 补默认值后每次都比出差异
					APIVersion: "v1",
					FieldPath:  "status.podIP",
				},
			},
		},
		{Name: "MASTER_HOST", Value: masterHost(rc)},
	}
	if rc.Spec.Auth != nil {
		env = append(env, corev1.EnvVar{
			Name: "REDIS_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: rc.Spec.Auth.SecretName},
					Key:                  secretKey(rc),
				},
			},
		})
	}

	return corev1.Container{
		Name:            containerName,
		Image:           rc.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/bin/sh", "-c"},
		Args:            []string{startupScript},
		Ports: []corev1.ContainerPort{{
			Name:          "redis",
			ContainerPort: redisPort,
			Protocol:      corev1.ProtocolTCP,
		}},
		Env:            env,
		Resources:      rc.Spec.Resources,
		VolumeMounts:   []corev1.VolumeMount{{Name: dataVolumeName, MountPath: dataMountPath}},
		ReadinessProbe: redisProbe(5, 3),
		LivenessProbe:  redisProbe(30, 3),
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: boolPtr(false),
		},
	}
}

// mergeRedisContainer 只覆盖控制器负责的字段，保留 API Server 默认填充的内容。
// 如果整体替换容器列表，merge patch 会把 terminationMessagePath 这类默认值
// 当成"要删除"的字段，于是每轮调谐都写一次 StatefulSet，日志里刷满 updated。
func (r *RedisClusterReconciler) mergeRedisContainer(rc *cachev1.RedisCluster, podSpec *corev1.PodSpec) {
	desired := r.redisContainer(rc)

	if len(podSpec.Containers) != 1 || podSpec.Containers[0].Name != desired.Name {
		podSpec.Containers = []corev1.Container{desired}
		return
	}

	target := &podSpec.Containers[0]
	target.Image = desired.Image
	target.ImagePullPolicy = desired.ImagePullPolicy
	target.Command = desired.Command
	target.Args = desired.Args
	target.Ports = desired.Ports
	target.Env = desired.Env
	target.Resources = desired.Resources
	target.VolumeMounts = desired.VolumeMounts
	target.ReadinessProbe = desired.ReadinessProbe
	target.LivenessProbe = desired.LivenessProbe
	target.SecurityContext = desired.SecurityContext
}

// redisProbe 返回统一风格的 exec 探针。
func redisProbe(initialDelay, failureThreshold int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", pingScript}},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       10,
		TimeoutSeconds:      3,
		FailureThreshold:    failureThreshold,
		SuccessThreshold:    1,
	}
}

// dataClaimTemplate 生成 PVC 模板；没配 storage 时返回 nil，改走 emptyDir。
func (r *RedisClusterReconciler) dataClaimTemplate(rc *cachev1.RedisCluster) *corev1.PersistentVolumeClaim {
	if rc.Spec.Storage == nil {
		return nil
	}
	size := rc.Spec.Storage.Size
	if size == "" {
		size = "1Gi"
	}

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: dataVolumeName},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
	if rc.Spec.Storage.ClassName != nil && *rc.Spec.Storage.ClassName != "" {
		claim.Spec.StorageClassName = rc.Spec.Storage.ClassName
	}
	return claim
}

// ---------------------------------------------------------------------------
// 观测与状态
// ---------------------------------------------------------------------------

// observe 汇总当前实际状态，供 status 回写使用。
func (r *RedisClusterReconciler) observe(
	ctx context.Context, rc *cachev1.RedisCluster,
) (cachev1.RedisClusterStatus, error) {
	status := cachev1.RedisClusterStatus{
		ObservedGeneration: rc.Generation,
		Master:             masterPodName(rc),
		Conditions:         rc.Status.Conditions,
	}

	pods, err := r.readyPods(ctx, rc)
	if err != nil {
		return status, err
	}
	status.ReadyReplicas = int32(len(pods))

	connected, redisErr := r.connectedReplicas(ctx, rc, pods)
	if redisErr != nil {
		status.ConnectedReplicas = 0
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               "ReplicationHealthy",
			Status:             metav1.ConditionUnknown,
			Reason:             "MasterUnreachable",
			Message:            redisErr.Error(),
			ObservedGeneration: rc.Generation,
		})
	} else {
		status.ConnectedReplicas = connected
		want := rc.Spec.Replicas - 1
		condStatus := metav1.ConditionFalse
		reason := "ReplicasNotConnected"
		if connected == want {
			condStatus = metav1.ConditionTrue
			reason = "AllReplicasConnected"
		}
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               "ReplicationHealthy",
			Status:             condStatus,
			Reason:             reason,
			Message:            fmt.Sprintf("主节点 %s 当前连接 %d/%d 个从节点", masterPodName(rc), connected, want),
			ObservedGeneration: rc.Generation,
		})
	}

	allReady := status.ReadyReplicas == rc.Spec.Replicas
	availStatus := metav1.ConditionFalse
	availReason := "InstancesNotReady"
	if allReady {
		availStatus = metav1.ConditionTrue
		availReason = "AllInstancesReady"
	}
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               "Available",
		Status:             availStatus,
		Reason:             availReason,
		Message:            fmt.Sprintf("%d/%d 个实例已就绪", status.ReadyReplicas, rc.Spec.Replicas),
		ObservedGeneration: rc.Generation,
	})

	progressStatus := metav1.ConditionTrue
	progressReason := "Reconciling"
	if allReady && redisErr == nil && connected == rc.Spec.Replicas-1 {
		progressStatus = metav1.ConditionFalse
		progressReason = "ReconcileComplete"
	}
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               "Progressing",
		Status:             progressStatus,
		Reason:             progressReason,
		Message:            fmt.Sprintf("generation %d 已处理", rc.Generation),
		ObservedGeneration: rc.Generation,
	})

	return status, nil
}

// readyPods 只统计真正 Ready 的 Pod。StatefulSet 上也有一份 ReadyReplicas，
// 但 Pod 维度更贴近实际情况，也能在 envtest 里独立构造。
func (r *RedisClusterReconciler) readyPods(
	ctx context.Context, rc *cachev1.RedisCluster,
) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(rc.Namespace),
		client.MatchingLabels(selectorLabels(rc)),
	); err != nil {
		return nil, fmt.Errorf("列出 Redis Pod 失败: %w", err)
	}

	ready := make([]corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pod := podList.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if isPodReady(&pod) {
			ready = append(ready, pod)
		}
	}
	return ready, nil
}

// connectedReplicas 连到主节点问一句"你下面挂了几个从节点"。
// 读不到不算错误——集群刚创建时主节点还没起，属于正常现象。
func (r *RedisClusterReconciler) connectedReplicas(
	ctx context.Context, rc *cachev1.RedisCluster, pods []corev1.Pod,
) (int32, error) {
	master := masterPodName(rc)
	var masterPod *corev1.Pod
	for i := range pods {
		if pods[i].Name == master {
			masterPod = &pods[i]
			break
		}
	}
	if masterPod == nil {
		return 0, fmt.Errorf("主节点 %s 尚未就绪", master)
	}
	if masterPod.Status.PodIP == "" {
		return 0, fmt.Errorf("主节点 %s 还没有分配到 IP", master)
	}

	password, err := r.redisPassword(ctx, rc)
	if err != nil {
		return 0, err
	}

	addr := net.JoinHostPort(masterPod.Status.PodIP, strconv.Itoa(int(redisPort)))
	info, err := redisinfo.Fetch(ctx, addr, password, redisTimeout)
	if err != nil {
		return 0, fmt.Errorf("读取主节点 %s 状态失败: %w", master, err)
	}
	if info.Role != "master" {
		return 0, fmt.Errorf("%s 的角色是 %s，不是 master", master, info.Role)
	}
	return info.ConnectedSlaves, nil
}

// redisPassword 从 Secret 里取密码；没配 auth 时返回空串。
func (r *RedisClusterReconciler) redisPassword(ctx context.Context, rc *cachev1.RedisCluster) (string, error) {
	if rc.Spec.Auth == nil {
		return "", nil
	}
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: rc.Namespace, Name: rc.Spec.Auth.SecretName}
	if err := r.Get(ctx, key, &secret); err != nil {
		return "", fmt.Errorf("读取 Secret %s 失败: %w", key, err)
	}
	raw, ok := secret.Data[secretKey(rc)]
	if !ok {
		return "", fmt.Errorf("Secret %s 里没有键 %s", key, secretKey(rc))
	}
	return string(raw), nil
}

// updateStatus 只在内容变化时提交，避免 status 更新触发下一轮 Reconcile 形成死循环。
func (r *RedisClusterReconciler) updateStatus(
	ctx context.Context, rc *cachev1.RedisCluster, observed cachev1.RedisClusterStatus,
) error {
	if statusEqual(rc.Status, observed) {
		return nil
	}
	rc.Status = observed
	if err := r.Status().Update(ctx, rc); err != nil {
		return fmt.Errorf("回写 RedisCluster %s 的 status 失败: %w", rc.Name, err)
	}
	return nil
}

func statusEqual(a, b cachev1.RedisClusterStatus) bool {
	return a.ObservedGeneration == b.ObservedGeneration &&
		a.ReadyReplicas == b.ReadyReplicas &&
		a.Master == b.Master &&
		a.ConnectedReplicas == b.ConnectedReplicas &&
		reflect.DeepEqual(a.Conditions, b.Conditions)
}

// event 在没挂 Recorder（例如单元测试）时静默跳过。
func (r *RedisClusterReconciler) event(rc *cachev1.RedisCluster, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(rc, eventType, reason, message)
}

// ---------------------------------------------------------------------------
// 命名与标签
// ---------------------------------------------------------------------------

func headlessName(rc *cachev1.RedisCluster) string {
	return rc.Name + headlessSuffix
}

func masterPodName(rc *cachev1.RedisCluster) string {
	return rc.Name + "-0"
}

// masterHost 是 pod-0 的稳定域名，从节点靠它找到主节点。
func masterHost(rc *cachev1.RedisCluster) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", masterPodName(rc), headlessName(rc), rc.Namespace)
}

func secretKey(rc *cachev1.RedisCluster) string {
	if rc.Spec.Auth != nil && rc.Spec.Auth.SecretKey != "" {
		return rc.Spec.Auth.SecretKey
	}
	return "password"
}

// selectorLabels 是不可变的选择器，Pod、Service、StatefulSet 共用。
func selectorLabels(rc *cachev1.RedisCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "redis",
		"app.kubernetes.io/instance": rc.Name,
	}
}

// objectLabels 在选择器之外再加几个描述性标签。
func objectLabels(rc *cachev1.RedisCluster) map[string]string {
	labels := selectorLabels(rc)
	labels["app.kubernetes.io/component"] = "cache"
	labels["app.kubernetes.io/managed-by"] = "redis-operator"
	return labels
}

// withPodName 复制一份标签并加上 Pod 名，用于把 Service 精确指向某个 Pod。
func withPodName(labels map[string]string, podName string) map[string]string {
	out := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		out[k] = v
	}
	out["statefulset.kubernetes.io/pod-name"] = podName
	return out
}

func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func boolPtr(b bool) *bool {
	return &b
}
