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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// AuthSpec 描述 Redis 的密码认证方式。密码本身放在 Secret 里，
// CR 里只保存引用，避免明文密码进入 Git。
type AuthSpec struct {
	// SecretName 是存放密码的 Secret 名称，必须与 RedisCluster 同命名空间。
	// +kubebuilder:validation:MinLength=1
	SecretName string `json:"secretName"`

	// SecretKey 是 Secret 中存放密码的键名。
	// +kubebuilder:default="password"
	// +optional
	SecretKey string `json:"secretKey,omitempty"`
}

// StorageSpec 描述数据盘。不填则使用 emptyDir，仅适合临时验证。
type StorageSpec struct {
	// Size 是每个实例申请的数据盘容量，形如 1Gi。
	// +kubebuilder:default="1Gi"
	// +kubebuilder:validation:Pattern=`^[0-9]+(Mi|Gi|Ti)$`
	// +optional
	Size string `json:"size,omitempty"`

	// ClassName 指定 StorageClass；留空则使用集群默认 StorageClass。
	// +optional
	ClassName *string `json:"className,omitempty"`
}

// RedisClusterSpec defines the desired state of RedisCluster
type RedisClusterSpec struct {
	// Replicas 是 Redis 实例总数，其中 1 个主节点，其余为从节点。
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=6
	// +kubebuilder:default=3
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Image 是 Redis 容器镜像，建议固定到具体版本，避免隐式升级。
	// +kubebuilder:default="redis:7.4.12"
	// +optional
	Image string `json:"image,omitempty"`

	// Resources 是每个 Redis 容器的资源声明。
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage 是数据盘配置。
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// Auth 是密码认证配置。
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`
}

// RedisClusterStatus defines the observed state of RedisCluster.
type RedisClusterStatus struct {
	// ObservedGeneration 是控制器最后一次处理过的 spec 版本。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ReadyReplicas 是当前处于 Ready 状态的 Redis Pod 数量。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`

	// Master 是当前主节点对应的 Pod 名称。
	// +optional
	Master string `json:"master,omitempty"`

	// ConnectedReplicas 是从主节点 INFO replication 里读到的已连接从节点数。
	// +optional
	ConnectedReplicas int32 `json:"connectedReplicas"`

	// conditions represent the current state of the RedisCluster resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rcl
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Master",type=string,JSONPath=`.status.master`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RedisCluster is the Schema for the redisclusters API
type RedisCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RedisCluster
	// +required
	Spec RedisClusterSpec `json:"spec"`

	// status defines the observed state of RedisCluster
	// +optional
	Status RedisClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RedisCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &RedisCluster{}, &RedisClusterList{})
		return nil
	})
}
