package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GodisClusterSpec defines the desired state of a Godis cluster.
type GodisClusterSpec struct {
	// Mode is the godis run mode: "standalone" or "cluster"
	// +kubebuilder:validation:Enum=standalone;cluster
	Mode string `json:"mode,omitempty"`

	// Replicas is the number of godis instances (only used in cluster mode)
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`

	// Port is the Redis service port
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`

	// RaftPort is the Raft consensus port (cluster mode only)
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	RaftPort int32 `json:"raftPort,omitempty"`

	// Image is the godis container image
	Image string `json:"image,omitempty"`

	// ImagePullPolicy for the godis container
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Config overrides for standalone.toml
	Config *GodisConfig `json:"config,omitempty"`

	// Resources for the godis container
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage configures persistent storage
	Storage *StorageSpec `json:"storage,omitempty"`

	// Service annotations
	ServiceAnnotations map[string]string `json:"serviceAnnotations,omitempty"`

	// NodeSelector for pod scheduling
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for pod scheduling
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// GodisConfig holds godis configuration overrides.
type GodisConfig struct {
	Appendonly  bool   `json:"appendonly,omitempty"`
	Appendfsync string `json:"appendfsync,omitempty"`
	Maxclients  int    `json:"maxclients,omitempty"`
	Requirepass string `json:"requirepass,omitempty"`
	ExtraConfig string `json:"extraConfig,omitempty"`
}

// StorageSpec configures persistent storage.
type StorageSpec struct {
	Enabled      bool   `json:"enabled,omitempty"`
	Size         string `json:"size,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}

// GodisClusterStatus defines the observed state of a Godis cluster.
type GodisClusterStatus struct {
	// ReadyReplicas is the number of ready godis instances
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ServiceName is the name of the created Kubernetes Service
	ServiceName string `json:"serviceName,omitempty"`

	// Conditions represent the latest observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=godisclusters,scope=Namespaced,shortName=gc

// GodisCluster is the Schema for the godisclusters API.
type GodisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GodisClusterSpec   `json:"spec,omitempty"`
	Status GodisClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GodisClusterList contains a list of GodisCluster.
type GodisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GodisCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GodisCluster{}, &GodisClusterList{})
}
