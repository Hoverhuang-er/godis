package controllers

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	godisv1 "github.com/Hoverhuang-er/godis/internal/operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
)

// GodisClusterReconciler reconciles a GodisCluster object.
type GodisClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const godisFinalizer = "godis.Hoverhuang-er.io/finalizer"

// +kubebuilder:rbac:groups=godis.Hoverhuang-er.io,resources=godisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=godis.Hoverhuang-er.io,resources=godisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=godis.Hoverhuang-er.io,resources=godisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile handles GodisCluster resource changes.
func (r *GodisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var cluster godisv1.GodisCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if cluster.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&cluster, godisFinalizer) {
			controllerutil.AddFinalizer(&cluster, godisFinalizer)
			if err := r.Update(ctx, &cluster); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		if controllerutil.ContainsFinalizer(&cluster, godisFinalizer) {
			controllerutil.RemoveFinalizer(&cluster, godisFinalizer)
			if err := r.Update(ctx, &cluster); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Reconcile ConfigMap
	cm, err := r.reconcileConfigMap(ctx, &cluster)
	if err != nil {
		logger.Error(err, "failed to reconcile ConfigMap")
		return ctrl.Result{}, err
	}

	// Reconcile Service
	svc, err := r.reconcileService(ctx, &cluster)
	if err != nil {
		logger.Error(err, "failed to reconcile Service")
		return ctrl.Result{}, err
	}

	// Reconcile StatefulSet
	sts, err := r.reconcileStatefulSet(ctx, &cluster, cm)
	if err != nil {
		logger.Error(err, "failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	// Update status
	replicas := cluster.Spec.Replicas
	if replicas == 0 {
		replicas = 1
	}
	cluster.Status.ReadyReplicas = sts.Status.ReadyReplicas
	cluster.Status.ServiceName = svc.Name

	if err := r.Status().Update(ctx, &cluster); err != nil {
		logger.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *GodisClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *godisv1.GodisCluster) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-config",
			Namespace: cluster.Namespace,
		},
	}

	mode := cluster.Spec.Mode
	if mode == "" {
		mode = "standalone"
	}

	port := cluster.Spec.Port
	if port == 0 {
		port = 6379
	}

	configContent := fmt.Sprintf(`config_mode = "%s"

[server]
bind = "0.0.0.0"
port = %d
maxclients = %d
databases = 16

[aof]
appendonly = %v
appendfsync = "%s"
aof_use_rdb_preamble = true

[slowlog]
log_slower_than = 10000
max_len = 128
`,
		mode, port,
		defaultInt(cluster.Spec.Config.Maxclients, 128),
		cluster.Spec.Config != nil && cluster.Spec.Config.Appendonly,
		configFsync(cluster.Spec.Config),
	)

	if cluster.Spec.Config != nil && cluster.Spec.Config.Requirepass != "" {
		configContent += fmt.Sprintf(`requirepass = "%s"
`, cluster.Spec.Config.Requirepass)
	}

	if mode == "cluster" {
		raftPort := cluster.Spec.RaftPort
		if raftPort == 0 {
			raftPort = 16666
		}
		configContent += fmt.Sprintf(`

[cluster]
enable = true
as_seed = true
raft_listen_address = "0.0.0.0:%d"
`, raftPort)
	}

	if cluster.Spec.Config != nil && cluster.Spec.Config.ExtraConfig != "" {
		configContent += "\n" + cluster.Spec.Config.ExtraConfig + "\n"
	}

	cm.Data = map[string]string{"standalone.toml": configContent}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		return controllerutil.SetControllerReference(cluster, cm, r.Scheme)
	})
	return cm, err
}

func (r *GodisClusterReconciler) reconcileService(ctx context.Context, cluster *godisv1.GodisCluster) (*corev1.Service, error) {
	port := cluster.Spec.Port
	if port == 0 {
		port = 6379
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = map[string]string{
			"app.kubernetes.io/name":      "godis",
			"app.kubernetes.io/instance":  cluster.Name,
			"app.kubernetes.io/component": "godis-cluster",
		}
		if cluster.Spec.ServiceAnnotations != nil {
			svc.Annotations = cluster.Spec.ServiceAnnotations
		}
		svc.Spec.Selector = map[string]string{
			"app.kubernetes.io/name":     "godis",
			"app.kubernetes.io/instance": cluster.Name,
		}
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "redis",
				Port:       port,
				TargetPort: intstr.FromString("redis"),
				Protocol:   corev1.ProtocolTCP,
			},
		}
		if cluster.Spec.Mode == "cluster" {
			raftPort := cluster.Spec.RaftPort
			if raftPort == 0 {
				raftPort = 16666
			}
			svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
				Name:       "raft",
				Port:       raftPort,
				TargetPort: intstr.FromString("raft"),
				Protocol:   corev1.ProtocolTCP,
			})
		}
		return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
	})
	return svc, err
}

func (r *GodisClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *godisv1.GodisCluster, cm *corev1.ConfigMap) (*appsv1.StatefulSet, error) {
	replicas := cluster.Spec.Replicas
	if replicas == 0 {
		if cluster.Spec.Mode == "cluster" {
			replicas = 3
		} else {
			replicas = 1
		}
	}

	image := cluster.Spec.Image
	if image == "" {
		image = "ghcr.io/Hoverhuang-er/godis:latest"
	}

	pullPolicy := cluster.Spec.ImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = corev1.PullIfNotPresent
	}

	port := cluster.Spec.Port
	if port == 0 {
		port = 6379
	}

	labels := map[string]string{
		"app.kubernetes.io/name":     "godis",
		"app.kubernetes.io/instance": cluster.Name,
	}

	// Default resources: 0.5 CPU, 1Gi memory
	resources := cluster.Spec.Resources
	if resources.Requests == nil && resources.Limits == nil {
		resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1000m"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
		}
	}

	// Autoscaling annotations for VPA and KEDA
	annotations := map[string]string{}
	if cluster.Spec.Autoscaling != nil && cluster.Spec.Autoscaling.EnableVPA {
		annotations["vpa.godis.Hoverhuang-er.io/enabled"] = "true"
	}
	if cluster.Spec.Autoscaling != nil && cluster.Spec.Autoscaling.EnableKEDA {
		annotations["keda.godis.Hoverhuang-er.io/enabled"] = "true"
		annotations["keda.godis.Hoverhuang-er.io/scalingStrategy"] = "default"
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		if sts.CreationTimestamp.IsZero() {
			sts.Annotations = annotations
		}
		sts.Labels = labels
		sts.Spec.ServiceName = cluster.Name
		sts.Spec.Replicas = &replicas
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		sts.Spec.Template.ObjectMeta.Labels = labels
		if len(annotations) > 0 {
			if sts.Spec.Template.Annotations == nil {
				sts.Spec.Template.Annotations = make(map[string]string)
			}
			for k, v := range annotations {
				sts.Spec.Template.Annotations[k] = v
			}
		}

		container := corev1.Container{
			Name:            "godis",
			Image:           image,
			ImagePullPolicy: pullPolicy,
			Env: []corev1.EnvVar{
				{Name: "CONFIG", Value: "/etc/godis/standalone.toml"},
			},
			Ports: []corev1.ContainerPort{
				{Name: "redis", ContainerPort: port, Protocol: corev1.ProtocolTCP},
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "config", MountPath: "/etc/godis", ReadOnly: true},
			},
			Resources: resources,
		}

		if cluster.Spec.Mode == "cluster" {
			raftPort := cluster.Spec.RaftPort
			if raftPort == 0 {
				raftPort = 16666
			}
			container.Ports = append(container.Ports, corev1.ContainerPort{
				Name: "raft", ContainerPort: raftPort, Protocol: corev1.ProtocolTCP,
			})
		}

		sts.Spec.Template.Spec.Containers = []corev1.Container{container}
		sts.Spec.Template.Spec.Volumes = []corev1.Volume{
			{Name: "config", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cm.Name},
				},
			}},
		}

		return controllerutil.SetControllerReference(cluster, sts, r.Scheme)
	})
	return sts, err
}

func defaultInt(val int, def int) int {
	if val == 0 {
		return def
	}
	return val
}

func configFsync(cfg *godisv1.GodisConfig) string {
	if cfg != nil && cfg.Appendfsync != "" {
		return cfg.Appendfsync
	}
	return "everysec"
}

// SetupWithManager registers the controller with the manager.
func (r *GodisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&godisv1.GodisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}
