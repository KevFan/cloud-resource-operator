/*
Copyright 2023.

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

package postgressnapshot

import (
	"context"
	"fmt"
	"time"

	croType "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1/types"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	croAws "github.com/integr8ly/cloud-resource-operator/pkg/providers/aws"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	errorUtil "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"

	integreatlyv1alpha1 "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1"
)

const (
	postgresProviderName = "aws-rds"
)

// PostgresSnapshotReconciler reconciles a PostgresSnapshot object
type PostgresSnapshotReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	logger        *logrus.Entry
	provider      providers.PostgresSnapshotProvider
	ConfigManager croAws.ConfigManager
}

func New(mgr manager.Manager) (*PostgresSnapshotReconciler, error) {
	restConfig := ctrl.GetConfigOrDie()
	restConfig.Timeout = time.Second * 10

	k8sclient, err := client.New(restConfig, client.Options{
		Scheme: mgr.GetScheme(),
	})
	if err != nil {
		return nil, err
	}

	logger := logrus.WithFields(logrus.Fields{"controller": "controller_postgres_snapshot"})
	awsPostgresSnapshotProvider, err := croAws.NewAWSPostgresSnapshotProvider(k8sclient, logger)
	if err != nil {
		return nil, err
	}
	return &PostgresSnapshotReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		logger:        logger,
		provider:      awsPostgresSnapshotProvider,
		ConfigManager: croAws.NewDefaultConfigMapConfigManager(mgr.GetClient()),
	}, nil
}

//+kubebuilder:rbac:groups=integreatly.org,resources=postgressnapshots,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=integreatly.org,resources=postgressnapshots/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=integreatly.org,resources=postgressnapshots/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PostgresSnapshot object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *PostgresSnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.logger.Info("reconciling postgres snapshot")

	// Fetch the PostgresSnapshot instance
	instance := &integreatlyv1alpha1.PostgresSnapshot{}
	err := r.Client.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return ctrl.Result{}, err
	}

	// set info metric
	defer r.exposePostgresSnapshotMetrics(ctx, instance)

	// get postgres cr
	postgresCr := &integreatlyv1alpha1.Postgres{}
	err = r.Client.Get(ctx, types.NamespacedName{Name: instance.Spec.ResourceName, Namespace: instance.Namespace}, postgresCr)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get postgres resource: %s", err.Error())
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.Client, instance, croType.PhaseFailed, croType.StatusMessage(errMsg)); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, errorUtil.New(errMsg)
	}

	// check postgres deployment strategy is aws
	if !r.provider.SupportsStrategy(postgresCr.Status.Strategy) {
		errMsg := fmt.Sprintf("the resource %s uses an unsupported provider strategy %s, only resources using the aws provider are valid", instance.Spec.ResourceName, postgresCr.Status.Strategy)
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.Client, instance, croType.PhaseFailed, croType.StatusMessage(errMsg)); updateErr != nil {
			return ctrl.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, updateErr
		}
		return ctrl.Result{}, errorUtil.New(errMsg)
	}

	if instance.DeletionTimestamp != nil {
		msg, err := r.provider.DeletePostgresSnapshot(ctx, instance, postgresCr)
		if err != nil {
			if updateErr := resources.UpdateSnapshotPhase(ctx, r.Client, instance, croType.PhaseFailed, msg.WrapError(err)); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, errorUtil.Wrapf(err, "failed to delete postgres snapshot")
		}

		r.logger.Info("waiting on postgres snapshot to successfully delete")
		if err = resources.UpdateSnapshotPhase(ctx, r.Client, instance, croType.PhaseDeleteInProgress, msg); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, nil
	}

	// check status, if complete return
	if instance.Status.Phase == croType.PhaseComplete {
		r.logger.Infof("skipping creation of snapshot for %s as phase is complete", instance.Name)
		return ctrl.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, nil
	}

	// create the snapshot and return the phase
	snap, msg, err := r.provider.CreatePostgresSnapshot(ctx, instance, postgresCr)

	// error trying to create snapshot
	if err != nil {
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.Client, instance, croType.PhaseFailed, msg); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, err
	}

	// no error but the snapshot doesn't exist yet
	if snap == nil {
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.Client, instance, croType.PhaseInProgress, msg); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, nil
	}

	// no error, snapshot exists
	if updateErr := resources.UpdateSnapshotPhase(ctx, r.Client, instance, croType.PhaseComplete, msg); updateErr != nil {
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, nil

}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&integreatlyv1alpha1.PostgresSnapshot{}).
		Watches(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
			IsController: true,
			OwnerType:    &integreatlyv1alpha1.PostgresSnapshot{},
		}).
		Complete(r)
}

func buildPostgresSnapshotStatusMetricLabels(cr *integreatlyv1alpha1.PostgresSnapshot, clusterID, snapshotName string, phase croType.StatusPhase) map[string]string {
	labels := map[string]string{}
	labels["clusterID"] = clusterID
	labels["resourceID"] = cr.Name
	labels["namespace"] = cr.Namespace
	labels["instanceID"] = snapshotName
	labels["productName"] = cr.Labels["productName"]
	labels["strategy"] = postgresProviderName
	labels["statusPhase"] = string(phase)
	return labels
}

func (r *PostgresSnapshotReconciler) exposePostgresSnapshotMetrics(ctx context.Context, cr *integreatlyv1alpha1.PostgresSnapshot) {
	// build instance name
	snapshotName := cr.Status.SnapshotID

	// get Cluster Id
	logrus.Info("setting postgres snapshot information metric")
	clusterID, err := resources.GetClusterID(ctx, r.Client)
	if err != nil {
		logrus.Errorf("failed to get cluster id while exposing information metric for %v", snapshotName)
		return
	}

	// set generic status metrics
	// a single metric should be exposed for each possible phase
	// the value of the metric should be 1.0 when the resource is in that phase
	// the value of the metric should be 0.0 when the resource is not in that phase
	// this follows the approach that pod status
	for _, phase := range []croType.StatusPhase{croType.PhaseFailed, croType.PhaseDeleteInProgress, croType.PhasePaused, croType.PhaseComplete, croType.PhaseInProgress} {
		labelsFailed := buildPostgresSnapshotStatusMetricLabels(cr, clusterID, snapshotName, phase)
		resources.SetMetric(resources.DefaultPostgresSnapshotStatusMetricName, labelsFailed, resources.Btof64(cr.Status.Phase == phase))
	}
}
