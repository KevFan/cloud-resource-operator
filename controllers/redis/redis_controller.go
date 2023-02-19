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

package redis

import (
	"context"
	"fmt"
	"time"

	croType "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1/types"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers/aws"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers/openshift"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	errorUtil "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"

	integreatlyv1alpha1 "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1"
)

// RedisReconciler reconciles a Redis object
type RedisReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	logger           *logrus.Entry
	resourceProvider *resources.ReconcileResourceProvider
	providerList     []providers.RedisProvider
}

// New returns a new reconcile.Reconciler
func New(mgr manager.Manager) (*RedisReconciler, error) {
	restConfig := ctrl.GetConfigOrDie()
	restConfig.Timeout = time.Second * 10

	k8sclient, err := client.New(restConfig, client.Options{
		Scheme: mgr.GetScheme(),
	})
	if err != nil {
		return nil, err
	}

	logger := logrus.WithFields(logrus.Fields{"controller": "controller_redis"})
	awsRedisProvider, err := aws.NewAWSRedisProvider(k8sclient, logger)
	if err != nil {
		return nil, err
	}
	providerList := []providers.RedisProvider{awsRedisProvider, openshift.NewOpenShiftRedisProvider(mgr.GetClient(), logger)}
	rp := resources.NewResourceProvider(mgr.GetClient(), mgr.GetScheme(), logger)
	return &RedisReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		logger:           logger,
		resourceProvider: rp,
		providerList:     providerList,
	}, nil
}

//+kubebuilder:rbac:groups=integreatly.org,resources=redis,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=integreatly.org,resources=redis/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=integreatly.org,resources=redis/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Redis object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *RedisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.logger.Info("reconciling Redis")
	cfgMgr := providers.NewConfigManager(providers.DefaultProviderConfigMapName, req.Namespace, r.Client)

	// Fetch the Redis instance
	instance := &integreatlyv1alpha1.Redis{}
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

	stratMap, err := cfgMgr.GetStrategyMappingForDeploymentType(ctx, instance.Spec.Type)
	if err != nil {
		if updateErr := resources.UpdatePhase(ctx, r.Client, instance, croType.PhaseFailed, croType.StatusDeploymentConfigNotFound.WrapError(err)); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, errorUtil.Wrapf(err, "failed to read deployment type config for deployment %s", instance.Spec.Type)
	}

	// Check the CR for existing Strategy
	strategyToUse := stratMap.Redis
	if instance.Status.Strategy != "" {
		strategyToUse = instance.Status.Strategy
		if strategyToUse != stratMap.Redis {
			r.logger.Infof("strategy and provider already set, changing of cloud-resource-config config maps not allowed in existing installation. the existing strategy is '%s' , cloud-resource-config is now set to '%s'. operator will continue to use existing strategy", strategyToUse, stratMap.Redis)
		}
	}

	for _, p := range r.providerList {
		if !p.SupportsStrategy(strategyToUse) {
			continue
		}
		instance.Status.Strategy = strategyToUse
		instance.Status.Provider = p.GetName()
		if instance.Status.Strategy != strategyToUse || instance.Status.Provider != p.GetName() {
			if err = r.Client.Status().Update(ctx, instance); err != nil {
				return ctrl.Result{}, errorUtil.Wrapf(err, "failed to update instance %s in namespace %s", instance.Name, instance.Namespace)
			}
		}

		// handle deletion of redis and remove any finalizers added
		if instance.GetDeletionTimestamp() != nil {
			msg, err := p.DeleteRedis(ctx, instance)
			if err != nil {
				if updateErr := resources.UpdatePhase(ctx, r.Client, instance, croType.PhaseFailed, msg.WrapError(err)); updateErr != nil {
					return ctrl.Result{}, updateErr
				}
				return ctrl.Result{}, errorUtil.Wrapf(err, "failed to perform provider specific cluster deletion")
			}

			r.logger.Info("waiting for redis cluster to successfully delete")
			if err = resources.UpdatePhase(ctx, r.Client, instance, croType.PhaseDeleteInProgress, msg); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true, RequeueAfter: p.GetReconcileTime(instance)}, nil
		}

		// handle skip create
		if instance.Spec.SkipCreate {
			r.logger.Info("skipCreate found, skipping redis reconcile")
			if err := resources.UpdatePhase(ctx, r.Client, instance, croType.PhasePaused, croType.StatusSkipCreate); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true, RequeueAfter: p.GetReconcileTime(instance)}, nil
		}

		// handle creation of redis and apply any finalizers to instance required for deletion
		redis, msg, err := p.CreateRedis(ctx, instance)
		if err != nil {
			instance.Status.SecretRef = &croType.SecretRef{}
			if updateErr := resources.UpdatePhase(ctx, r.Client, instance, croType.PhaseFailed, msg.WrapError(err)); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, err
		}
		if redis == nil {
			instance.Status.SecretRef = &croType.SecretRef{}
			r.logger.Info("waiting for redis cluster to become available")
			if err = resources.UpdatePhase(ctx, r.Client, instance, croType.PhaseInProgress, msg); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true, RequeueAfter: p.GetReconcileTime(instance)}, nil
		}

		// create the secret with the redis cluster connection details
		if err := r.resourceProvider.ReconcileResultSecret(ctx, instance, redis.DeploymentDetails.Data()); err != nil {
			return ctrl.Result{}, errorUtil.Wrap(err, "failed to reconcile secret")
		}

		// update the redis custom resource
		instance.Status.Phase = croType.PhaseComplete
		instance.Status.Message = msg
		instance.Status.SecretRef = instance.Spec.SecretRef
		instance.Status.Strategy = strategyToUse
		instance.Status.Provider = p.GetName()
		if err = r.Client.Status().Update(ctx, instance); err != nil {
			return ctrl.Result{}, errorUtil.Wrapf(err, "failed to update instance %s in namespace %s", instance.Name, instance.Namespace)
		}
		return ctrl.Result{Requeue: true, RequeueAfter: p.GetReconcileTime(instance)}, nil
	}

	// unsupported strategy
	if err = resources.UpdatePhase(ctx, r.Client, instance, croType.PhaseInProgress, croType.StatusUnsupportedType.WrapError(err)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, errorUtil.New(fmt.Sprintf("unsupported deployment strategy %s", stratMap.Redis))
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&integreatlyv1alpha1.Redis{}).
		Watches(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
			IsController: true,
			OwnerType:    &integreatlyv1alpha1.Redis{},
		}).
		Complete(r)
}
