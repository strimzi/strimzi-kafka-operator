/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;

/**
 * This class contains the methods for migrating Kafka Connect and Kafka Mirror Maker 2 clusters between Deployments
 * and StrimziPodSets and the other way.
 */
public class KafkaConnectMigration {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaConnectMigration.class.getName());

    private final Reconciliation reconciliation;
    private final KafkaConnectCluster connect;

    private final Map<String, String> controllerAnnotations;
    private final Map<String, String> podAnnotations;
    private final long operationTimeoutMs;
    private final boolean isOpenshift;
    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;
    private final String customContainerImage;

    private final DeploymentOperator deploymentOperator;
    private final StrimziPodSetOperator podSetOperator;
    private final PodOperator podOperator;

    /**
     * Constructs the Connect migration tool
     *
     * @param reconciliation            Reconciliation marker
     * @param connect                   Kafka Connect Cluster model
     * @param controllerAnnotations     Map with controller annotations
     * @param podAnnotations            Map with Pod annotations
     * @param operationTimeoutMs        Operations timeout in milliseconds
     * @param isOpenshift               Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy           Image pull policy
     * @param imagePullSecrets          List of image pull secrets
     * @param customContainerImage      Container image built by Kafka Connect Build
     * @param deploymentOperator        Operator for managing Deployments
     * @param podSetOperator            Operator for managing StrimziPodSets
     * @param podOperator               Resource Operator for managing Pods
     */
    public KafkaConnectMigration(
            Reconciliation reconciliation,
            KafkaConnectCluster connect,
            Map<String, String> controllerAnnotations,
            Map<String, String> podAnnotations,
            long operationTimeoutMs,
            boolean isOpenshift,
            ImagePullPolicy imagePullPolicy,
            List<LocalObjectReference> imagePullSecrets,
            String customContainerImage,
            DeploymentOperator deploymentOperator,
            StrimziPodSetOperator podSetOperator,
            PodOperator podOperator
    ) {
        this.reconciliation = reconciliation;
        this.connect = connect;

        this.controllerAnnotations = controllerAnnotations;
        this.podAnnotations = podAnnotations;
        this.operationTimeoutMs = operationTimeoutMs;
        this.isOpenshift = isOpenshift;
        this.imagePullPolicy = imagePullPolicy;
        this.imagePullSecrets = imagePullSecrets;
        this.customContainerImage = customContainerImage;

        this.deploymentOperator = deploymentOperator;
        this.podSetOperator = podSetOperator;
        this.podOperator = podOperator;
    }

    /**
     * Migrates Kafka Connect or Kafka Mirror Maker 2 from Deployment to StrimziPodSet
     *
     * @param deployment    Current Deployment resource
     * @param podSet        Current StrimziPodSet resource
     *
     * @return  Future which completes when the migration is completed
     */
    public Future<Void> migrateFromDeploymentToStrimziPodSets(Deployment deployment, StrimziPodSet podSet)    {
        if (deployment == null) {
            // Deployment does not exist anymore => no migration needed
            return Future.succeededFuture();
        } else {
            int depReplicas = deployment.getSpec().getReplicas();
            int podSetReplicas = podSet != null ? podSet.getSpec().getPods().size() : 0;

            return moveOnePodFromDeploymentToStrimziPodSet(depReplicas - 1, Math.min(podSetReplicas + 1, connect.getReplicas()));
        }
    }

    /**
     * Moves one Pod from Deployment to StrimziPodSet in the order defined by the deploymentStrategy
     *
     * @param desiredDeploymentReplicas     Number of desired Deployment replicas in this step
     * @param desiredPodSetReplicas         Number of desired PodSets in this step
     *
     * @return Future which completes when the Pod is moved
     */
    private Future<Void> moveOnePodFromDeploymentToStrimziPodSet(int desiredDeploymentReplicas, int desiredPodSetReplicas)  {
        if (DeploymentStrategy.ROLLING_UPDATE.equals(connect.deploymentStrategy())) {
            return moveOnePodFromDeploymentToStrimziPodSetWithRollingUpdateStrategy(desiredDeploymentReplicas, desiredPodSetReplicas);
        } else {
            return moveOnePodFromDeploymentToStrimziPodSetWithRecreateStrategy(desiredDeploymentReplicas, desiredPodSetReplicas);
        }
    }

    /**
     * Executes one migration step which consists from removing one Deployment replicas and adding one PodSet replica.
     * When it completes the step, it recursively calls itself again to do the next step and so on. This method is
     * following the Rolling Update Deployment Strategy: it first creates a new pod before removing the old one.
     *
     * @param desiredDeploymentReplicas     Number of desired Deployment replicas in this step
     * @param desiredPodSetReplicas         Number of desired PodSets in this step
     *
     * @return  Future which completes when all steps are done
     */
    private Future<Void> moveOnePodFromDeploymentToStrimziPodSetWithRollingUpdateStrategy(int desiredDeploymentReplicas, int desiredPodSetReplicas)  {
        if (desiredDeploymentReplicas < 1)  {
            // We are done, there will be no more Pods inside the Deployment
            // We scale the StrimziPodSet to the final number of replicas and delete the Deployment
            LOGGER.infoCr(reconciliation, "Migration from Deployment to StrimziPodSet is finishing. Deleting the Deployment.");
            return scaleUpStrimziPodSet(connect.getReplicas())
                    .compose(i -> deploymentOperator.deleteAsync(reconciliation, reconciliation.namespace(), connect.getComponentName(), true))
                    .map((Void) null);
        } else {
            LOGGER.infoCr(reconciliation, "Moving one Pod from Deployment to StrimziPodSet");
            return scaleUpStrimziPodSet(desiredPodSetReplicas)
                    .compose(i -> scaleDownDeployment(desiredDeploymentReplicas))
                    .compose(i -> moveOnePodFromDeploymentToStrimziPodSetWithRollingUpdateStrategy(desiredDeploymentReplicas - 1, Math.min(desiredPodSetReplicas + 1, connect.getReplicas())));
        }
    }

    /**
     * Executes one migration step which consists from removing one Deployment replicas and adding one PodSet replica.
     * When it completes the step, it recursively calls itself again to do the next step and so on. This method is
     * following the Recreate Deployment Strategy: it first deletes the old pod before creating the new one.
     *
     * @param desiredDeploymentReplicas     Number of desired Deployment replicas in this step
     * @param desiredPodSetReplicas         Number of desired PodSets in this step
     *
     * @return  Future which completes when all steps are done
     */
    private Future<Void> moveOnePodFromDeploymentToStrimziPodSetWithRecreateStrategy(int desiredDeploymentReplicas, int desiredPodSetReplicas)  {
        if (desiredDeploymentReplicas < 1)  {
            // We are done, there will be no more Pods inside the Deployment
            // We scale the StrimziPodSet to the final number of replicas and delete the Deployment
            LOGGER.infoCr(reconciliation, "Migration from Deployment to StrimziPodSet is finishing. Deleting the Deployment.");
            return deploymentOperator.deleteAsync(reconciliation, reconciliation.namespace(), connect.getComponentName(), true)
                    .compose(i -> scaleUpStrimziPodSet(connect.getReplicas()));
        } else {
            LOGGER.infoCr(reconciliation, "Moving one Pod from Deployment to StrimziPodSet");
            return scaleDownDeployment(desiredDeploymentReplicas)
                    .compose(i -> scaleUpStrimziPodSet(desiredPodSetReplicas))
                    .compose(i -> moveOnePodFromDeploymentToStrimziPodSetWithRecreateStrategy(desiredDeploymentReplicas - 1, Math.min(desiredPodSetReplicas + 1, connect.getReplicas())));
        }
    }

    /**
     * Scales-down Deployment
     *
     * @param replicas  New number of replicas
     *
     * @return  Future which completes when the scale-down is done and the Deployment is ready
     */
    private Future<Void> scaleDownDeployment(int replicas)    {
        LOGGER.infoCr(reconciliation, "Scaling down Deployment {}", connect.getComponentName());
        return deploymentOperator.scaleDown(reconciliation, reconciliation.namespace(), connect.getComponentName(), replicas, operationTimeoutMs)
                .compose(i -> deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), connect.getComponentName(), 1_000, operationTimeoutMs))
                .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), connect.getComponentName(), 1_000, operationTimeoutMs));
    }

    /**
     * Scales-up StrimziPodSet and waits for the new Pod to be ready
     *
     * @param replicas  New number of replicas
     *
     * @return  Future which completes when the scale-up is done and the new Pod is ready
     */
    private Future<Void> scaleUpStrimziPodSet(int replicas)    {
        LOGGER.infoCr(reconciliation, "Scaling up StrimziPodSet {}", connect.getComponentName());
        return podSetOperator.reconcile(reconciliation, reconciliation.namespace(), connect.getComponentName(), connect.generatePodSet(replicas, controllerAnnotations, podAnnotations, isOpenshift, imagePullPolicy, imagePullSecrets, customContainerImage))
                .compose(i -> podOperator.readiness(reconciliation, reconciliation.namespace(), connect.getPodName(replicas - 1), 1_000, operationTimeoutMs));
    }
}