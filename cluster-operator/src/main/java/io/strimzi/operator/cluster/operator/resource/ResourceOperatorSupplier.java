/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;

import io.fabric8.openshift.client.OpenShiftClient;
import io.vertx.core.Vertx;

public class ResourceOperatorSupplier {
    public final SecretOperator secretOperations;
    public final ServiceOperator serviceOperations;
    public final RouteOperator routeOperations;
    public final ZookeeperSetOperator zkSetOperations;
    public final KafkaSetOperator kafkaSetOperations;
    public final ConfigMapOperator configMapOperations;
    public final PvcOperator pvcOperations;
    public final DeploymentOperator deploymentOperations;
    public final ServiceAccountOperator serviceAccountOperator;
    public final RoleBindingOperator roleBindingOperator;
    public final ClusterRoleBindingOperator clusterRoleBindingOperator;
    public final CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> kafkaOperator;
    public final NetworkPolicyOperator networkPolicyOperator;
    public final PodDisruptionBudgetOperator podDisruptionBudgetOperator;

    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, PlatformFeaturesAvailability pfa, long operationTimeoutMs) {
        this(vertx, client, new ZookeeperLeaderFinder(vertx, new SecretOperator(vertx, client),
            // Retry up to 3 times (4 attempts), with overall max delay of 35000ms
            () -> new BackOff(5_000, 2, 4)),
                    pfa, operationTimeoutMs);
    }

    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, ZookeeperLeaderFinder zlf, PlatformFeaturesAvailability pfa, long operationTimeoutMs) {
        this(new ServiceOperator(vertx, client),
                pfa.isOpenshift() ? new RouteOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new ZookeeperSetOperator(vertx, client, zlf, operationTimeoutMs),
                new KafkaSetOperator(vertx, client, operationTimeoutMs),
                new ConfigMapOperator(vertx, client),
                new SecretOperator(vertx, client),
                new PvcOperator(vertx, client),
                new DeploymentOperator(vertx, client),
                new ServiceAccountOperator(vertx, client),
                new RoleBindingOperator(vertx, client),
                new ClusterRoleBindingOperator(vertx, client),
                new NetworkPolicyOperator(vertx, client),
                new PodDisruptionBudgetOperator(vertx, client),
                new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class));
    }

    public ResourceOperatorSupplier(ServiceOperator serviceOperations,
                                    RouteOperator routeOperations,
                                    ZookeeperSetOperator zkSetOperations,
                                    KafkaSetOperator kafkaSetOperations,
                                    ConfigMapOperator configMapOperations,
                                    SecretOperator secretOperations,
                                    PvcOperator pvcOperations,
                                    DeploymentOperator deploymentOperations,
                                    ServiceAccountOperator serviceAccountOperator,
                                    RoleBindingOperator roleBindingOperator,
                                    ClusterRoleBindingOperator clusterRoleBindingOperator,
                                    NetworkPolicyOperator networkPolicyOperator,
                                    PodDisruptionBudgetOperator podDisruptionBudgetOperator,
                                    CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> kafkaOperator) {
        this.serviceOperations = serviceOperations;
        this.routeOperations = routeOperations;
        this.zkSetOperations = zkSetOperations;
        this.kafkaSetOperations = kafkaSetOperations;
        this.configMapOperations = configMapOperations;
        this.secretOperations = secretOperations;
        this.pvcOperations = pvcOperations;
        this.deploymentOperations = deploymentOperations;
        this.serviceAccountOperator = serviceAccountOperator;
        this.roleBindingOperator = roleBindingOperator;
        this.clusterRoleBindingOperator = clusterRoleBindingOperator;
        this.networkPolicyOperator = networkPolicyOperator;
        this.podDisruptionBudgetOperator = podDisruptionBudgetOperator;
        this.kafkaOperator = kafkaOperator;
    }
}
