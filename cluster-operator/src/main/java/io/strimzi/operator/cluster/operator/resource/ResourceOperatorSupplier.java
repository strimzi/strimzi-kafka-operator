/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.BuildOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.IngressV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RoleOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.Vertx;

// Deprecation is suppressed because of KafkaMirrorMaker
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "deprecation"})
public class ResourceOperatorSupplier {
    public final SecretOperator secretOperations;
    public final ServiceOperator serviceOperations;
    public final RouteOperator routeOperations;
    public final StatefulSetOperator stsOperations;
    public final ConfigMapOperator configMapOperations;
    public final PvcOperator pvcOperations;
    public final DeploymentOperator deploymentOperations;
    public final ServiceAccountOperator serviceAccountOperations;
    public final RoleBindingOperator roleBindingOperations;
    public final RoleOperator roleOperations;
    public final ClusterRoleBindingOperator clusterRoleBindingOperator;
    public final CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
    public final CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator;
    public final CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> mirrorMakerOperator;
    public final CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator;
    public final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator;
    public final CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator;
    public final CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator;
    public final CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> strimziPodSetOperator;
    public final NetworkPolicyOperator networkPolicyOperator;
    public final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    public final PodDisruptionBudgetV1Beta1Operator podDisruptionBudgetV1Beta1Operator;
    public final PodOperator podOperations;
    public final IngressOperator ingressOperations;
    public final IngressV1Beta1Operator ingressV1Beta1Operations;
    public final BuildConfigOperator buildConfigOperations;
    public final BuildOperator buildOperations;
    public final StorageClassOperator storageClassOperations;
    public final NodeOperator nodeOperator;
    public final ZookeeperScalerProvider zkScalerProvider;
    public final MetricsProvider metricsProvider;
    public final AdminClientProvider adminClientProvider;
    public final ZookeeperLeaderFinder zookeeperLeaderFinder;

    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, PlatformFeaturesAvailability pfa, FeatureGates gates, long operationTimeoutMs) {
        this(vertx, client,
            new ZookeeperLeaderFinder(vertx,
            // Retry up to 3 times (4 attempts), with overall max delay of 35000ms
                () -> new BackOff(5_000, 2, 4)),
                    new DefaultAdminClientProvider(),
                    new DefaultZookeeperScalerProvider(),
                    new MicrometerMetricsProvider(),
                    pfa, gates, operationTimeoutMs);
    }

    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, ZookeeperLeaderFinder zlf,
                                    AdminClientProvider adminClientProvider, ZookeeperScalerProvider zkScalerProvider,
                                    MetricsProvider metricsProvider, PlatformFeaturesAvailability pfa, FeatureGates gates, long operationTimeoutMs) {
        this(new ServiceOperator(vertx, client),
                pfa.hasRoutes() ? new RouteOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new StatefulSetOperator(vertx, client, operationTimeoutMs),
                new ConfigMapOperator(vertx, client),
                new SecretOperator(vertx, client),
                new PvcOperator(vertx, client),
                new DeploymentOperator(vertx, client),
                new ServiceAccountOperator(vertx, client, gates.serviceAccountPatchingEnabled()),
                new RoleBindingOperator(vertx, client),
                new RoleOperator(vertx, client),
                new ClusterRoleBindingOperator(vertx, client),
                new NetworkPolicyOperator(vertx, client),
                new PodDisruptionBudgetOperator(vertx, client),
                new PodDisruptionBudgetV1Beta1Operator(vertx, client),
                new PodOperator(vertx, client),
                new IngressOperator(vertx, client),
                new IngressV1Beta1Operator(vertx, client),
                pfa.hasBuilds() ? new BuildConfigOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                pfa.hasBuilds() ? new BuildOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaConnect.class, KafkaConnectList.class, KafkaConnect.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaMirrorMaker.class, KafkaMirrorMakerList.class, KafkaMirrorMaker.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaBridge.class, KafkaBridgeList.class, KafkaBridge.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaConnector.class, KafkaConnectorList.class, KafkaConnector.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, KafkaMirrorMaker2.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaRebalance.class, KafkaRebalanceList.class, KafkaRebalance.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, StrimziPodSet.class, StrimziPodSetList.class, StrimziPodSet.RESOURCE_KIND),
                new StorageClassOperator(vertx, client),
                new NodeOperator(vertx, client),
                zkScalerProvider,
                metricsProvider,
                adminClientProvider,
                zlf);
    }

    public ResourceOperatorSupplier(ServiceOperator serviceOperations,
                                    RouteOperator routeOperations,
                                    StatefulSetOperator stsOperations,
                                    ConfigMapOperator configMapOperations,
                                    SecretOperator secretOperations,
                                    PvcOperator pvcOperations,
                                    DeploymentOperator deploymentOperations,
                                    ServiceAccountOperator serviceAccountOperations,
                                    RoleBindingOperator roleBindingOperations,
                                    RoleOperator roleOperations,
                                    ClusterRoleBindingOperator clusterRoleBindingOperator,
                                    NetworkPolicyOperator networkPolicyOperator,
                                    PodDisruptionBudgetOperator podDisruptionBudgetOperator,
                                    PodDisruptionBudgetV1Beta1Operator podDisruptionBudgetV1Beta1Operator,
                                    PodOperator podOperations,
                                    IngressOperator ingressOperations,
                                    IngressV1Beta1Operator ingressV1Beta1Operations,
                                    BuildConfigOperator buildConfigOperations,
                                    BuildOperator buildOperations,
                                    CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator,
                                    CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator,
                                    CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> mirrorMakerOperator,
                                    CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator,
                                    CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator,
                                    CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator,
                                    CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator,
                                    CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> strimziPodSetOperator,
                                    StorageClassOperator storageClassOperator,
                                    NodeOperator nodeOperator,
                                    ZookeeperScalerProvider zkScalerProvider,
                                    MetricsProvider metricsProvider,
                                    AdminClientProvider adminClientProvider,
                                    ZookeeperLeaderFinder zookeeperLeaderFinder) {
        this.serviceOperations = serviceOperations;
        this.routeOperations = routeOperations;
        this.stsOperations = stsOperations;
        this.configMapOperations = configMapOperations;
        this.secretOperations = secretOperations;
        this.pvcOperations = pvcOperations;
        this.deploymentOperations = deploymentOperations;
        this.serviceAccountOperations = serviceAccountOperations;
        this.roleBindingOperations = roleBindingOperations;
        this.roleOperations = roleOperations;
        this.clusterRoleBindingOperator = clusterRoleBindingOperator;
        this.networkPolicyOperator = networkPolicyOperator;
        this.podDisruptionBudgetOperator = podDisruptionBudgetOperator;
        this.podDisruptionBudgetV1Beta1Operator = podDisruptionBudgetV1Beta1Operator;
        this.kafkaOperator = kafkaOperator;
        this.podOperations = podOperations;
        this.ingressOperations = ingressOperations;
        this.ingressV1Beta1Operations = ingressV1Beta1Operations;
        this.buildConfigOperations = buildConfigOperations;
        this.buildOperations = buildOperations;
        this.connectOperator = connectOperator;
        this.mirrorMakerOperator = mirrorMakerOperator;
        this.kafkaBridgeOperator = kafkaBridgeOperator;
        this.storageClassOperations = storageClassOperator;
        this.kafkaConnectorOperator = kafkaConnectorOperator;
        this.mirrorMaker2Operator = mirrorMaker2Operator;
        this.kafkaRebalanceOperator = kafkaRebalanceOperator;
        this.strimziPodSetOperator = strimziPodSetOperator;
        this.nodeOperator = nodeOperator;
        this.zkScalerProvider = zkScalerProvider;
        this.metricsProvider = metricsProvider;
        this.adminClientProvider = adminClientProvider;
        this.zookeeperLeaderFinder = zookeeperLeaderFinder;
    }
}
