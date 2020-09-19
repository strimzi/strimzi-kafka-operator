/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafkaBridge;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker2;
import io.strimzi.api.kafka.model.DoneableKafkaRebalance;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.Vertx;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ResourceOperatorSupplier {
    public final SecretOperator secretOperations;
    public final ServiceOperator serviceOperations;
    public final RouteOperator routeOperations;
    public final ZookeeperSetOperator zkSetOperations;
    public final KafkaSetOperator kafkaSetOperations;
    public final ConfigMapOperator configMapOperations;
    public final PvcOperator pvcOperations;
    public final DeploymentOperator deploymentOperations;
    public final ServiceAccountOperator serviceAccountOperations;
    public final RoleBindingOperator roleBindingOperations;
    public final ClusterRoleBindingOperator clusterRoleBindingOperator;
    public final CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> kafkaOperator;
    public final CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect> connectOperator;
    public final CrdOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I> connectS2IOperator;
    public final CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker> mirrorMakerOperator;
    public final CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList, DoneableKafkaBridge> kafkaBridgeOperator;
    public final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> kafkaConnectorOperator;
    public final CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List, DoneableKafkaMirrorMaker2> mirrorMaker2Operator;
    public final CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList, DoneableKafkaRebalance> kafkaRebalanceOperator;
    public final NetworkPolicyOperator networkPolicyOperator;
    public final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    public final PodOperator podOperations;
    public final IngressOperator ingressOperations;
    public final ImageStreamOperator imagesStreamOperations;
    public final BuildConfigOperator buildConfigOperations;
    public final DeploymentConfigOperator deploymentConfigOperations;
    public final StorageClassOperator storageClassOperations;
    public final NodeOperator nodeOperator;
    public final ZookeeperScalerProvider zkScalerProvider;
    public final MetricsProvider metricsProvider;
    public AdminClientProvider adminClientProvider;

    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, PlatformFeaturesAvailability pfa, long operationTimeoutMs) {
        this(vertx, client,
            new ZookeeperLeaderFinder(vertx, new SecretOperator(vertx, client),
            // Retry up to 3 times (4 attempts), with overall max delay of 35000ms
                () -> new BackOff(5_000, 2, 4)),
                    new DefaultAdminClientProvider(),
                    new DefaultZookeeperScalerProvider(),
                    new MicrometerMetricsProvider(),
                    pfa, operationTimeoutMs);
    }

    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, ZookeeperLeaderFinder zlf,
                                    AdminClientProvider adminClientProvider, ZookeeperScalerProvider zkScalerProvider,
                                    MetricsProvider metricsProvider, PlatformFeaturesAvailability pfa, long operationTimeoutMs) {
        this(new ServiceOperator(vertx, client),
                pfa.hasRoutes() ? new RouteOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new ZookeeperSetOperator(vertx, client, zlf, operationTimeoutMs),
                new KafkaSetOperator(vertx, client, operationTimeoutMs, adminClientProvider),
                new ConfigMapOperator(vertx, client),
                new SecretOperator(vertx, client),
                new PvcOperator(vertx, client),
                new DeploymentOperator(vertx, client),
                new ServiceAccountOperator(vertx, client),
                new RoleBindingOperator(vertx, client),
                new ClusterRoleBindingOperator(vertx, client, operationTimeoutMs),
                new NetworkPolicyOperator(vertx, client),
                new PodDisruptionBudgetOperator(vertx, client),
                new PodOperator(vertx, client),
                new IngressOperator(vertx, client),
                pfa.hasImages() ? new ImageStreamOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                pfa.hasBuilds() ? new BuildConfigOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                pfa.hasApps() ? new DeploymentConfigOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class, Crds.kafka()),
                new CrdOperator<>(vertx, client, KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class, Crds.kafkaConnect()),
                pfa.hasBuilds() && pfa.hasApps() && pfa.hasImages() ? new CrdOperator<>(vertx, client.adapt(OpenShiftClient.class), KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class, Crds.kafkaConnectS2I()) : null,
                new CrdOperator<>(vertx, client, KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class, Crds.kafkaMirrorMaker()),
                new CrdOperator<>(vertx, client, KafkaBridge.class, KafkaBridgeList.class, DoneableKafkaBridge.class, Crds.kafkaBridge()),
                new CrdOperator<>(vertx, client, KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class, Crds.kafkaConnector()),
                new CrdOperator<>(vertx, client, KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, DoneableKafkaMirrorMaker2.class, Crds.kafkaMirrorMaker2()),
                new CrdOperator<>(vertx, client, KafkaRebalance.class, KafkaRebalanceList.class, DoneableKafkaRebalance.class, Crds.kafkaRebalance()),
                new StorageClassOperator(vertx, client, operationTimeoutMs),
                new NodeOperator(vertx, client, operationTimeoutMs),
                zkScalerProvider,
                metricsProvider,
                adminClientProvider);
    }

    public ResourceOperatorSupplier(ServiceOperator serviceOperations,
                                    RouteOperator routeOperations,
                                    ZookeeperSetOperator zkSetOperations,
                                    KafkaSetOperator kafkaSetOperations,
                                    ConfigMapOperator configMapOperations,
                                    SecretOperator secretOperations,
                                    PvcOperator pvcOperations,
                                    DeploymentOperator deploymentOperations,
                                    ServiceAccountOperator serviceAccountOperations,
                                    RoleBindingOperator roleBindingOperations,
                                    ClusterRoleBindingOperator clusterRoleBindingOperator,
                                    NetworkPolicyOperator networkPolicyOperator,
                                    PodDisruptionBudgetOperator podDisruptionBudgetOperator,
                                    PodOperator podOperations,
                                    IngressOperator ingressOperations,
                                    ImageStreamOperator imagesStreamOperations,
                                    BuildConfigOperator buildConfigOperations,
                                    DeploymentConfigOperator deploymentConfigOperations,
                                    CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> kafkaOperator,
                                    CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect> connectOperator,
                                    CrdOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I> connectS2IOperator,
                                    CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker> mirrorMakerOperator,
                                    CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList, DoneableKafkaBridge> kafkaBridgeOperator,
                                    CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> kafkaConnectorOperator,
                                    CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List, DoneableKafkaMirrorMaker2> mirrorMaker2Operator,
                                    CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList, DoneableKafkaRebalance> kafkaRebalanceOperator,
                                    StorageClassOperator storageClassOperator,
                                    NodeOperator nodeOperator,
                                    ZookeeperScalerProvider zkScalerProvider,
                                    MetricsProvider metricsProvider,
                                    AdminClientProvider adminClientProvider) {
        this.serviceOperations = serviceOperations;
        this.routeOperations = routeOperations;
        this.zkSetOperations = zkSetOperations;
        this.kafkaSetOperations = kafkaSetOperations;
        this.configMapOperations = configMapOperations;
        this.secretOperations = secretOperations;
        this.pvcOperations = pvcOperations;
        this.deploymentOperations = deploymentOperations;
        this.serviceAccountOperations = serviceAccountOperations;
        this.roleBindingOperations = roleBindingOperations;
        this.clusterRoleBindingOperator = clusterRoleBindingOperator;
        this.networkPolicyOperator = networkPolicyOperator;
        this.podDisruptionBudgetOperator = podDisruptionBudgetOperator;
        this.kafkaOperator = kafkaOperator;
        this.podOperations = podOperations;
        this.ingressOperations = ingressOperations;
        this.imagesStreamOperations = imagesStreamOperations;
        this.buildConfigOperations = buildConfigOperations;
        this.deploymentConfigOperations = deploymentConfigOperations;
        this.connectOperator = connectOperator;
        this.connectS2IOperator = connectS2IOperator;
        this.mirrorMakerOperator = mirrorMakerOperator;
        this.kafkaBridgeOperator = kafkaBridgeOperator;
        this.storageClassOperations = storageClassOperator;
        this.kafkaConnectorOperator = kafkaConnectorOperator;
        this.mirrorMaker2Operator = mirrorMaker2Operator;
        this.kafkaRebalanceOperator = kafkaRebalanceOperator;
        this.nodeOperator = nodeOperator;
        this.zkScalerProvider = zkScalerProvider;
        this.metricsProvider = metricsProvider;
        this.adminClientProvider = adminClientProvider;
    }
}
