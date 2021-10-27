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
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
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
    private final SecretOperator secretOperations;
    private final ServiceOperator serviceOperations;
    private final RouteOperator routeOperations;
    private final ZookeeperSetOperator zkSetOperations;
    private final KafkaSetOperator kafkaSetOperations;
    private final ConfigMapOperator configMapOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;
    private final ServiceAccountOperator serviceAccountOperations;
    private final RoleBindingOperator roleBindingOperations;
    private final RoleOperator roleOperations;
    private final ClusterRoleBindingOperator clusterRoleBindingOperator;
    private final CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
    private final CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator;
    private final CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> mirrorMakerOperator;
    private final CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator;
    private final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator;
    private final CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator;
    private final CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    private final PodOperator podOperations;
    private final IngressOperator ingressOperations;
    private final IngressV1Beta1Operator ingressV1Beta1Operations;
    private final BuildConfigOperator buildConfigOperations;
    private final BuildOperator buildOperations;
    private final StorageClassOperator storageClassOperations;
    private final NodeOperator nodeOperator;
    private final ZookeeperScalerProvider zkScalerProvider;
    private final MetricsProvider metricsProvider;
    private AdminClientProvider adminClientProvider;

    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, PlatformFeaturesAvailability pfa, FeatureGates gates, long operationTimeoutMs) {
        this(vertx, client,
            new ZookeeperLeaderFinder(vertx, new SecretOperator(vertx, client),
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
                new ZookeeperSetOperator(vertx, client, zlf, operationTimeoutMs),
                new KafkaSetOperator(vertx, client, operationTimeoutMs, adminClientProvider),
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
                new StorageClassOperator(vertx, client),
                new NodeOperator(vertx, client),
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
                                    RoleOperator roleOperations,
                                    ClusterRoleBindingOperator clusterRoleBindingOperator,
                                    NetworkPolicyOperator networkPolicyOperator,
                                    PodDisruptionBudgetOperator podDisruptionBudgetOperator,
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
        this.roleOperations = roleOperations;
        this.clusterRoleBindingOperator = clusterRoleBindingOperator;
        this.networkPolicyOperator = networkPolicyOperator;
        this.podDisruptionBudgetOperator = podDisruptionBudgetOperator;
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
        this.nodeOperator = nodeOperator;
        this.zkScalerProvider = zkScalerProvider;
        this.metricsProvider = metricsProvider;
        this.adminClientProvider = adminClientProvider;
    }

    public SecretOperator getSecretOperations() {
        return secretOperations;
    }

    public ServiceOperator getServiceOperations() {
        return serviceOperations;
    }

    public RouteOperator getRouteOperations() {
        return routeOperations;
    }

    public ZookeeperSetOperator getZkSetOperations() {
        return zkSetOperations;
    }

    public KafkaSetOperator getKafkaSetOperations() {
        return kafkaSetOperations;
    }

    public ConfigMapOperator getConfigMapOperations() {
        return configMapOperations;
    }

    public PvcOperator getPvcOperations() {
        return pvcOperations;
    }

    public DeploymentOperator getDeploymentOperations() {
        return deploymentOperations;
    }

    public ServiceAccountOperator getServiceAccountOperations() {
        return serviceAccountOperations;
    }

    public RoleBindingOperator getRoleBindingOperations() {
        return roleBindingOperations;
    }

    public RoleOperator getRoleOperations() {
        return roleOperations;
    }

    public ClusterRoleBindingOperator getClusterRoleBindingOperator() {
        return clusterRoleBindingOperator;
    }

    public CrdOperator<KubernetesClient, Kafka, KafkaList> getKafkaOperator() {
        return kafkaOperator;
    }

    public CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> getConnectOperator() {
        return connectOperator;
    }

    public CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> getMirrorMakerOperator() {
        return mirrorMakerOperator;
    }

    public CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> getKafkaBridgeOperator() {
        return kafkaBridgeOperator;
    }

    public CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> getKafkaConnectorOperator() {
        return kafkaConnectorOperator;
    }

    public CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> getMirrorMaker2Operator() {
        return mirrorMaker2Operator;
    }

    public CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> getKafkaRebalanceOperator() {
        return kafkaRebalanceOperator;
    }

    public NetworkPolicyOperator getNetworkPolicyOperator() {
        return networkPolicyOperator;
    }

    public PodDisruptionBudgetOperator getPodDisruptionBudgetOperator() {
        return podDisruptionBudgetOperator;
    }

    public PodOperator getPodOperations() {
        return podOperations;
    }

    public IngressOperator getIngressOperations() {
        return ingressOperations;
    }

    public IngressV1Beta1Operator getIngressV1Beta1Operations() {
        return ingressV1Beta1Operations;
    }

    public BuildConfigOperator getBuildConfigOperations() {
        return buildConfigOperations;
    }

    public BuildOperator getBuildOperations() {
        return buildOperations;
    }

    public StorageClassOperator getStorageClassOperations() {
        return storageClassOperations;
    }

    public NodeOperator getNodeOperator() {
        return nodeOperator;
    }

    public ZookeeperScalerProvider getZkScalerProvider() {
        return zkScalerProvider;
    }

    public MetricsProvider getMetricsProvider() {
        return metricsProvider;
    }

    public AdminClientProvider getAdminClientProvider() {
        return adminClientProvider;
    }
}
