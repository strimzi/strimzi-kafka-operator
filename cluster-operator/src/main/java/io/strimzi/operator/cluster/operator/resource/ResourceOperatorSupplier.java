/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
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
    private final AdminClientProvider adminClientProvider;

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

    @SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "deprecation"})
    public static class Builder {
        private ServiceOperator serviceOperations;
        private RouteOperator routeOperations;
        private ZookeeperSetOperator zkSetOperations;
        private KafkaSetOperator kafkaSetOperations;
        private ConfigMapOperator configMapOperations;
        private SecretOperator secretOperations;
        private PvcOperator pvcOperations;
        private DeploymentOperator deploymentOperations;
        private ServiceAccountOperator serviceAccountOperations;
        private RoleBindingOperator roleBindingOperations;
        private RoleOperator roleOperations;
        private ClusterRoleBindingOperator clusterRoleBindingOperator;
        private NetworkPolicyOperator networkPolicyOperator;
        private PodDisruptionBudgetOperator podDisruptionBudgetOperator;
        private PodOperator podOperations;
        private IngressOperator ingressOperations;
        private IngressV1Beta1Operator ingressV1Beta1Operations;
        private BuildConfigOperator buildConfigOperations;
        private BuildOperator buildOperations;
        private CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
        private CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator;
        private CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> mirrorMakerOperator;
        private CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator;
        private CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator;
        private CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator;
        private CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator;
        private StorageClassOperator storageClassOperator;
        private NodeOperator nodeOperator;
        private ZookeeperScalerProvider zkScalerProvider;
        private MetricsProvider metricsProvider;
        private AdminClientProvider adminClientProvider;

        public Builder(Vertx vertx, KubernetesClient client) {
            this.vertx = vertx;
            this.client = client;
        }

        public Builder withServiceOperations(ServiceOperator serviceOperations) {
            this.serviceOperations = serviceOperations;
            return this;
        }

        public Builder withRouteOperations(RouteOperator routeOperations) {
            this.routeOperations = routeOperations;
            return this;
        }

        public Builder withZkSetOperations(ZookeeperSetOperator zkSetOperations) {
            this.zkSetOperations = zkSetOperations;
            return this;
        }

        public Builder withKafkaSetOperations(KafkaSetOperator kafkaSetOperations) {
            this.kafkaSetOperations = kafkaSetOperations;
            return this;
        }

        public Builder withConfigMapOperations(ConfigMapOperator configMapOperations) {
            this.configMapOperations = configMapOperations;
            return this;
        }

        public Builder withSecretOperations(SecretOperator secretOperations) {
            this.secretOperations = secretOperations;
            return this;
        }

        public Builder setPvcOperations(PvcOperator pvcOperations) {
            this.pvcOperations = pvcOperations;
            return this;
        }

        public Builder withDeploymentOperations(DeploymentOperator deploymentOperations) {
            this.deploymentOperations = deploymentOperations;
            return this;
        }

        public Builder withServiceAccountOperations(ServiceAccountOperator serviceAccountOperations) {
            this.serviceAccountOperations = serviceAccountOperations;
            return this;
        }

        public Builder withRoleBindingOperations(RoleBindingOperator roleBindingOperations) {
            this.roleBindingOperations = roleBindingOperations;
            return this;
        }

        public Builder setRoleOperations(RoleOperator roleOperations) {
            this.roleOperations = roleOperations;
            return this;
        }

        public Builder withClusterRoleBindingOperator(ClusterRoleBindingOperator clusterRoleBindingOperator) {
            this.clusterRoleBindingOperator = clusterRoleBindingOperator;
            return this;
        }

        public Builder withNetworkPolicyOperator(NetworkPolicyOperator networkPolicyOperator) {
            this.networkPolicyOperator = networkPolicyOperator;
            return this;
        }

        public Builder withPodDisruptionBudgetOperator(PodDisruptionBudgetOperator podDisruptionBudgetOperator) {
            this.podDisruptionBudgetOperator = podDisruptionBudgetOperator;
            return this;
        }

        public Builder withPodOperations(PodOperator podOperations) {
            this.podOperations = podOperations;
            return this;
        }

        public Builder withIngressOperations(IngressOperator ingressOperations) {
            this.ingressOperations = ingressOperations;
            return this;
        }

        public Builder withIngressV1Beta1Operations(IngressV1Beta1Operator ingressV1Beta1Operations) {
            this.ingressV1Beta1Operations = ingressV1Beta1Operations;
            return this;
        }

        public Builder withBuildConfigOperations(BuildConfigOperator buildConfigOperations) {
            this.buildConfigOperations = buildConfigOperations;
            return this;
        }

        public Builder withBuildOperations(BuildOperator buildOperations) {
            this.buildOperations = buildOperations;
            return this;
        }

        public Builder withKafkaOperator(CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator) {
            this.kafkaOperator = kafkaOperator;
            return this;
        }

        public Builder withConnectOperator(CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator) {
            this.connectOperator = connectOperator;
            return this;
        }

        public Builder withMirrorMakerOperator(CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> mirrorMakerOperator) {
            this.mirrorMakerOperator = mirrorMakerOperator;
            return this;
        }

        public Builder withKafkaBridgeOperator(CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator) {
            this.kafkaBridgeOperator = kafkaBridgeOperator;
            return this;
        }

        public Builder withKafkaConnectorOperator(CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator) {
            this.kafkaConnectorOperator = kafkaConnectorOperator;
            return this;
        }

        public Builder withMirrorMaker2Operator(CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator) {
            this.mirrorMaker2Operator = mirrorMaker2Operator;
            return this;
        }

        public Builder withKafkaRebalanceOperator(CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator) {
            this.kafkaRebalanceOperator = kafkaRebalanceOperator;
            return this;
        }

        public Builder withStorageClassOperator(StorageClassOperator storageClassOperator) {
            this.storageClassOperator = storageClassOperator;
            return this;
        }

        public Builder withNodeOperator(NodeOperator nodeOperator) {
            this.nodeOperator = nodeOperator;
            return this;
        }

        public Builder withZkScalerProvider(ZookeeperScalerProvider zkScalerProvider) {
            this.zkScalerProvider = zkScalerProvider;
            return this;
        }

        public Builder withMetricsProvider(MetricsProvider metricsProvider) {
            this.metricsProvider = metricsProvider;
            return this;
        }

        public Builder withAdminClientProvider(AdminClientProvider adminClientProvider) {
            this.adminClientProvider = adminClientProvider;
            return this;
        }

        private Vertx vertx;

        private KubernetesClient client;

        @SuppressWarnings("checkstyle:CyclomaticComplexity")
        public ResourceOperatorSupplier build(PlatformFeaturesAvailability pfa, FeatureGates gates, long operationTimeoutMs) {

            var vertx = this.vertx != null ? this.vertx : Vertx.vertx();
            var secretOperator = this.secretOperations != null ? this.secretOperations : new SecretOperator(vertx, client);
            var adminClientProvider = this.adminClientProvider != null ? this.adminClientProvider : new DefaultAdminClientProvider();
            var kafkaSetOperations = this.kafkaSetOperations != null ? this.kafkaSetOperations : new KafkaSetOperator(vertx, client, operationTimeoutMs, adminClientProvider);
            return new ResourceOperatorSupplier(serviceOperations != null ? serviceOperations : new ServiceOperator(vertx, client),
                    routeOperations != null ? routeOperations : pfa != null && pfa.hasRoutes() ? new RouteOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                    zkSetOperations != null ? zkSetOperations : new ZookeeperSetOperator(vertx, client, new ZookeeperLeaderFinder(vertx, secretOperator,
                            // Retry up to 3 times (4 attempts), with overall max delay of 35000ms
                            () -> new BackOff(5_000, 2, 4)), operationTimeoutMs),
                    kafkaSetOperations,
                    configMapOperations != null ? configMapOperations : new ConfigMapOperator(vertx, client),
                    secretOperator,
                    pvcOperations != null ? pvcOperations : new PvcOperator(vertx, client),
                    deploymentOperations != null ? deploymentOperations : new DeploymentOperator(vertx, client),
                    serviceAccountOperations != null ? serviceAccountOperations : new ServiceAccountOperator(vertx, client, gates != null && gates.serviceAccountPatchingEnabled()),
                    roleBindingOperations != null ? roleBindingOperations : new RoleBindingOperator(vertx, client),
                    roleOperations != null ? roleOperations : new RoleOperator(vertx, client),
                    clusterRoleBindingOperator != null ? clusterRoleBindingOperator : new ClusterRoleBindingOperator(vertx, client),
                    networkPolicyOperator != null ? networkPolicyOperator : new NetworkPolicyOperator(vertx, client),
                    podDisruptionBudgetOperator != null ? podDisruptionBudgetOperator : new PodDisruptionBudgetOperator(vertx, client),
                    podOperations != null ? podOperations : new PodOperator(vertx, client),
                    ingressOperations != null ? ingressOperations : new IngressOperator(vertx, client),
                    ingressV1Beta1Operations != null ? ingressV1Beta1Operations : new IngressV1Beta1Operator(vertx, client),
                    buildConfigOperations != null ? buildConfigOperations : pfa != null && pfa.hasBuilds() ? new BuildConfigOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                    buildOperations != null ? buildOperations : pfa != null && pfa.hasBuilds() ? new BuildOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                    kafkaOperator != null ? kafkaOperator : new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND),
                    connectOperator != null ? connectOperator : new CrdOperator<>(vertx, client, KafkaConnect.class, KafkaConnectList.class, KafkaConnect.RESOURCE_KIND),
                    this.mirrorMakerOperator != null ? this.mirrorMakerOperator : new CrdOperator<>(vertx, client, KafkaMirrorMaker.class, KafkaMirrorMakerList.class, KafkaMirrorMaker.RESOURCE_KIND),
                    this.kafkaBridgeOperator != null ? this.kafkaBridgeOperator : new CrdOperator<>(vertx, client, KafkaBridge.class, KafkaBridgeList.class, KafkaBridge.RESOURCE_KIND),
                    this.kafkaConnectorOperator != null ? this.kafkaConnectorOperator : new CrdOperator<>(vertx, client, KafkaConnector.class, KafkaConnectorList.class, KafkaConnector.RESOURCE_KIND),
                    this.mirrorMaker2Operator != null ? this.mirrorMaker2Operator : new CrdOperator<>(vertx, client, KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, KafkaMirrorMaker2.RESOURCE_KIND),
                    this.kafkaRebalanceOperator != null ? this.kafkaRebalanceOperator : new CrdOperator<>(vertx, client, KafkaRebalance.class, KafkaRebalanceList.class, KafkaRebalance.RESOURCE_KIND),
                    storageClassOperator != null ? storageClassOperator : new StorageClassOperator(vertx, client),
                    nodeOperator != null ? nodeOperator : new NodeOperator(vertx, client),
                    zkScalerProvider != null ? zkScalerProvider : new DefaultZookeeperScalerProvider(),
                    metricsProvider != null ? metricsProvider : new MicrometerMetricsProvider(),
                    adminClientProvider);
        }
    }
}
