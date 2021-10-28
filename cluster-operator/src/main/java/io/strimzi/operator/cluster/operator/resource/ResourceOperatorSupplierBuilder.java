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

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "deprecation"})
public class ResourceOperatorSupplierBuilder {
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

    public ResourceOperatorSupplierBuilder(Vertx vertx, KubernetesClient client) {
        this.vertx = vertx;
        this.client = client;
    }

    public ResourceOperatorSupplierBuilder withServiceOperations(ServiceOperator serviceOperations) {
        this.serviceOperations = serviceOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withRouteOperations(RouteOperator routeOperations) {
        this.routeOperations = routeOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withZkSetOperations(ZookeeperSetOperator zkSetOperations) {
        this.zkSetOperations = zkSetOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withKafkaSetOperations(KafkaSetOperator kafkaSetOperations) {
        this.kafkaSetOperations = kafkaSetOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withConfigMapOperations(ConfigMapOperator configMapOperations) {
        this.configMapOperations = configMapOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withSecretOperations(SecretOperator secretOperations) {
        this.secretOperations = secretOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder setPvcOperations(PvcOperator pvcOperations) {
        this.pvcOperations = pvcOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withDeploymentOperations(DeploymentOperator deploymentOperations) {
        this.deploymentOperations = deploymentOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withServiceAccountOperations(ServiceAccountOperator serviceAccountOperations) {
        this.serviceAccountOperations = serviceAccountOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withRoleBindingOperations(RoleBindingOperator roleBindingOperations) {
        this.roleBindingOperations = roleBindingOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder setRoleOperations(RoleOperator roleOperations) {
        this.roleOperations = roleOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withClusterRoleBindingOperator(ClusterRoleBindingOperator clusterRoleBindingOperator) {
        this.clusterRoleBindingOperator = clusterRoleBindingOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withNetworkPolicyOperator(NetworkPolicyOperator networkPolicyOperator) {
        this.networkPolicyOperator = networkPolicyOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withPodDisruptionBudgetOperator(PodDisruptionBudgetOperator podDisruptionBudgetOperator) {
        this.podDisruptionBudgetOperator = podDisruptionBudgetOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withPodOperations(PodOperator podOperations) {
        this.podOperations = podOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withIngressOperations(IngressOperator ingressOperations) {
        this.ingressOperations = ingressOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withIngressV1Beta1Operations(IngressV1Beta1Operator ingressV1Beta1Operations) {
        this.ingressV1Beta1Operations = ingressV1Beta1Operations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withBuildConfigOperations(BuildConfigOperator buildConfigOperations) {
        this.buildConfigOperations = buildConfigOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withBuildOperations(BuildOperator buildOperations) {
        this.buildOperations = buildOperations;
        return this;
    }

    public ResourceOperatorSupplierBuilder withKafkaOperator(CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator) {
        this.kafkaOperator = kafkaOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withConnectOperator(CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator) {
        this.connectOperator = connectOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withMirrorMakerOperator(CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> mirrorMakerOperator) {
        this.mirrorMakerOperator = mirrorMakerOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withKafkaBridgeOperator(CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator) {
        this.kafkaBridgeOperator = kafkaBridgeOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withKafkaConnectorOperator(CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator) {
        this.kafkaConnectorOperator = kafkaConnectorOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withMirrorMaker2Operator(CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator) {
        this.mirrorMaker2Operator = mirrorMaker2Operator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withKafkaRebalanceOperator(CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator) {
        this.kafkaRebalanceOperator = kafkaRebalanceOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withStorageClassOperator(StorageClassOperator storageClassOperator) {
        this.storageClassOperator = storageClassOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withNodeOperator(NodeOperator nodeOperator) {
        this.nodeOperator = nodeOperator;
        return this;
    }

    public ResourceOperatorSupplierBuilder withZkScalerProvider(ZookeeperScalerProvider zkScalerProvider) {
        this.zkScalerProvider = zkScalerProvider;
        return this;
    }

    public ResourceOperatorSupplierBuilder withMetricsProvider(MetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
        return this;
    }

    public ResourceOperatorSupplierBuilder withAdminClientProvider(AdminClientProvider adminClientProvider) {
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