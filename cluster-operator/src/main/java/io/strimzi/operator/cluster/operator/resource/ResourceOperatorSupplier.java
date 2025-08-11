/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.DefaultSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.assembly.BrokersInUseCheck;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.BuildConfigOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.BuildOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ImageStreamOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NodeOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StorageClassOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.featuregates.FeatureGates;
import io.vertx.core.Vertx;

/**
 * Class holding the various resource operator and providers of various clients
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
public class ResourceOperatorSupplier {
    /**
     * Secret operator
     */
    public final SecretOperator secretOperations;

    /**
     * Service operator
     */
    public final ServiceOperator serviceOperations;

    /**
     * Route operator
     */
    public final RouteOperator routeOperations;

    /**
     * ImageStream operator
     */
    public final ImageStreamOperator imageStreamOperations;

    /**
     * Config Map operator
     */
    public final ConfigMapOperator configMapOperations;

    /**
     * PVC operator
     */
    public final PvcOperator pvcOperations;

    /**
     * Deployment operator
     */
    public final DeploymentOperator deploymentOperations;

    /**
     * Service Account operator
     */
    public final ServiceAccountOperator serviceAccountOperations;

    /**
     * Role Binding operator
     */
    public final RoleBindingOperator roleBindingOperations;

    /**
     * Role operator
     */
    public final RoleOperator roleOperations;

    /**
     * Cluster Role Binding operator
     */
    public final ClusterRoleBindingOperator clusterRoleBindingOperator;

    /**
     * Kafka CR operator
     */
    public final CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;

    /**
     * KafkaConnect CR operator
     */
    public final CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator;

    /**
     * KafkaBridge CR operator
     */
    public final CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator;

    /**
     * KafkaConnector CR operator
     */
    public final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator;

    /**
     * KafkaMirrorMaker2 CR operator
     */
    public final CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator;

    /**
     * KafkaRebalance CR operator
     */
    public final CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator;

    /**
     * KafkaNodePool CR operator
     */
    public final CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> kafkaNodePoolOperator;

    /**
     * Strimzi Pod Set operator
     */
    public final StrimziPodSetOperator strimziPodSetOperator;

    /**
     * Network Policy operator
     */
    public final NetworkPolicyOperator networkPolicyOperator;

    /**
     * PDB operator
     */
    public final PodDisruptionBudgetOperator podDisruptionBudgetOperator;


    /**
     * Pod operator
     */
    public final PodOperator podOperations;

    /**
     * Ingress operator
     */
    public final IngressOperator ingressOperations;

    /**
     * Build Config operator
     */
    public final BuildConfigOperator buildConfigOperations;

    /**
     * Build operator
     */
    public final BuildOperator buildOperations;

    /**
     * Storage Class operator
     */
    public final StorageClassOperator storageClassOperations;

    /**
     * Node operator
     */
    public final NodeOperator nodeOperator;

    /**
     * Metrics provider
     */
    public final MetricsProvider metricsProvider;

    /**
     * Kafka Admin API client provider
     */
    public final AdminClientProvider adminClientProvider;

    /**
     * Kafka Agent client provider
     */
    public final KafkaAgentClientProvider kafkaAgentClientProvider;

    /**
     * Restart Events publisher
     */
    public final KubernetesRestartEventPublisher restartEventsPublisher;

    /**
     * Shared environment provider
     */
    public final SharedEnvironmentProvider sharedEnvironmentProvider;

    /**
     * Broker Scale Down operations
     */
    public final BrokersInUseCheck brokersInUseCheck;

    /**
     * Constructor
     *
     * @param vertx                 Vert.x instance
     * @param client                Kubernetes Client
     * @param metricsProvider       Metrics provider
     * @param pfa                   Platform Availability Features
     * @param operatorName          Name of this operator instance
     * @param featureGates          Feature Gates configuration of operator
     */
    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, MetricsProvider metricsProvider, PlatformFeaturesAvailability pfa, String operatorName, FeatureGates featureGates) {
        this(vertx,
            client,
            new DefaultAdminClientProvider(),
            new DefaultKafkaAgentClientProvider(),
            metricsProvider,
            pfa,
            new KubernetesRestartEventPublisher(client, operatorName),
            featureGates
        );
    }

    /**
     * Constructor used for tests
     *
     * @param vertx                    Vert.x instance
     * @param client                   Kubernetes Client
     * @param adminClientProvider      Kafka Admin client provider
     * @param kafkaAgentClientProvider Kafka Agent client provider
     * @param metricsProvider          Metrics provider
     * @param pfa                      Platform Availability Features
     */
    public ResourceOperatorSupplier(Vertx vertx,
                                    KubernetesClient client,
                                    AdminClientProvider adminClientProvider,
                                    KafkaAgentClientProvider kafkaAgentClientProvider,
                                    MetricsProvider metricsProvider,
                                    PlatformFeaturesAvailability pfa) {
        this(vertx,
                client,
                adminClientProvider,
                kafkaAgentClientProvider,
                metricsProvider,
                pfa,
                new KubernetesRestartEventPublisher(client, "operatorName")
        );
    }

    private ResourceOperatorSupplier(Vertx vertx,
                                     KubernetesClient client,
                                     AdminClientProvider adminClientProvider,
                                     KafkaAgentClientProvider kafkaAgentClientProvider,
                                     MetricsProvider metricsProvider,
                                     PlatformFeaturesAvailability pfa,
                                     KubernetesRestartEventPublisher restartEventPublisher) {
        this(vertx,
            client,
            adminClientProvider,
            kafkaAgentClientProvider,
            metricsProvider,
            pfa,
            restartEventPublisher,
            new FeatureGates("")
        );
    }

    private ResourceOperatorSupplier(Vertx vertx,
                                     KubernetesClient client,
                                     AdminClientProvider adminClientProvider,
                                     KafkaAgentClientProvider kafkaAgentClientProvider,
                                     MetricsProvider metricsProvider,
                                     PlatformFeaturesAvailability pfa,
                                     KubernetesRestartEventPublisher restartEventPublisher,
                                     FeatureGates featureGates) {
        this(new ServiceOperator(vertx, client, featureGates.serverSideApplyPhase1Enabled()),
                pfa.hasRoutes() ? new RouteOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                pfa.hasImages() ? new ImageStreamOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new ConfigMapOperator(vertx, client, featureGates.serverSideApplyPhase1Enabled()),
                new SecretOperator(vertx, client),
                new PvcOperator(vertx, client, featureGates.serverSideApplyPhase1Enabled()),
                new DeploymentOperator(vertx, client),
                new ServiceAccountOperator(vertx, client, featureGates.serverSideApplyPhase1Enabled()),
                new RoleBindingOperator(vertx, client),
                new RoleOperator(vertx, client),
                new ClusterRoleBindingOperator(vertx, client),
                new NetworkPolicyOperator(vertx, client),
                new PodDisruptionBudgetOperator(vertx, client),
                new PodOperator(vertx, client),
                new IngressOperator(vertx, client, featureGates.serverSideApplyPhase1Enabled()),
                pfa.hasBuilds() ? new BuildConfigOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                pfa.hasBuilds() ? new BuildOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaConnect.class, KafkaConnectList.class, KafkaConnect.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaBridge.class, KafkaBridgeList.class, KafkaBridge.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaConnector.class, KafkaConnectorList.class, KafkaConnector.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, KafkaMirrorMaker2.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaRebalance.class, KafkaRebalanceList.class, KafkaRebalance.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaNodePool.class, KafkaNodePoolList.class, KafkaNodePool.RESOURCE_KIND),
                new StrimziPodSetOperator(vertx, client),
                new StorageClassOperator(vertx, client),
                new NodeOperator(vertx, client),
                kafkaAgentClientProvider,
                metricsProvider,
                adminClientProvider,
                restartEventPublisher,
                new DefaultSharedEnvironmentProvider(),
                new BrokersInUseCheck());
    }

    /**
     * Constructor
     *
     * @param serviceOperations                     Service operator
     * @param routeOperations                       Route operator
     * @param imageStreamOperations                 ImageStream operator
     * @param configMapOperations                   ConfigMap operator
     * @param secretOperations                      Secret operator
     * @param pvcOperations                         PVC operator
     * @param deploymentOperations                  Deployment operator
     * @param serviceAccountOperations              Service Account operator
     * @param roleBindingOperations                 Role Binding operator
     * @param roleOperations                        Role operator
     * @param clusterRoleBindingOperator            Cluster Role Binding operator
     * @param networkPolicyOperator                 Network Policy operator
     * @param podDisruptionBudgetOperator           Pod Disruption Budget operator
     * @param podOperations                         Pod operator
     * @param ingressOperations                     Ingress operator
     * @param buildConfigOperations                 Build Config operator
     * @param buildOperations                       Build operator
     * @param kafkaOperator                         Kafka CR operator
     * @param connectOperator                       KafkaConnect CR operator
     * @param kafkaBridgeOperator                   KafkaBridge operator
     * @param kafkaConnectorOperator                KafkaConnector operator
     * @param mirrorMaker2Operator                  KafkaMirrorMaker2 operator
     * @param kafkaRebalanceOperator                KafkaRebalance operator
     * @param kafkaNodePoolOperator                 kafkaNodePool operator
     * @param strimziPodSetOperator                 StrimziPodSet operator
     * @param storageClassOperator                  StorageClass operator
     * @param nodeOperator                          Node operator
     * @param kafkaAgentClientProvider              Kafka Agent client provider
     * @param metricsProvider                       Metrics provider
     * @param adminClientProvider                   Kafka Admin client provider
     * @param restartEventsPublisher                Kubernetes Events publisher
     * @param sharedEnvironmentProvider             Shared environment provider
     * @param brokersInUseCheck                     Broker scale down operations
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public ResourceOperatorSupplier(ServiceOperator serviceOperations,
                                    RouteOperator routeOperations,
                                    ImageStreamOperator imageStreamOperations,
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
                                    BuildConfigOperator buildConfigOperations,
                                    BuildOperator buildOperations,
                                    CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator,
                                    CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator,
                                    CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator,
                                    CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator,
                                    CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator,
                                    CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator,
                                    CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> kafkaNodePoolOperator,
                                    StrimziPodSetOperator strimziPodSetOperator,
                                    StorageClassOperator storageClassOperator,
                                    NodeOperator nodeOperator,
                                    KafkaAgentClientProvider kafkaAgentClientProvider,
                                    MetricsProvider metricsProvider,
                                    AdminClientProvider adminClientProvider,
                                    KubernetesRestartEventPublisher restartEventsPublisher,
                                    SharedEnvironmentProvider sharedEnvironmentProvider,
                                    BrokersInUseCheck brokersInUseCheck) {
        this.serviceOperations = serviceOperations;
        this.routeOperations = routeOperations;
        this.imageStreamOperations = imageStreamOperations;
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
        this.buildConfigOperations = buildConfigOperations;
        this.buildOperations = buildOperations;
        this.connectOperator = connectOperator;
        this.kafkaBridgeOperator = kafkaBridgeOperator;
        this.storageClassOperations = storageClassOperator;
        this.kafkaConnectorOperator = kafkaConnectorOperator;
        this.mirrorMaker2Operator = mirrorMaker2Operator;
        this.kafkaRebalanceOperator = kafkaRebalanceOperator;
        this.kafkaNodePoolOperator = kafkaNodePoolOperator;
        this.strimziPodSetOperator = strimziPodSetOperator;
        this.nodeOperator = nodeOperator;
        this.kafkaAgentClientProvider = kafkaAgentClientProvider;
        this.metricsProvider = metricsProvider;
        this.adminClientProvider = adminClientProvider;
        this.restartEventsPublisher = restartEventsPublisher;
        this.sharedEnvironmentProvider = sharedEnvironmentProvider;
        this.brokersInUseCheck = brokersInUseCheck;
    }
}
