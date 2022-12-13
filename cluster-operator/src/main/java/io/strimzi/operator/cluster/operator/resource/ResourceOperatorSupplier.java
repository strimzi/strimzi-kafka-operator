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
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.BuildOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
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
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.Vertx;

/**
 * Class holding the various resource operator and providers of various clients
 */
// Deprecation is suppressed because of KafkaMirrorMaker
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "deprecation"})
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
     * StatefulSet operator
     */    
    public final StatefulSetOperator stsOperations;

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
     * KafkaMirrorMaker CR operator
     */
    public final CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> mirrorMakerOperator;

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
     * PDB v1beta1 operator
     */
    public final PodDisruptionBudgetV1Beta1Operator podDisruptionBudgetV1Beta1Operator;

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
     * ZooKeeper Scaler provider
     */
    public final ZookeeperScalerProvider zkScalerProvider;

    /**
     * Metrics provider
     */
    public final MetricsProvider metricsProvider;

    /**
     * Kafka Admin API client provider
     */
    public final AdminClientProvider adminClientProvider;

    /**
     * ZooKeeper Leader finder
     */
    public final ZookeeperLeaderFinder zookeeperLeaderFinder;

    /**
     * Restart Events publisher
     */
    public final KubernetesRestartEventPublisher restartEventsPublisher;

    /**
     * Constructor
     *
     * @param vertx                 Vert.x instance
     * @param client                Kubernetes Client
     * @param metricsProvider       Metrics provider
     * @param pfa                   Platform Availability Features
     * @param operationTimeoutMs    Operation timeout in milliseconds
     * @param operatorName          Name of this operator instance
     */
    public ResourceOperatorSupplier(Vertx vertx, KubernetesClient client, MetricsProvider metricsProvider, PlatformFeaturesAvailability pfa, long operationTimeoutMs, String operatorName) {
        this(vertx,
                client,
                new ZookeeperLeaderFinder(vertx,
                        // Retry up to 3 times (4 attempts), with overall max delay of 35000ms
                        () -> new BackOff(5_000, 2, 4)),
                new DefaultAdminClientProvider(),
                new DefaultZookeeperScalerProvider(),
                metricsProvider,
                pfa,
                operationTimeoutMs,
                new KubernetesRestartEventPublisher(client, operatorName)
        );
    }

    /**
     * Constructor used for tests
     *
     * @param vertx                 Vert.x instance
     * @param client                Kubernetes Client
     * @param zlf                   ZooKeeper Leader Finder
     * @param adminClientProvider   Kafka Admin client provider
     * @param zkScalerProvider      ZooKeeper Scaler provider
     * @param metricsProvider       Metrics provider
     * @param pfa                   Platform Availability Features
     * @param operationTimeoutMs    Operation timeout in milliseconds
     */
    public ResourceOperatorSupplier(Vertx vertx,
                                    KubernetesClient client,
                                    ZookeeperLeaderFinder zlf,
                                    AdminClientProvider adminClientProvider,
                                    ZookeeperScalerProvider zkScalerProvider,
                                    MetricsProvider metricsProvider,
                                    PlatformFeaturesAvailability pfa,
                                    long operationTimeoutMs) {
        this(vertx,
                client,
                zlf,
                adminClientProvider,
                zkScalerProvider,
                metricsProvider,
                pfa,
                operationTimeoutMs,
                new KubernetesRestartEventPublisher(client, "operatorName")
        );
    }

    private ResourceOperatorSupplier(Vertx vertx,
                                    KubernetesClient client,
                                    ZookeeperLeaderFinder zlf,
                                    AdminClientProvider adminClientProvider,
                                    ZookeeperScalerProvider zkScalerProvider,
                                    MetricsProvider metricsProvider,
                                    PlatformFeaturesAvailability pfa,
                                    long operationTimeoutMs,
                                    KubernetesRestartEventPublisher restartEventPublisher) {
        this(new ServiceOperator(vertx, client),
                pfa.hasRoutes() ? new RouteOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                pfa.hasImages() ? new ImageStreamOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new StatefulSetOperator(vertx, client, operationTimeoutMs),
                new ConfigMapOperator(vertx, client),
                new SecretOperator(vertx, client),
                new PvcOperator(vertx, client),
                new DeploymentOperator(vertx, client),
                new ServiceAccountOperator(vertx, client),
                new RoleBindingOperator(vertx, client),
                new RoleOperator(vertx, client),
                new ClusterRoleBindingOperator(vertx, client),
                new NetworkPolicyOperator(vertx, client),
                new PodDisruptionBudgetOperator(vertx, client),
                new PodDisruptionBudgetV1Beta1Operator(vertx, client),
                new PodOperator(vertx, client),
                new IngressOperator(vertx, client),
                pfa.hasBuilds() ? new BuildConfigOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                pfa.hasBuilds() ? new BuildOperator(vertx, client.adapt(OpenShiftClient.class)) : null,
                new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaConnect.class, KafkaConnectList.class, KafkaConnect.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaMirrorMaker.class, KafkaMirrorMakerList.class, KafkaMirrorMaker.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaBridge.class, KafkaBridgeList.class, KafkaBridge.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaConnector.class, KafkaConnectorList.class, KafkaConnector.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, KafkaMirrorMaker2.RESOURCE_KIND),
                new CrdOperator<>(vertx, client, KafkaRebalance.class, KafkaRebalanceList.class, KafkaRebalance.RESOURCE_KIND),
                new StrimziPodSetOperator(vertx, client, operationTimeoutMs),
                new StorageClassOperator(vertx, client),
                new NodeOperator(vertx, client),
                zkScalerProvider,
                metricsProvider,
                adminClientProvider,
                zlf,
                restartEventPublisher);
    }

    /**
     * Constructor
     *
     * @param serviceOperations                     Service operator
     * @param routeOperations                       Route operator
     * @param imageStreamOperations                 ImageStream operator
     * @param stsOperations                         StatefulSet operator
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
     * @param podDisruptionBudgetV1Beta1Operator    Pod Disruption Budget v1beta1 operator
     * @param podOperations                         Pod operator
     * @param ingressOperations                     Ingress operator
     * @param buildConfigOperations                 Build Config operator
     * @param buildOperations                       Build operator
     * @param kafkaOperator                         Kafka CR operator
     * @param connectOperator                       KafkaConnect CR operator
     * @param mirrorMakerOperator                   KafkaMirrorMaker CR operator
     * @param kafkaBridgeOperator                   KafkaBridge operator
     * @param kafkaConnectorOperator                KafkaConnector operator
     * @param mirrorMaker2Operator                  KafkaMirrorMaker2 operator
     * @param kafkaRebalanceOperator                KafkaRebalance operator
     * @param strimziPodSetOperator                 StrimziPodSet operator
     * @param storageClassOperator                  StorageClass operator
     * @param nodeOperator                          Node operator
     * @param zkScalerProvider                      ZooKeeper Scaler provider
     * @param metricsProvider                       Metrics provider
     * @param adminClientProvider                   Kafka Admin client provider
     * @param zookeeperLeaderFinder                 ZooKeeper Leader Finder
     * @param restartEventsPublisher                Kubernetes Events publisher
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public ResourceOperatorSupplier(ServiceOperator serviceOperations,
                                    RouteOperator routeOperations,
                                    ImageStreamOperator imageStreamOperations,
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
                                    BuildConfigOperator buildConfigOperations,
                                    BuildOperator buildOperations,
                                    CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator,
                                    CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> connectOperator,
                                    CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> mirrorMakerOperator,
                                    CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> kafkaBridgeOperator,
                                    CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> kafkaConnectorOperator,
                                    CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mirrorMaker2Operator,
                                    CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator,
                                    StrimziPodSetOperator strimziPodSetOperator,
                                    StorageClassOperator storageClassOperator,
                                    NodeOperator nodeOperator,
                                    ZookeeperScalerProvider zkScalerProvider,
                                    MetricsProvider metricsProvider,
                                    AdminClientProvider adminClientProvider,
                                    ZookeeperLeaderFinder zookeeperLeaderFinder,
                                    KubernetesRestartEventPublisher restartEventsPublisher) {
        this.serviceOperations = serviceOperations;
        this.routeOperations = routeOperations;
        this.imageStreamOperations = imageStreamOperations;
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
        this.restartEventsPublisher = restartEventsPublisher;
    }
}
