/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicy;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.EntityTopicOperator;
import io.strimzi.operator.cluster.model.EntityUserOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaAssemblyList, DoneableKafka, Resource<Kafka, DoneableKafka>> {
    private static final Logger log = LogManager.getLogger(KafkaAssemblyOperator.class.getName());

    private final long operationTimeoutMs;

    private final ZookeeperSetOperator zkSetOperations;
    private final KafkaSetOperator kafkaSetOperations;
    private final ServiceOperator serviceOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;
    private final ConfigMapOperator configMapOperations;
    private final ServiceAccountOperator serviceAccountOperator;
    private final RoleBindingOperator roleBindingOperator;
    private final ClusterRoleBindingOperator clusterRoleBindingOperator;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     */
    public KafkaAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                 long operationTimeoutMs,
                                 CertManager certManager,
                                 ResourceOperatorSupplier supplier) {
        super(vertx, isOpenShift, ResourceType.KAFKA, certManager, supplier.kafkaOperator, supplier.secretOperations, supplier.networkPolicyOperator);
        this.operationTimeoutMs = operationTimeoutMs;
        this.serviceOperations = supplier.serviceOperations;
        this.zkSetOperations = supplier.zkSetOperations;
        this.kafkaSetOperations = supplier.kafkaSetOperations;
        this.configMapOperations = supplier.configMapOperations;
        this.pvcOperations = supplier.pvcOperations;
        this.deploymentOperations = supplier.deploymentOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperator;
        this.roleBindingOperator = supplier.roleBindingOperator;
        this.clusterRoleBindingOperator = supplier.clusterRoleBindingOperator;
    }

    @Override
    public Future<Void> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly, List<Secret> assemblySecrets) {

        return createOrUpdateZk(reconciliation, kafkaAssembly, assemblySecrets)
            .compose(i -> createOrUpdateKafka(reconciliation, kafkaAssembly, assemblySecrets))
            .compose(i -> createOrUpdateTopicOperator(reconciliation, kafkaAssembly, assemblySecrets))
            .compose(i -> createOrUpdateEntityOperator(reconciliation, kafkaAssembly, assemblySecrets));
    }

    /**
     * Brings the description of a Zookeeper cluster entity
     * An instance of this class is used during the Future(s) composition when a Zookeeper cluster
     * is created or updated. It brings information used from a call to the next one and can be
     * enriched if the subsequent call needs more information.
     */
    private static class ZookeeperClusterDescription {

        private final ZookeeperCluster zookeeper;
        private final Service zkService;
        private final Service zkHeadlessService;
        private final ConfigMap zkMetricsAndLogsConfigMap;
        private final StatefulSet zkStatefulSet;
        private final Secret zkNodesSecret;
        private final NetworkPolicy zkNetworkPolicy;
        private ReconcileResult<StatefulSet> zkDiffs;
        private boolean forceZkRestart;

        ZookeeperClusterDescription(ZookeeperCluster zookeeper, Service zkService, Service zkHeadlessService,
                                    ConfigMap zkMetricsAndLogsConfigMap, StatefulSet zkStatefulSet, Secret zkNodesSecret,
                                    NetworkPolicy zkNetworkPolicy) {
            this.zookeeper = zookeeper;
            this.zkService = zkService;
            this.zkHeadlessService = zkHeadlessService;
            this.zkMetricsAndLogsConfigMap = zkMetricsAndLogsConfigMap;
            this.zkStatefulSet = zkStatefulSet;
            this.zkNodesSecret = zkNodesSecret;
            this.zkNetworkPolicy = zkNetworkPolicy;
        }

        ZookeeperCluster zookeeper() {
            return this.zookeeper;
        }

        Service zkService() {
            return this.zkService;
        }

        Service zkHeadlessService() {
            return this.zkHeadlessService;
        }

        ConfigMap zkMetricsAndLogsConfigMap() {
            return this.zkMetricsAndLogsConfigMap;
        }

        StatefulSet zkStatefulSet() {
            return this.zkStatefulSet;
        }

        Secret zkNodesSecret() {
            return this.zkNodesSecret;
        }

        NetworkPolicy zkNetworkPolicy() {
            return this.zkNetworkPolicy;
        }

        private boolean isForceZkRestart() {
            return this.forceZkRestart;
        }

        ReconcileResult<StatefulSet> zkDiffs() {
            return this.zkDiffs;
        }

        Future<ZookeeperClusterDescription> withZkDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.zkDiffs = rr;
                return this;
            });
        }

        Future<ZookeeperClusterDescription> withVoid(Future<?> r) {
            return r.map(this);
        }

        Future<ZookeeperClusterDescription> withZkAncillaryCmChanged(Future<ReconcileResult<ConfigMap>> r) {
            return r.map(rr -> {
                this.forceZkRestart = rr instanceof ReconcileResult.Patched;
                return this;
            });
        }
    }

    /**
     * Brings the description of a Kafka cluster entity
     * An instance of this class is used during the Future(s) composition when a Kafka cluster
     * is created or updated. It brings information used from a call to the next one and can be
     * enriched if the subsequent call needs more information.
     */
    private static class KafkaClusterDescription {

        private final KafkaCluster kafka;
        private final Service kafkaService;
        private final Service kafkaHeadlessService;
        private final ConfigMap kafkaMetricsAndLogsConfigMap;
        private final StatefulSet kafkaStatefulSet;
        private final Secret kafkaClientsCaSecret;
        private final Secret kafkaClientsPublicKeySecret;
        private final Secret kafkaClusterPublicKeySecret;
        private final Secret kafkaBrokersInternalSecret;
        private final NetworkPolicy kafkaNetworkPolicy;
        private ReconcileResult<StatefulSet> kafkaDiffs;
        private boolean forceKafkaRestart;

        KafkaClusterDescription(KafkaCluster kafka, Service kafkaService, Service kafkaHeadlessService,
                                ConfigMap kafkaMetricsAndLogsConfigMap, StatefulSet kafkaStatefulSet,
                                Secret kafkaClientsCaSecret, Secret kafkaClientsPublicKeySecret,
                                Secret kafkaClusterPublicKeySecret, Secret kafkaBrokersInternalSecret,
                                NetworkPolicy kafkaNetworkPolicy) {
            this.kafka = kafka;
            this.kafkaService = kafkaService;
            this.kafkaHeadlessService = kafkaHeadlessService;
            this.kafkaMetricsAndLogsConfigMap = kafkaMetricsAndLogsConfigMap;
            this.kafkaStatefulSet = kafkaStatefulSet;
            this.kafkaClientsCaSecret = kafkaClientsCaSecret;
            this.kafkaClientsPublicKeySecret = kafkaClientsPublicKeySecret;
            this.kafkaClusterPublicKeySecret = kafkaClusterPublicKeySecret;
            this.kafkaBrokersInternalSecret = kafkaBrokersInternalSecret;
            this.kafkaNetworkPolicy = kafkaNetworkPolicy;
        }

        KafkaCluster kafka() {
            return this.kafka;
        }

        Service kafkaService() {
            return this.kafkaService;
        }

        Service kafkaHeadlessService() {
            return this.kafkaHeadlessService;
        }

        ConfigMap kafkaMetricsAndLogsConfigMap() {
            return this.kafkaMetricsAndLogsConfigMap;
        }

        StatefulSet kafkaStatefulSet() {
            return this.kafkaStatefulSet;
        }

        Secret kafkaClientsCaSecret() {
            return this.kafkaClientsCaSecret;
        }

        Secret kafkaClientsPublicKeySecret() {
            return this.kafkaClientsPublicKeySecret;
        }

        Secret kafkaClusterPublicKeySecret() {
            return this.kafkaClusterPublicKeySecret;
        }

        Secret kafkaBrokersInternalSecret() {
            return this.kafkaBrokersInternalSecret;
        }

        ReconcileResult<StatefulSet> kafkaDiffs() {
            return this.kafkaDiffs;
        }

        private boolean isForceKafkaRestart() {
            return this.forceKafkaRestart;
        }

        NetworkPolicy kafkaNetworkPolicy() {
            return this.kafkaNetworkPolicy;
        }

        Future<KafkaClusterDescription> withKafkaDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.kafkaDiffs = rr;
                return this;
            });
        }

        Future<KafkaClusterDescription> withVoid(Future<?> r) {
            return r.map(this);
        }

        Future<KafkaClusterDescription> withKafkaAncillaryCmChanged(Future<ReconcileResult<ConfigMap>> r) {
            return r.map(rr -> {
                this.forceKafkaRestart = rr instanceof ReconcileResult.Patched;
                return this;
            });
        }
    }

    /**
     * Brings the description of a Topic Operator entity
     * An instance of this class is used during the Future(s) composition when the Topic Operator
     * is created or updated. It brings information used from a call to the next one and can be
     * enriched if the subsequent call needs more information.
     */
    private static class TopicOperatorDescription {

        public static final TopicOperatorDescription EMPTY =
                new TopicOperatorDescription(null, null, null, null);

        private final TopicOperator topicOperator;
        private final Deployment toDeployment;
        private final Secret topicOperatorSecret;
        private final ConfigMap toMetricsAndLogsConfigMap;

        TopicOperatorDescription(TopicOperator topicOperator, Deployment toDeployment,
                                 Secret topicOperatorSecret, ConfigMap toMetricsAndLogsConfigMap) {
            this.topicOperator = topicOperator;
            this.toDeployment = toDeployment;
            this.topicOperatorSecret = topicOperatorSecret;
            this.toMetricsAndLogsConfigMap = toMetricsAndLogsConfigMap;
        }

        TopicOperator topicOperator() {
            return this.topicOperator;
        }

        Deployment toDeployment() {
            try {
                this.toDeployment.getSpec().getTemplate().getMetadata().getAnnotations().put("strimzi.io/logging", this.toMetricsAndLogsConfigMap.getData().get("log4j2.properties"));
            } catch (NullPointerException ex) {
                
            }
            return this.toDeployment;
        }

        Secret topicOperatorSecret() {
            return this.topicOperatorSecret;
        }

        ConfigMap toMetricsAndLogsConfigMap() {
            return this.toMetricsAndLogsConfigMap;
        }

        Future<TopicOperatorDescription> withVoid(Future<?> r) {
            return r.map(this);
        }
    }

    /**
     * Brings the description of a Entity Operator entity
     * An instance of this class is used during the Future(s) composition when the Entity Operator
     * is created or updated. It brings information used from a call to the next one and can be
     * enriched if the subsequent call needs more information.
     */
    private static class EntityOperatorDescription {

        public static final EntityOperatorDescription EMPTY =
                new EntityOperatorDescription(null, null, null, null, null);

        private final EntityOperator entityOperator;
        private final Deployment eoDeployment;
        private final Secret entityOperatorSecret;
        private final ConfigMap topicOperatorMetricsAndLogsConfigMap;
        private final ConfigMap userOperatorMetricsAndLogsConfigMap;

        EntityOperatorDescription(EntityOperator entityOperator, Deployment eoDeployment, Secret entityOperatorSecret,
                                  ConfigMap topicOperatorMetricsAndLogsConfigMap, ConfigMap userOperatorMetricsAndLogsConfigMap) {
            this.entityOperator = entityOperator;
            this.eoDeployment = eoDeployment;
            this.entityOperatorSecret = entityOperatorSecret;
            this.topicOperatorMetricsAndLogsConfigMap = topicOperatorMetricsAndLogsConfigMap;
            this.userOperatorMetricsAndLogsConfigMap = userOperatorMetricsAndLogsConfigMap;
        }

        EntityOperator entityOperator() {
            return this.entityOperator;
        }

        ConfigMap topicOperatorMetricsAndLogsConfigMap() {
            return topicOperatorMetricsAndLogsConfigMap;
        }

        ConfigMap userOperatorMetricsAndLogsConfigMap() {
            return userOperatorMetricsAndLogsConfigMap;
        }

        Deployment eoDeployment() {
            return eoDeployment;
        }

        Secret entityOperatorSecret() {
            return entityOperatorSecret;
        }

        Future<EntityOperatorDescription> withVoid(Future<?> r) {
            return r.map(this);
        }
    }

    private final Future<KafkaClusterDescription> getKafkaClusterDescription(Kafka kafkaAssembly, List<Secret> assemblySecrets) {
        Future<KafkaClusterDescription> fut = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    KafkaCluster kafka = KafkaCluster.fromCrd(certManager, kafkaAssembly, assemblySecrets);

                    ConfigMap logAndMetricsConfigMap = kafka.generateMetricsAndLogConfigMap(
                            kafka.getLogging() instanceof ExternalLogging ?
                                    configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) kafka.getLogging()).getName()) :
                                    null);
                    KafkaClusterDescription desc =
                            new KafkaClusterDescription(kafka, kafka.generateService(), kafka.generateHeadlessService(),
                                    logAndMetricsConfigMap, kafka.generateStatefulSet(isOpenShift),
                                    kafka.generateClientsCASecret(), kafka.generateClientsPublicKeySecret(),
                                    kafka.generateClusterPublicKeySecret(), kafka.generateBrokersSecret(),
                                    kafka.generateNetworkPolicy());

                    future.complete(desc);
                } catch (Throwable e) {
                    future.fail(e);
                }
            }, true,
            res -> {
                if (res.succeeded()) {
                    fut.complete((KafkaClusterDescription) res.result());
                } else {
                    fut.fail(res.cause());
                }
            }
        );
        return fut;
    }

    private final Future<Void> createOrUpdateKafka(Reconciliation reconciliation, Kafka kafkaAssembly, List<Secret> assemblySecrets) {
        String namespace = kafkaAssembly.getMetadata().getNamespace();
        String name = kafkaAssembly.getMetadata().getName();
        log.debug("{}: create/update kafka {}", reconciliation, name);

        Future<Void> chainFuture = Future.future();

        getKafkaClusterDescription(kafkaAssembly, assemblySecrets)
                .compose(desc -> desc.withVoid(
                        serviceAccountOperator.reconcile(namespace,
                        KafkaCluster.initContainerServiceAccountName(desc.kafka().getCluster()),
                        desc.kafka.generateInitContainerServiceAccount())))
                .compose(desc -> desc.withVoid(clusterRoleBindingOperator.reconcile(
                        KafkaCluster.initContainerClusterRoleBindingName(namespace, name),
                        desc.kafka.generateClusterRoleBinding(namespace))))
                .compose(desc -> desc.withVoid(kafkaSetOperations.scaleDown(namespace, desc.kafka().getName(), desc.kafka().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.kafka().getServiceName(), desc.kafkaService())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.kafka().getHeadlessServiceName(), desc.kafkaHeadlessService())))
                .compose(desc -> desc.withKafkaAncillaryCmChanged(configMapOperations.reconcile(namespace, desc.kafka().getAncillaryConfigName(), desc.kafkaMetricsAndLogsConfigMap())))                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, KafkaCluster.clientsCASecretName(name), desc.kafkaClientsCaSecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, KafkaCluster.clientsPublicKeyName(name), desc.kafkaClientsPublicKeySecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, KafkaCluster.clusterPublicKeyName(name), desc.kafkaClusterPublicKeySecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, KafkaCluster.brokersSecretName(name), desc.kafkaBrokersInternalSecret())))
                .compose(desc -> desc.withVoid(networkPolicyOperator.reconcile(namespace, KafkaCluster.policyName(name), desc.kafkaNetworkPolicy())))
                .compose(desc -> desc.withKafkaDiff(kafkaSetOperations.reconcile(namespace, desc.kafka().getName(), desc.kafkaStatefulSet())))
                .compose(desc -> desc.withVoid(kafkaSetOperations.maybeRollingUpdate(desc.kafkaDiffs().resource(), desc.isForceKafkaRestart())))
                .compose(desc -> desc.withVoid(kafkaSetOperations.scaleUp(namespace, desc.kafka().getName(), desc.kafka().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.kafkaService(), 1_000, operationTimeoutMs)))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.kafkaHeadlessService(), 1_000, operationTimeoutMs)))
                .compose(desc -> chainFuture.complete(), chainFuture);

        return chainFuture;
    }


    private final Future<CompositeFuture> deleteKafka(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        log.debug("{}: delete kafka {}", reconciliation, name);
        String kafkaSsName = KafkaCluster.kafkaClusterName(name);
        StatefulSet ss = kafkaSetOperations.get(namespace, kafkaSsName);
        boolean deleteClaims = ss == null ? false : KafkaCluster.deleteClaim(ss);
        List<Future> result = new ArrayList<>(8 + (deleteClaims ? ss.getSpec().getReplicas() : 0));

        result.add(configMapOperations.reconcile(namespace, KafkaCluster.metricAndLogConfigsName(name), null));
        result.add(serviceOperations.reconcile(namespace, KafkaCluster.serviceName(name), null));
        result.add(serviceOperations.reconcile(namespace, KafkaCluster.headlessServiceName(name), null));
        result.add(kafkaSetOperations.reconcile(namespace, KafkaCluster.kafkaClusterName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.clientsCASecretName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.clientsPublicKeyName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.clusterPublicKeyName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.brokersSecretName(name), null));
        result.add(networkPolicyOperator.reconcile(namespace, KafkaCluster.policyName(name), null));

        if (deleteClaims) {
            log.debug("{}: delete kafka {} PVCs", reconciliation, name);

            for (int i = 0; i < ss.getSpec().getReplicas(); i++) {
                result.add(pvcOperations.reconcile(namespace,
                        KafkaCluster.getPersistentVolumeClaimName(kafkaSsName, i), null));
            }
        }
        result.add(clusterRoleBindingOperator.reconcile(KafkaCluster.initContainerClusterRoleBindingName(namespace, name), null));
        result.add(serviceAccountOperator.reconcile(namespace, KafkaCluster.initContainerServiceAccountName(name), null));
        return CompositeFuture.join(result);
    }

    private final Future<ZookeeperClusterDescription> getZookeeperClusterDescription(Kafka kafkaAssembly, List<Secret> assemblySecrets) {
        Future<ZookeeperClusterDescription> fut = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    ZookeeperCluster zk = ZookeeperCluster.fromCrd(certManager, kafkaAssembly, assemblySecrets);

                    ConfigMap logAndMetricsConfigMap = zk.generateMetricsAndLogConfigMap(zk.getLogging() instanceof ExternalLogging ?
                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) zk.getLogging()).getName()) :
                            null);

                    NetworkPolicy networkPolicy = zk.generateNetworkPolicy();
                    ZookeeperClusterDescription desc =
                            new ZookeeperClusterDescription(zk, zk.generateService(), zk.generateHeadlessService(),
                                    logAndMetricsConfigMap, zk.generateStatefulSet(isOpenShift), zk.generateNodesSecret(), networkPolicy);

                    future.complete(desc);
                } catch (Throwable e) {
                    future.fail(e);
                }
            }, true,
            res -> {
                if (res.succeeded()) {
                    fut.complete((ZookeeperClusterDescription) res.result());
                } else {
                    fut.fail(res.cause());
                }
            }
        );

        return fut;
    }

    private final Future<Void> createOrUpdateZk(Reconciliation reconciliation, Kafka kafkaAssembly, List<Secret> assemblySecrets) {
        String namespace = kafkaAssembly.getMetadata().getNamespace();
        String name = kafkaAssembly.getMetadata().getName();
        log.debug("{}: create/update zookeeper {}", reconciliation, name);
        Future<Void> chainFuture = Future.future();
        getZookeeperClusterDescription(kafkaAssembly, assemblySecrets)
                .compose(desc -> desc.withVoid(zkSetOperations.scaleDown(namespace, desc.zookeeper().getName(), desc.zookeeper().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.zookeeper().getServiceName(), desc.zkService())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.zookeeper().getHeadlessServiceName(), desc.zkHeadlessService())))
                .compose(desc -> desc.withZkAncillaryCmChanged(configMapOperations.reconcile(namespace, desc.zookeeper().getAncillaryConfigName(), desc.zkMetricsAndLogsConfigMap())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, ZookeeperCluster.nodesSecretName(name), desc.zkNodesSecret())))
                .compose(desc -> desc.withVoid(networkPolicyOperator.reconcile(namespace, desc.zookeeper().policyName(name), desc.zkNetworkPolicy())))
                .compose(desc -> desc.withZkDiff(zkSetOperations.reconcile(namespace, desc.zookeeper().getName(), desc.zkStatefulSet())))
                .compose(desc -> desc.withVoid(zkSetOperations.maybeRollingUpdate(desc.zkDiffs().resource(), desc.isForceZkRestart())))
                .compose(desc -> desc.withVoid(zkSetOperations.scaleUp(namespace, desc.zookeeper().getName(), desc.zookeeper().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.zkService(), 1_000, operationTimeoutMs)))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.zkHeadlessService(), 1_000, operationTimeoutMs)))
                .compose(desc -> chainFuture.complete(), chainFuture);

        return chainFuture;
    }

    private final Future<CompositeFuture> deleteZk(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        log.debug("{}: delete zookeeper {}", reconciliation, name);
        String zkSsName = ZookeeperCluster.zookeeperClusterName(name);
        StatefulSet ss = zkSetOperations.get(namespace, zkSsName);
        boolean deleteClaims = ss == null ? false : ZookeeperCluster.deleteClaim(ss);
        List<Future> result = new ArrayList<>(4 + (deleteClaims ? ss.getSpec().getReplicas() : 0));

        result.add(configMapOperations.reconcile(namespace, ZookeeperCluster.zookeeperMetricAndLogConfigsName(name), null));
        result.add(serviceOperations.reconcile(namespace, ZookeeperCluster.serviceName(name), null));
        result.add(serviceOperations.reconcile(namespace, ZookeeperCluster.headlessServiceName(name), null));
        result.add(zkSetOperations.reconcile(namespace, zkSsName, null));
        result.add(secretOperations.reconcile(namespace, ZookeeperCluster.nodesSecretName(name), null));
        result.add(networkPolicyOperator.reconcile(namespace, ZookeeperCluster.policyName(name), null));

        if (deleteClaims) {
            log.debug("{}: delete zookeeper {} PVCs", reconciliation, name);
            for (int i = 0; i < ss.getSpec().getReplicas(); i++) {
                result.add(pvcOperations.reconcile(namespace, ZookeeperCluster.getPersistentVolumeClaimName(zkSsName, i), null));
            }
        }

        return CompositeFuture.join(result);
    }

    private final Future<TopicOperatorDescription> getTopicOperatorDescription(Kafka kafkaAssembly, List<Secret> assemblySecrets) {
        Future<TopicOperatorDescription> fut = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    TopicOperator topicOperator = TopicOperator.fromCrd(certManager, kafkaAssembly, assemblySecrets);

                    TopicOperatorDescription desc;
                    if (topicOperator != null) {
                        ConfigMap logAndMetricsConfigMap = topicOperator.generateMetricsAndLogConfigMap(
                                topicOperator.getLogging() instanceof ExternalLogging ?
                                        configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) topicOperator.getLogging()).getName()) :
                                        null);

                        desc = new TopicOperatorDescription(topicOperator, topicOperator.generateDeployment(),
                                topicOperator.generateSecret(), logAndMetricsConfigMap);
                    } else {
                        desc = TopicOperatorDescription.EMPTY;
                    }

                    future.complete(desc);
                } catch (Throwable e) {
                    future.fail(e);
                }
            }, true,
            res -> {
                if (res.succeeded()) {
                    fut.complete((TopicOperatorDescription) res.result());
                } else {
                    fut.fail(res.cause());
                }
            }
        );
        return fut;
    }

    private final Future<Void> createOrUpdateTopicOperator(Reconciliation reconciliation, Kafka kafkaAssembly, List<Secret> assemblySecrets) {
        String namespace = kafkaAssembly.getMetadata().getNamespace();
        String name = kafkaAssembly.getMetadata().getName();
        log.debug("{}: create/update topic operator {}", reconciliation, name);

        Future<Void> chainFuture = Future.future();
        getTopicOperatorDescription(kafkaAssembly, assemblySecrets)
                .compose(desc -> desc.withVoid(serviceAccountOperator.reconcile(namespace,
                        TopicOperator.topicOperatorServiceAccountName(name),
                        desc != TopicOperatorDescription.EMPTY ? desc.topicOperator().generateServiceAccount() : null)))
                .compose(desc -> {
                    String watchedNamespace = desc.topicOperator() != null ? desc.topicOperator().getWatchedNamespace() : null;
                    return desc.withVoid(roleBindingOperator.reconcile(
                            watchedNamespace != null && !watchedNamespace.isEmpty() ?
                                    watchedNamespace : namespace,
                            TopicOperator.roleBindingName(name),
                            desc != TopicOperatorDescription.EMPTY ? desc.topicOperator().generateRoleBinding(namespace) : null));
                })
                .compose(desc -> desc.withVoid(configMapOperations.reconcile(namespace,
                        desc != TopicOperatorDescription.EMPTY ? desc.topicOperator().getAncillaryConfigName() : TopicOperator.metricAndLogConfigsName(name),
                        desc.toMetricsAndLogsConfigMap())))
                .compose(desc -> desc.withVoid(deploymentOperations.reconcile(namespace, TopicOperator.topicOperatorName(name), desc.toDeployment())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, TopicOperator.secretName(name), desc.topicOperatorSecret())))
                .compose(desc -> chainFuture.complete(), chainFuture);

        return chainFuture;
    }

    private final Future<CompositeFuture> deleteTopicOperator(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        log.debug("{}: delete topic operator {}", reconciliation, name);

        List<Future> result = new ArrayList<>(3);
        result.add(configMapOperations.reconcile(namespace, TopicOperator.metricAndLogConfigsName(name), null));
        result.add(deploymentOperations.reconcile(namespace, TopicOperator.topicOperatorName(name), null));
        result.add(secretOperations.reconcile(namespace, TopicOperator.secretName(name), null));
        result.add(roleBindingOperator.reconcile(namespace, TopicOperator.roleBindingName(name), null));
        result.add(serviceAccountOperator.reconcile(namespace, TopicOperator.topicOperatorServiceAccountName(name), null));
        return CompositeFuture.join(result);
    }

    private final Future<EntityOperatorDescription> getEntityOperatorDescription(Kafka kafkaAssembly, List<Secret> assemblySecrets) {
        Future<EntityOperatorDescription> fut = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    EntityOperator entityOperator = EntityOperator.fromCrd(certManager, kafkaAssembly, assemblySecrets);

                    EntityOperatorDescription desc;
                    if (entityOperator != null) {
                        EntityTopicOperator topicOperator = entityOperator.getTopicOperator();
                        EntityUserOperator userOperator = entityOperator.getUserOperator();

                        ConfigMap topicOperatorLogAndMetricsConfigMap = topicOperator != null ?
                                topicOperator.generateMetricsAndLogConfigMap(topicOperator.getLogging() instanceof ExternalLogging ?
                                        configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) topicOperator.getLogging()).getName()) :
                                        null) : null;

                        ConfigMap userOperatorLogAndMetricsConfigMap = userOperator != null ?
                                userOperator.generateMetricsAndLogConfigMap(userOperator.getLogging() instanceof ExternalLogging ?
                                        configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) userOperator.getLogging()).getName()) :
                                        null) : null;

                        desc = new EntityOperatorDescription(entityOperator, entityOperator.generateDeployment(),
                                entityOperator.generateSecret(), topicOperatorLogAndMetricsConfigMap, userOperatorLogAndMetricsConfigMap);
                    } else {
                        desc = EntityOperatorDescription.EMPTY;
                    }

                    future.complete(desc);
                } catch (Throwable e) {
                    future.fail(e);
                }
            }, true,
            res -> {
                if (res.succeeded()) {
                    fut.complete((EntityOperatorDescription) res.result());
                } else {
                    fut.fail(res.cause());
                }
            }
        );
        return fut;
    }

    private final Future<Void> createOrUpdateEntityOperator(Reconciliation reconciliation, Kafka kafkaAssembly, List<Secret> assemblySecrets) {
        String namespace = kafkaAssembly.getMetadata().getNamespace();
        String name = kafkaAssembly.getMetadata().getName();
        log.debug("{}: create/update entity operator {}", reconciliation, name);

        Future<Void> chainFuture = Future.future();
        getEntityOperatorDescription(kafkaAssembly, assemblySecrets)
                .compose(desc -> desc.withVoid(serviceAccountOperator.reconcile(namespace,
                        EntityOperator.entityOperatorServiceAccountName(name),
                        desc != EntityOperatorDescription.EMPTY ? desc.entityOperator().generateServiceAccount() : null)))
                .compose(desc -> {
                    String watchedNamespace = desc.entityOperator() != null && desc.entityOperator().getTopicOperator() != null ?
                            desc.entityOperator().getTopicOperator().getWatchedNamespace() : null;
                    return desc.withVoid(roleBindingOperator.reconcile(
                            watchedNamespace != null && !watchedNamespace.isEmpty() ?
                                    watchedNamespace : namespace,
                            EntityTopicOperator.roleBindingName(name),
                            desc != EntityOperatorDescription.EMPTY && desc.entityOperator().getTopicOperator() != null ?
                                    desc.entityOperator().getTopicOperator().generateRoleBinding(namespace) : null));
                })
                .compose(desc -> {
                    String watchedNamespace = desc.entityOperator() != null && desc.entityOperator().getUserOperator() != null ?
                            desc.entityOperator().getUserOperator().getWatchedNamespace() : null;
                    return desc.withVoid(roleBindingOperator.reconcile(
                            watchedNamespace != null && !watchedNamespace.isEmpty() ?
                                    watchedNamespace : namespace,
                            EntityUserOperator.roleBindingName(name),
                            desc != EntityOperatorDescription.EMPTY && desc.entityOperator().getUserOperator() != null ?
                                    desc.entityOperator().getUserOperator().generateRoleBinding(namespace) : null));
                })
                .compose(desc -> desc.withVoid(configMapOperations.reconcile(namespace,
                        desc != EntityOperatorDescription.EMPTY && desc.entityOperator().getTopicOperator() != null ?
                                desc.entityOperator().getTopicOperator().getAncillaryConfigName() : EntityTopicOperator.metricAndLogConfigsName(name),
                        desc.topicOperatorMetricsAndLogsConfigMap())))
                .compose(desc -> desc.withVoid(configMapOperations.reconcile(namespace,
                        desc != EntityOperatorDescription.EMPTY && desc.entityOperator().getUserOperator() != null ?
                                desc.entityOperator().getUserOperator().getAncillaryConfigName() : EntityUserOperator.metricAndLogConfigsName(name),
                        desc.userOperatorMetricsAndLogsConfigMap())))
                .compose(desc -> desc.withVoid(deploymentOperations.reconcile(namespace, EntityOperator.entityOperatorName(name), desc.eoDeployment())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, EntityOperator.secretName(name), desc.entityOperatorSecret())))
                .compose(desc -> chainFuture.complete(), chainFuture);

        return chainFuture;
    }

    private final Future<CompositeFuture> deleteEntityOperator(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        log.debug("{}: delete entity operator {}", reconciliation, name);

        List<Future> result = new ArrayList<>(3);
        result.add(configMapOperations.reconcile(namespace, EntityTopicOperator.metricAndLogConfigsName(name), null));
        result.add(configMapOperations.reconcile(namespace, EntityUserOperator.metricAndLogConfigsName(name), null));
        result.add(deploymentOperations.reconcile(namespace, EntityOperator.entityOperatorName(name), null));
        result.add(secretOperations.reconcile(namespace, EntityOperator.secretName(name), null));
        result.add(roleBindingOperator.reconcile(namespace, EntityTopicOperator.roleBindingName(name), null));
        result.add(roleBindingOperator.reconcile(namespace, EntityUserOperator.roleBindingName(name), null));
        result.add(serviceAccountOperator.reconcile(namespace, EntityOperator.entityOperatorServiceAccountName(name), null));
        return CompositeFuture.join(result);
    }

    @Override
    protected Future<Void> delete(Reconciliation reconciliation) {
        return deleteEntityOperator(reconciliation)
                .compose(i -> deleteTopicOperator(reconciliation))
                .compose(i -> deleteKafka(reconciliation))
                .compose(i -> deleteZk(reconciliation))
                .map((Void) null);
    }

    @Override
    protected List<HasMetadata> getResources(String namespace, Labels selector) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(kafkaSetOperations.list(namespace, selector));
        result.addAll(zkSetOperations.list(namespace, selector));
        result.addAll(deploymentOperations.list(namespace, selector));
        result.addAll(serviceOperations.list(namespace, selector));
        result.addAll(resourceOperator.list(namespace, selector));
        return result;
    }
}