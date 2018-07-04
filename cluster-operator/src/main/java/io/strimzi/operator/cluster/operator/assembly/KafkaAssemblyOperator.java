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
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.DoneableKafkaAssembly;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.ReconcileResult;
import io.strimzi.operator.cluster.operator.resource.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.strimzi.operator.cluster.model.TopicOperator.topicOperatorName;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaAssembly, KafkaAssemblyList, DoneableKafkaAssembly, Resource<KafkaAssembly, DoneableKafkaAssembly>> {
    private static final Logger log = LogManager.getLogger(KafkaAssemblyOperator.class.getName());

    private final long operationTimeoutMs;

    private final ZookeeperSetOperator zkSetOperations;
    private final KafkaSetOperator kafkaSetOperations;
    private final ServiceOperator serviceOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;
    private final ConfigMapOperator configMapOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param serviceOperations For operating on Services
     * @param zkSetOperations For operating on StatefulSets
     * @param pvcOperations For operating on PersistentVolumeClaims
     * @param deploymentOperations For operating on Deployments
     * @param secretOperations For operating on Secrets
     */
    public KafkaAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                 long operationTimeoutMs,
                                 CertManager certManager,
                                 CrdOperator<KubernetesClient, KafkaAssembly, KafkaAssemblyList, DoneableKafkaAssembly> kafkaAssemblyCrdOperator,
                                 ConfigMapOperator configMapOperations,
                                 ServiceOperator serviceOperations,
                                 ZookeeperSetOperator zkSetOperations,
                                 KafkaSetOperator kafkaSetOperations,
                                 PvcOperator pvcOperations,
                                 DeploymentOperator deploymentOperations,
                                SecretOperator secretOperations) {
        super(vertx, isOpenShift, AssemblyType.KAFKA, certManager, kafkaAssemblyCrdOperator, secretOperations);
        this.operationTimeoutMs = operationTimeoutMs;
        this.zkSetOperations = zkSetOperations;
        this.serviceOperations = serviceOperations;
        this.pvcOperations = pvcOperations;
        this.deploymentOperations = deploymentOperations;
        this.kafkaSetOperations = kafkaSetOperations;
        this.configMapOperations = configMapOperations;
    }

    @Override
    public void createOrUpdate(Reconciliation reconciliation, KafkaAssembly kafkaAssembly, List<Secret> assemblySecrets, Handler<AsyncResult<Void>> handler) {
        Future<Void> f = Future.<Void>future().setHandler(handler);
        createOrUpdateZk(reconciliation, kafkaAssembly, assemblySecrets)
            .compose(i -> createOrUpdateKafka(reconciliation, kafkaAssembly, assemblySecrets))
            .compose(i -> createOrUpdateTopicOperator(reconciliation, kafkaAssembly))
            .compose(ar -> f.complete(), f);
    }

    /**
     * Brings the description of a Zookeeper cluster entity
     * An instance of this class is used during the Future(s) composition when a Zookeeper cluster
     * is created or updated. It brings information used from a call to the next one and can be
     * enriched if the subsequent call needs more information.
     */
    private static class ZookeeperClusterDescription {

        private final ZookeeperCluster zookeeper;
        private final Service service;
        private final Service headlessService;
        private final ConfigMap metricsAndLogsConfigMap;
        private final StatefulSet statefulSet;
        private final Secret nodesSecret;
        private ReconcileResult<StatefulSet> diffs;

        ZookeeperClusterDescription(ZookeeperCluster zookeeper, Service service, Service headlessService,
                                    ConfigMap metricsAndLogsConfigMap, StatefulSet statefulSet, Secret nodesSecret) {
            this.zookeeper = zookeeper;
            this.service = service;
            this.headlessService = headlessService;
            this.metricsAndLogsConfigMap = metricsAndLogsConfigMap;
            this.statefulSet = statefulSet;
            this.nodesSecret = nodesSecret;
        }

        ZookeeperCluster zookeeper() {
            return this.zookeeper;
        }

        Service service() {
            return this.service;
        }

        Service headlessService() {
            return this.headlessService;
        }

        ConfigMap metricsAndLogsConfigMap() {
            return this.metricsAndLogsConfigMap;
        }

        StatefulSet statefulSet() {
            return this.statefulSet;
        }

        Secret nodesSecret() {
            return this.nodesSecret;
        }

        ReconcileResult<StatefulSet> diffs() {
            return this.diffs;
        }

        Future<ZookeeperClusterDescription> withDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.diffs = rr;
                return this;
            });
        }

        Future<ZookeeperClusterDescription> withVoid(Future<?> r) {
            return r.map(this);
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
        private final Service service;
        private final Service headlessService;
        private final ConfigMap metricsAndLogsConfigMap;
        private final StatefulSet statefulSet;
        private final Secret clientsCASecret;
        private final Secret clientsPublicKeySecret;
        private final Secret brokersClientsSecret;
        private final Secret brokersInternalSecret;
        private ReconcileResult<StatefulSet> diffs;

        KafkaClusterDescription(KafkaCluster kafka, Service service, Service headlessService,
                                ConfigMap metricsAndLogsConfigMap, StatefulSet statefulSet,
                                Secret clientsCASecret, Secret clientsPublicKeySecret,
                                Secret brokersClientsSecret, Secret brokersInternalSecret) {
            this.kafka = kafka;
            this.service = service;
            this.headlessService = headlessService;
            this.metricsAndLogsConfigMap = metricsAndLogsConfigMap;
            this.statefulSet = statefulSet;
            this.clientsCASecret = clientsCASecret;
            this.clientsPublicKeySecret = clientsPublicKeySecret;
            this.brokersClientsSecret = brokersClientsSecret;
            this.brokersInternalSecret = brokersInternalSecret;
        }

        KafkaCluster kafka() {
            return this.kafka;
        }

        Service service() {
            return this.service;
        }

        Service headlessService() {
            return this.headlessService;
        }

        ConfigMap metricsAndLogsConfigMap() {
            return this.metricsAndLogsConfigMap;
        }

        StatefulSet statefulSet() {
            return this.statefulSet;
        }

        Secret clientsCASecret() {
            return this.clientsCASecret;
        }

        Secret clientsPublicKeySecret() {
            return this.clientsPublicKeySecret;
        }

        Secret brokersClientsSecret() {
            return this.brokersClientsSecret;
        }

        Secret brokersInternalSecret() {
            return this.brokersInternalSecret;
        }

        ReconcileResult<StatefulSet> diffs() {
            return this.diffs;
        }

        Future<KafkaClusterDescription> withDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.diffs = rr;
                return this;
            });
        }

        Future<KafkaClusterDescription> withVoid(Future<?> r) {
            return r.map(this);
        }
    }

    private final Future<KafkaClusterDescription> getKafkaClusterDescription(KafkaAssembly assemblyCm, List<Secret> assemblySecrets) {
        Future<KafkaClusterDescription> fut = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    KafkaCluster kafka = KafkaCluster.fromCrd(certManager, assemblyCm, assemblySecrets);

                    ConfigMap logAndMetricsConfigMap = kafka.generateMetricsAndLogConfigMap(
                            kafka.getLogging() instanceof ExternalLogging ?
                                    configMapOperations.get(assemblyCm.getMetadata().getNamespace(), ((ExternalLogging) kafka.getLogging()).getName()) :
                                    null);
                    KafkaClusterDescription desc =
                            new KafkaClusterDescription(kafka, kafka.generateService(), kafka.generateHeadlessService(),
                                    logAndMetricsConfigMap, kafka.generateStatefulSet(isOpenShift),
                                    kafka.generateClientsCASecret(), kafka.generateClientsPublicKeySecret(),
                                    kafka.generateBrokersClientsSecret(), kafka.generateBrokersInternalSecret());

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

    private final Future<Void> createOrUpdateKafka(Reconciliation reconciliation, KafkaAssembly assemblyCm, List<Secret> assemblySecrets) {
        String namespace = assemblyCm.getMetadata().getNamespace();
        String name = assemblyCm.getMetadata().getName();
        log.debug("{}: create/update kafka {}", reconciliation, name);

        Future<Void> chainFuture = Future.future();
        getKafkaClusterDescription(assemblyCm, assemblySecrets)
                .compose(desc -> desc.withVoid(kafkaSetOperations.scaleDown(namespace, desc.kafka().getName(), desc.kafka().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.kafka().getName(), desc.service())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.kafka().getHeadlessName(), desc.headlessService())))
                .compose(desc -> desc.withVoid(configMapOperations.reconcile(namespace, desc.kafka().getAncillaryConfigName(), desc.metricsAndLogsConfigMap())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, KafkaCluster.clientsCASecretName(name), desc.clientsCASecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, KafkaCluster.clientsPublicKeyName(name), desc.clientsPublicKeySecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, KafkaCluster.brokersClientsSecretName(name), desc.brokersClientsSecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, KafkaCluster.brokersInternalSecretName(name), desc.brokersInternalSecret())))
                .compose(desc -> desc.withDiff(kafkaSetOperations.reconcile(namespace, desc.kafka().getName(), desc.statefulSet())))
                .compose(desc -> desc.withVoid(kafkaSetOperations.maybeRollingUpdate(desc.diffs().resource())))
                .compose(desc -> desc.withVoid(kafkaSetOperations.scaleUp(namespace, desc.kafka().getName(), desc.kafka().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.service(), 1_000, operationTimeoutMs)))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.headlessService(), 1_000, operationTimeoutMs)))
                .compose(desc -> chainFuture.complete(), chainFuture);

        return chainFuture;
    }

    private final Future<CompositeFuture> deleteKafka(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.assemblyName();
        log.debug("{}: delete kafka {}", reconciliation, name);
        String kafkaSsName = KafkaCluster.kafkaClusterName(name);
        StatefulSet ss = kafkaSetOperations.get(namespace, kafkaSsName);
        boolean deleteClaims = ss == null ? false : KafkaCluster.deleteClaim(ss);
        List<Future> result = new ArrayList<>(8 + (deleteClaims ? ss.getSpec().getReplicas() : 0));

        result.add(configMapOperations.reconcile(namespace, KafkaCluster.metricAndLogConfigsName(name), null));
        result.add(serviceOperations.reconcile(namespace, KafkaCluster.kafkaClusterName(name), null));
        result.add(serviceOperations.reconcile(namespace, KafkaCluster.headlessName(name), null));
        result.add(kafkaSetOperations.reconcile(namespace, KafkaCluster.kafkaClusterName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.clientsCASecretName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.clientsPublicKeyName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.brokersClientsSecretName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.brokersInternalSecretName(name), null));

        if (deleteClaims) {
            log.debug("{}: delete kafka {} PVCs", reconciliation, name);

            for (int i = 0; i < ss.getSpec().getReplicas(); i++) {
                result.add(pvcOperations.reconcile(namespace,
                        KafkaCluster.getPersistentVolumeClaimName(kafkaSsName, i), null));
            }
        }

        return CompositeFuture.join(result);
    }

    private final Future<ZookeeperClusterDescription> getZookeeperClusterDescription(KafkaAssembly kafkaAssembly, List<Secret> assemblySecrets) {
        Future<ZookeeperClusterDescription> fut = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    ZookeeperCluster zk = ZookeeperCluster.fromCrd(certManager, kafkaAssembly, assemblySecrets);

                    ConfigMap logAndMetricsConfigMap = zk.generateMetricsAndLogConfigMap(zk.getLogging() instanceof ExternalLogging ?
                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) zk.getLogging()).getName()) :
                            null);

                    ZookeeperClusterDescription desc =
                            new ZookeeperClusterDescription(zk, zk.generateService(), zk.generateHeadlessService(),
                                    logAndMetricsConfigMap, zk.generateStatefulSet(isOpenShift), zk.generateNodesSecret());

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

    private final Future<Void> createOrUpdateZk(Reconciliation reconciliation, KafkaAssembly kafkaAssembly, List<Secret> assemblySecrets) {
        String namespace = kafkaAssembly.getMetadata().getNamespace();
        String name = kafkaAssembly.getMetadata().getName();
        log.debug("{}: create/update zookeeper {}", reconciliation, name);
        Future<Void> chainFuture = Future.future();
        getZookeeperClusterDescription(kafkaAssembly, assemblySecrets)
                .compose(desc -> desc.withVoid(zkSetOperations.scaleDown(namespace, desc.zookeeper().getName(), desc.zookeeper().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.zookeeper().getName(), desc.service())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.zookeeper().getHeadlessName(), desc.headlessService())))
                .compose(desc -> desc.withVoid(configMapOperations.reconcile(namespace, desc.zookeeper().getAncillaryConfigName(), desc.metricsAndLogsConfigMap())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, ZookeeperCluster.nodesSecretName(name), desc.nodesSecret())))
                .compose(desc -> desc.withDiff(zkSetOperations.reconcile(namespace, desc.zookeeper().getName(), desc.statefulSet())))
                .compose(desc -> desc.withVoid(zkSetOperations.maybeRollingUpdate(desc.diffs().resource())))
                .compose(desc -> desc.withVoid(zkSetOperations.scaleUp(namespace, desc.zookeeper().getName(), desc.zookeeper().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.service(), 1_000, operationTimeoutMs)))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.headlessService(), 1_000, operationTimeoutMs)))
                .compose(desc -> chainFuture.complete(), chainFuture);

        return chainFuture;
    }

    private final Future<CompositeFuture> deleteZk(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.assemblyName();
        log.debug("{}: delete zookeeper {}", reconciliation, name);
        String zkSsName = ZookeeperCluster.zookeeperClusterName(name);
        StatefulSet ss = zkSetOperations.get(namespace, zkSsName);
        boolean deleteClaims = ss == null ? false : ZookeeperCluster.deleteClaim(ss);
        List<Future> result = new ArrayList<>(4 + (deleteClaims ? ss.getSpec().getReplicas() : 0));

        result.add(configMapOperations.reconcile(namespace, ZookeeperCluster.zookeeperMetricAndLogConfigsName(name), null));
        result.add(serviceOperations.reconcile(namespace, zkSsName, null));
        result.add(serviceOperations.reconcile(namespace, ZookeeperCluster.zookeeperHeadlessName(name), null));
        result.add(zkSetOperations.reconcile(namespace, zkSsName, null));
        result.add(secretOperations.reconcile(namespace, ZookeeperCluster.nodesSecretName(name), null));

        if (deleteClaims) {
            log.debug("{}: delete zookeeper {} PVCs", reconciliation, name);
            for (int i = 0; i < ss.getSpec().getReplicas(); i++) {
                result.add(pvcOperations.reconcile(namespace, ZookeeperCluster.getPersistentVolumeClaimName(zkSsName, i), null));
            }
        }

        return CompositeFuture.join(result);
    };

    private final Future<ReconcileResult<Deployment>> createOrUpdateTopicOperator(Reconciliation reconciliation, KafkaAssembly kafkaAssembly) {
        String namespace = kafkaAssembly.getMetadata().getNamespace();
        String name = kafkaAssembly.getMetadata().getName();
        log.debug("{}: create/update topic operator {}", reconciliation, name);
        TopicOperator topicOperator;
        try {
            topicOperator = TopicOperator.fromCrd(kafkaAssembly);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
        Deployment deployment = topicOperator != null ? topicOperator.generateDeployment() : null;
        return deploymentOperations.reconcile(namespace, topicOperatorName(name), deployment);
    };

    private final Future<ReconcileResult<Deployment>> deleteTopicOperator(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.assemblyName();
        log.debug("{}: delete topic operator {}", reconciliation, name);
        return deploymentOperations.reconcile(namespace, topicOperatorName(name), null);
    };

    @Override
    protected void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        Future<Void> f = Future.<Void>future().setHandler(handler);
        deleteTopicOperator(reconciliation)
                .compose(i -> deleteKafka(reconciliation))
                .compose(i -> deleteZk(reconciliation))
                .compose(ar -> f.complete(), f);
    }

    @Override
    protected List<HasMetadata> getResources(String namespace) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(kafkaSetOperations.list(namespace, Labels.forType(AssemblyType.KAFKA)));
        result.addAll(zkSetOperations.list(namespace, Labels.forType(AssemblyType.KAFKA)));
        result.addAll(deploymentOperations.list(namespace, Labels.forType(AssemblyType.KAFKA)));
        result.addAll(serviceOperations.list(namespace, Labels.forType(AssemblyType.KAFKA)));
        result.addAll(resourceOperator.list(namespace, Labels.forType(AssemblyType.KAFKA)));
        return result;
    }
}