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
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.ReconcileResult;
import io.strimzi.operator.cluster.operator.resource.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.ServiceOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.model.Storage;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
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
public class KafkaAssemblyOperator extends AbstractAssemblyOperator {
    private static final Logger log = LogManager.getLogger(KafkaAssemblyOperator.class.getName());

    private final long operationTimeoutMs;

    private final ZookeeperSetOperator zkSetOperations;
    private final KafkaSetOperator kafkaSetOperations;
    private final ServiceOperator serviceOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;

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
                                 ConfigMapOperator configMapOperations,
                                 ServiceOperator serviceOperations,
                                 ZookeeperSetOperator zkSetOperations,
                                 KafkaSetOperator kafkaSetOperations,
                                 PvcOperator pvcOperations,
                                 DeploymentOperator deploymentOperations,
                                 SecretOperator secretOperations) {
        super(vertx, isOpenShift, AssemblyType.KAFKA, configMapOperations, secretOperations);
        this.operationTimeoutMs = operationTimeoutMs;
        this.zkSetOperations = zkSetOperations;
        this.serviceOperations = serviceOperations;
        this.pvcOperations = pvcOperations;
        this.deploymentOperations = deploymentOperations;
        this.kafkaSetOperations = kafkaSetOperations;
    }

    @Override
    public void createOrUpdate(Reconciliation reconciliation, ConfigMap assemblyCm, List<Secret> assemblySecrets, Handler<AsyncResult<Void>> handler) {
        Future<Void> f = Future.<Void>future().setHandler(handler);
        createOrUpdateZk(reconciliation, assemblyCm)
            .compose(i -> createOrUpdateKafka(reconciliation, assemblyCm, assemblySecrets))
            .compose(i -> createOrUpdateTopicOperator(reconciliation, assemblyCm))
            .compose(ar -> f.complete(), f);
    }

    /**
     * Brings the description of a Kafka cluster entity
     */
    private static class KafkaClusterDescription {

        private final KafkaCluster kafka;
        private final Service service;
        private final Service headlessService;
        private final ConfigMap metricsConfigMap;
        private final StatefulSet statefulSet;
        private final Secret clientsCASecret;
        private final Secret clientsPublicKeySecret;
        private final Secret brokersClientsSecret;
        private final Secret brokersInternalSecret;
        private ReconcileResult<StatefulSet> diffs;

        KafkaClusterDescription(KafkaCluster kafka, Service service, Service headlessService,
                                ConfigMap metricsConfigMap, StatefulSet statefulSet,
                                Secret clientsCASecret, Secret clientsPublicKeySecret,
                                Secret brokersClientsSecret, Secret brokersInternalSecret) {
            this.kafka = kafka;
            this.service = service;
            this.headlessService = headlessService;
            this.metricsConfigMap = metricsConfigMap;
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

        ConfigMap metricsConfigMap() {
            return this.metricsConfigMap;
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

    private final Future<KafkaClusterDescription> getKafkaCluster(ConfigMap assemblyCm, List<Secret> assemblySecrets) {
        Future<KafkaClusterDescription> fut = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        KafkaCluster kafka = KafkaCluster.fromDescription(assemblyCm, assemblySecrets);

                        KafkaClusterDescription desc =
                                new KafkaClusterDescription(kafka, kafka.generateService(), kafka.generateHeadlessService(),
                                        kafka.generateMetricsConfigMap(), kafka.generateStatefulSet(isOpenShift),
                                        kafka.generateClientsCASecret(), kafka.generateClientsPublicKeySecret(),
                                        kafka.generateBrokersClientsSecret(), kafka.generateBrokersInternalSecret());

                        future.complete(desc);
                    } catch (Exception e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete((KafkaClusterDescription) res.result());
                    } else {
                        fut.fail("");
                    }
                }
        );
        return fut;
    }

    private final Future<Void> createOrUpdateKafka(Reconciliation reconciliation, ConfigMap assemblyCm, List<Secret> assemblySecrets) {
        String namespace = assemblyCm.getMetadata().getNamespace();
        String name = assemblyCm.getMetadata().getName();
        log.debug("{}: create/update kafka {}", reconciliation, name);

        Future<Void> chainFuture = Future.future();
        getKafkaCluster(assemblyCm, assemblySecrets)
                .compose(desc -> desc.withVoid(kafkaSetOperations.scaleDown(namespace, desc.kafka().getName(), desc.kafka().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.kafka().getName(), desc.service())))
                .compose(desc -> desc.withVoid(serviceOperations.reconcile(namespace, desc.kafka().getHeadlessName(), desc.headlessService())))
                .compose(desc -> desc.withVoid(configMapOperations.reconcile(namespace, desc.kafka().getMetricsConfigName(), desc.metricsConfigMap())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, desc.kafka().getName() + "-clients-ca", desc.clientsCASecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, desc.kafka().getName() + "-clients-ca-cert", desc.clientsPublicKeySecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, desc.kafka().getName() + "-brokers-clients", desc.brokersClientsSecret())))
                .compose(desc -> desc.withVoid(secretOperations.reconcile(namespace, desc.kafka().getName() + "-brokers-internal", desc.brokersInternalSecret())))
                .compose(desc -> desc.withDiff(kafkaSetOperations.reconcile(namespace, desc.kafka().getName(), desc.statefulSet())))
                .compose(desc -> desc.withVoid(kafkaSetOperations.maybeRollingUpdate(desc.diffs().resource())))
                .compose(desc -> desc.withVoid(kafkaSetOperations.scaleUp(namespace, desc.kafka().getName(), desc.kafka().getReplicas())))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.service(), 1_000, operationTimeoutMs)))
                .compose(desc -> desc.withVoid(serviceOperations.endpointReadiness(namespace, desc.headlessService(), 1_000, operationTimeoutMs)))
                .compose(desc -> chainFuture.complete(), chainFuture);

        return chainFuture;
    };

    private final Future<CompositeFuture> deleteKafka(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.assemblyName();
        log.debug("{}: delete kafka {}", reconciliation, name);
        StatefulSet ss = kafkaSetOperations.get(namespace, KafkaCluster.kafkaClusterName(name));

        final KafkaCluster kafka = ss == null ? null : KafkaCluster.fromAssembly(ss, namespace, name);
        boolean deleteClaims = kafka != null && kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
            && kafka.getStorage().isDeleteClaim();
        List<Future> result = new ArrayList<>(4 + (deleteClaims ? kafka.getReplicas() : 0));

        result.add(configMapOperations.reconcile(namespace, KafkaCluster.metricConfigsName(name), null));
        result.add(serviceOperations.reconcile(namespace, KafkaCluster.kafkaClusterName(name), null));
        result.add(serviceOperations.reconcile(namespace, KafkaCluster.headlessName(name), null));
        result.add(kafkaSetOperations.reconcile(namespace, KafkaCluster.kafkaClusterName(name), null));

        if (deleteClaims) {
            log.debug("{}: delete kafka {} PVCs", reconciliation, name);

            for (int i = 0; i < kafka.getReplicas(); i++) {
                result.add(pvcOperations.reconcile(namespace,
                        kafka.getPersistentVolumeClaimName(i), null));
            }
        }

        return CompositeFuture.join(result);
    };

    private final Future<Void> createOrUpdateZk(Reconciliation reconciliation, ConfigMap assemblyCm) {
        String namespace = assemblyCm.getMetadata().getNamespace();
        String name = assemblyCm.getMetadata().getName();
        log.debug("{}: create/update zookeeper {}", reconciliation, name);
        ZookeeperCluster zk;
        try {
            zk = ZookeeperCluster.fromConfigMap(assemblyCm);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
        Service service = zk.generateService();
        Service headlessService = zk.generateHeadlessService();
        Future<Void> chainFuture = Future.future();
        zkSetOperations.scaleDown(namespace, zk.getName(), zk.getReplicas())
                .compose(scale -> serviceOperations.reconcile(namespace, zk.getName(), service))
                .compose(i -> serviceOperations.reconcile(namespace, zk.getHeadlessName(), headlessService))
                .compose(i -> configMapOperations.reconcile(namespace, zk.getMetricsConfigName(), zk.generateMetricsConfigMap()))
                .compose(i -> zkSetOperations.reconcile(namespace, zk.getName(), zk.generateStatefulSet(isOpenShift)))
                .compose(diffs -> zkSetOperations.maybeRollingUpdate(diffs.resource()))
                .compose(i -> zkSetOperations.scaleUp(namespace, zk.getName(), zk.getReplicas()))
                .compose(scale -> serviceOperations.endpointReadiness(namespace, service, 1_000, operationTimeoutMs))
                .compose(i -> serviceOperations.endpointReadiness(namespace, headlessService, 1_000, operationTimeoutMs))
                .compose(chainFuture::complete, chainFuture);
        return chainFuture;
    };

    private final Future<CompositeFuture> deleteZk(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.assemblyName();
        log.debug("{}: delete zookeeper {}", reconciliation, name);
        StatefulSet ss = zkSetOperations.get(namespace, ZookeeperCluster.zookeeperClusterName(name));
        ZookeeperCluster zk = ss == null ? null : ZookeeperCluster.fromAssembly(ss, namespace, name);
        boolean deleteClaims = zk != null && zk.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                && zk.getStorage().isDeleteClaim();
        List<Future> result = new ArrayList<>(4 + (deleteClaims ? zk.getReplicas() : 0));

        result.add(configMapOperations.reconcile(namespace, ZookeeperCluster.zookeeperMetricsName(name), null));
        result.add(serviceOperations.reconcile(namespace, ZookeeperCluster.zookeeperClusterName(name), null));
        result.add(serviceOperations.reconcile(namespace, ZookeeperCluster.zookeeperHeadlessName(name), null));
        result.add(zkSetOperations.reconcile(namespace, ZookeeperCluster.zookeeperClusterName(name), null));

        if (deleteClaims) {
            log.debug("{}: delete zookeeper {} PVCs", reconciliation, name);

            for (int i = 0; i < zk.getReplicas(); i++) {
                result.add(pvcOperations.reconcile(namespace, zk.getPersistentVolumeClaimName(i), null));
            }
        }

        return CompositeFuture.join(result);
    };

    private final Future<ReconcileResult<Deployment>> createOrUpdateTopicOperator(Reconciliation reconciliation, ConfigMap assemblyCm) {
        String namespace = assemblyCm.getMetadata().getNamespace();
        String name = assemblyCm.getMetadata().getName();
        log.debug("{}: create/update topic operator {}", reconciliation, name);
        TopicOperator topicOperator;
        try {
            topicOperator = TopicOperator.fromConfigMap(assemblyCm);
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
        result.addAll(configMapOperations.list(namespace, Labels.forType(AssemblyType.KAFKA)));
        return result;
    }
}