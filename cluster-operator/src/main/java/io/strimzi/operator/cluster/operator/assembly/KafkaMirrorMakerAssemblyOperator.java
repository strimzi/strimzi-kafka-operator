/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerSpec;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaMirrorMakerCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Assembly operator for a "Kafka Mirror Maker" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Mirror Maker Deployment and related Services</li>
 * </ul>
 */
// Deprecation is suppressed because of KafkaMirrorMaker
@SuppressWarnings("deprecation")
public class KafkaMirrorMakerAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, Resource<KafkaMirrorMaker>, KafkaMirrorMakerSpec, KafkaMirrorMakerStatus> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaMirrorMakerAssemblyOperator.class.getName());

    private final DeploymentOperator deploymentOperations;
    private final KafkaVersion.Lookup versions;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param certManager Certificate manager
     * @param passwordGenerator Password generator
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaMirrorMakerAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                            CertManager certManager, PasswordGenerator passwordGenerator,
                                            ResourceOperatorSupplier supplier,
                                            ClusterOperatorConfig config) {
        super(vertx, pfa, KafkaMirrorMaker.RESOURCE_KIND, certManager, passwordGenerator, supplier.mirrorMakerOperator, supplier, config);
        this.deploymentOperations = supplier.deploymentOperations;
        this.versions = config.versions();
    }

    @Override
    protected Future<KafkaMirrorMakerStatus> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker assemblyResource) {
        String namespace = reconciliation.namespace();
        KafkaMirrorMakerCluster mirror;
        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = new KafkaMirrorMakerStatus();

        try {
            mirror = KafkaMirrorMakerCluster.fromCrd(reconciliation, assemblyResource, versions);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaMirrorMakerStatus, Future.failedFuture(e));
            return Future.failedFuture(new ReconciliationException(kafkaMirrorMakerStatus, e));
        }

        Map<String, String> annotations = new HashMap<>(1);

        KafkaClientAuthentication authConsumer = assemblyResource.getSpec().getConsumer().getAuthentication();
        List<CertSecretSource> trustedCertificatesConsumer = assemblyResource.getSpec().getConsumer().getTls() == null ? Collections.emptyList() : assemblyResource.getSpec().getConsumer().getTls().getTrustedCertificates();
        KafkaClientAuthentication authProducer = assemblyResource.getSpec().getProducer().getAuthentication();
        List<CertSecretSource> trustedCertificatesProducer = assemblyResource.getSpec().getProducer().getTls() == null ? Collections.emptyList() : assemblyResource.getSpec().getProducer().getTls().getTrustedCertificates();

        Promise<KafkaMirrorMakerStatus> createOrUpdatePromise = Promise.promise();

        boolean mirrorHasZeroReplicas = mirror.getReplicas() == 0;

        LOGGER.debugCr(reconciliation, "Updating Kafka Mirror Maker cluster");
        mirrorMakerServiceAccount(reconciliation, namespace, mirror)
                .compose(i -> deploymentOperations.scaleDown(reconciliation, namespace, mirror.getComponentName(), mirror.getReplicas()))
                .compose(i -> Util.metricsAndLogging(reconciliation, configMapOperations, namespace, mirror.getLogging(), mirror.getMetricsConfigInCm()))
                .compose(metricsAndLoggingCm -> {
                    ConfigMap logAndMetricsConfigMap = mirror.generateMetricsAndLogConfigMap(metricsAndLoggingCm);
                    annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, logAndMetricsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG));
                    return configMapOperations.reconcile(reconciliation, namespace, mirror.getAncillaryConfigMapName(), logAndMetricsConfigMap);
                })
                .compose(i -> pfa.hasPodDisruptionBudgetV1() ? podDisruptionBudgetOperator.reconcile(reconciliation, namespace, mirror.getComponentName(), mirror.generatePodDisruptionBudget()) : Future.succeededFuture())
                .compose(i -> !pfa.hasPodDisruptionBudgetV1() ? podDisruptionBudgetV1Beta1Operator.reconcile(reconciliation, namespace, mirror.getComponentName(), mirror.generatePodDisruptionBudgetV1Beta1()) : Future.succeededFuture())
                .compose(i -> CompositeFuture.join(Util.authTlsHash(secretOperations, namespace, authConsumer, trustedCertificatesConsumer),
                        Util.authTlsHash(secretOperations, namespace, authProducer, trustedCertificatesProducer)))
                .compose(hashFut -> {
                    if (hashFut != null) {
                        annotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString((int) hashFut.resultAt(0) + (int) hashFut.resultAt(1)));
                    }
                    return Future.succeededFuture();
                })
                .compose(i -> deploymentOperations.reconcile(reconciliation, namespace, mirror.getComponentName(), mirror.generateDeployment(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                .compose(i -> deploymentOperations.scaleUp(reconciliation, namespace, mirror.getComponentName(), mirror.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(reconciliation, namespace, mirror.getComponentName(), 1_000, operationTimeoutMs))
                .compose(i -> mirrorHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(reconciliation, namespace, mirror.getComponentName(), 1_000, operationTimeoutMs))
                .onComplete(reconciliationResult -> {
                        StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaMirrorMakerStatus, reconciliationResult);

                        kafkaMirrorMakerStatus.setReplicas(mirror.getReplicas());
                        kafkaMirrorMakerStatus.setLabelSelector(mirror.getSelectorLabels().toSelectorString());

                        if (reconciliationResult.succeeded())   {
                            createOrUpdatePromise.complete(kafkaMirrorMakerStatus);
                        } else {
                            createOrUpdatePromise.fail(new ReconciliationException(kafkaMirrorMakerStatus, reconciliationResult.cause()));
                        }
                }
            );

        return createOrUpdatePromise.future();
    }

    @Override
    protected KafkaMirrorMakerStatus createStatus() {
        return new KafkaMirrorMakerStatus();
    }

    Future<ReconcileResult<ServiceAccount>> mirrorMakerServiceAccount(Reconciliation reconciliation, String namespace, KafkaMirrorMakerCluster mirror) {
        return serviceAccountOperations.reconcile(reconciliation, namespace,
                KafkaMirrorMakerResources.serviceAccountName(mirror.getCluster()),
                mirror.generateServiceAccount());
    }
}
