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
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerSpec;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaMirrorMakerCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Assembly operator for a "Kafka Mirror Maker" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Mirror Maker Deployment and related Services</li>
 * </ul>
 */
public class KafkaMirrorMakerAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>, KafkaMirrorMakerSpec, KafkaMirrorMakerStatus> {

    private static final Logger log = LogManager.getLogger(KafkaMirrorMakerAssemblyOperator.class.getName());

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
            mirror = KafkaMirrorMakerCluster.fromCrd(assemblyResource, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaMirrorMakerStatus, Future.failedFuture(e));
            return Future.failedFuture(new ReconciliationException(kafkaMirrorMakerStatus, e));
        }

        Map<String, String> annotations = new HashMap<>(1);

        Promise<KafkaMirrorMakerStatus> createOrUpdatePromise = Promise.promise();

        boolean mirrorHasZeroReplicas = mirror.getReplicas() == 0;

        log.debug("{}: Updating Kafka Mirror Maker cluster", reconciliation);
        mirrorMakerServiceAccount(namespace, mirror)
                .compose(i -> deploymentOperations.scaleDown(namespace, mirror.getName(), mirror.getReplicas()))
                .compose(i -> mirrorMetricsAndLoggingConfigMap(namespace, mirror))
                .compose(metricsAndLoggingCm -> {
                    ConfigMap logAndMetricsConfigMap = mirror.generateMetricsAndLogConfigMap(metricsAndLoggingCm.loggingCm, metricsAndLoggingCm.metricsCm);
                    annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, logAndMetricsConfigMap.getData().get(mirror.ANCILLARY_CM_KEY_LOG_CONFIG));
                    return configMapOperations.reconcile(namespace, mirror.getAncillaryConfigMapName(), logAndMetricsConfigMap);
                })
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, mirror.getName(), mirror.generatePodDisruptionBudget()))
                .compose(i -> deploymentOperations.reconcile(namespace, mirror.getName(), mirror.generateDeployment(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                .compose(i -> deploymentOperations.scaleUp(namespace, mirror.getName(), mirror.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(namespace, mirror.getName(), 1_000, operationTimeoutMs))
                .compose(i -> mirrorHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(namespace, mirror.getName(), 1_000, operationTimeoutMs))
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

    Future<ReconcileResult<ServiceAccount>> mirrorMakerServiceAccount(String namespace, KafkaMirrorMakerCluster mirror) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaMirrorMakerResources.serviceAccountName(mirror.getCluster()),
                mirror.generateServiceAccount());
    }

    private Future<MetricsAndLoggingCm> mirrorMetricsAndLoggingConfigMap(String namespace, KafkaMirrorMakerCluster mirror) {
        final Future<ConfigMap> metricsCmFut;
        if (mirror.isMetricsConfigured()) {
            if (mirror.getMetricsConfigInCm() instanceof JmxPrometheusExporterMetrics) {
                metricsCmFut = configMapOperations.getAsync(namespace, ((JmxPrometheusExporterMetrics) mirror.getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName());
            } else {
                log.warn("Unknown metrics type {}", mirror.getMetricsConfigInCm().getType());
                throw new InvalidResourceException("Unknown metrics type " + mirror.getMetricsConfigInCm().getType());
            }
        } else {
            metricsCmFut = Future.succeededFuture(null);
        }

        Future<ConfigMap> loggingCmFut = mirror.getLogging() instanceof ExternalLogging ?
                configMapOperations.getAsync(namespace, ((ExternalLogging) mirror.getLogging()).getName()) :
                Future.succeededFuture(null);

        return CompositeFuture.join(metricsCmFut, loggingCmFut).map(res -> new MetricsAndLoggingCm(res.resultAt(0), res.resultAt(1)));
    }
}
