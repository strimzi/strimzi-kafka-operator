/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaConnectS2ICluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

/**
 * <p>Assembly operator for a "Kafka Connect S2I" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 *     <li>An ImageBuildStream</li>
 *     <li>A BuildConfig</li>
 * </ul>
 */
public class KafkaConnectS2IAssemblyOperator extends AbstractAssemblyOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> {

    private static final Logger log = LogManager.getLogger(KafkaConnectS2IAssemblyOperator.class.getName());
    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "/logging";
    private final DeploymentConfigOperator deploymentConfigOperations;
    private final ImageStreamOperator imagesStreamOperations;
    private final BuildConfigOperator buildConfigOperations;
    private final KafkaVersion.Lookup versions;

    /**
     * @param vertx                     The Vertx instance
     * @param pfa                       Platform features availability properties
     * @param certManager               Certificate manager
     * @param supplier                  Supplies the operators for different resources
     * @param config                    ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    @SuppressWarnings("checkstyle:parameternumber")
    public KafkaConnectS2IAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                           CertManager certManager,
                                           ResourceOperatorSupplier supplier,
                                           ClusterOperatorConfig config) {
        super(vertx, pfa, ResourceType.CONNECT_S2I, certManager, supplier.connectS2IOperator, supplier, config);
        this.deploymentConfigOperations = supplier.deploymentConfigOperations;
        this.imagesStreamOperations = supplier.imagesStreamOperations;
        this.buildConfigOperations = supplier.buildConfigOperations;
        this.versions = config.versions();
    }

    @Override
    public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnectS2I kafkaConnectS2I) {
        String namespace = reconciliation.namespace();
        if (kafkaConnectS2I.getSpec() == null) {
            log.error("{} spec cannot be null", kafkaConnectS2I.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }
        if (pfa.hasImages() && pfa.hasApps() && pfa.hasBuilds()) {
            KafkaConnectS2ICluster connect;
            try {
                connect = KafkaConnectS2ICluster.fromCrd(kafkaConnectS2I, versions);
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
            connect.generateBuildConfig();
            ConfigMap logAndMetricsConfigMap = connect.generateMetricsAndLogConfigMap(connect.getLogging() instanceof ExternalLogging ?
                    configMapOperations.get(namespace, ((ExternalLogging) connect.getLogging()).getName()) :
                    null);

            HashMap<String, String> annotations = new HashMap<>();
            annotations.put(ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(connect.ANCILLARY_CM_KEY_LOG_CONFIG));

            return connectServiceAccount(namespace, connect)
                    .compose(i -> deploymentConfigOperations.scaleDown(namespace, connect.getName(), connect.getReplicas()))
                    .compose(scale -> serviceOperations.reconcile(namespace, connect.getServiceName(), connect.generateService()))
                    .compose(i -> configMapOperations.reconcile(namespace, connect.getAncillaryConfigName(), logAndMetricsConfigMap))
                    .compose(i -> deploymentConfigOperations.reconcile(namespace, connect.getName(), connect.generateDeploymentConfig(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                    .compose(i -> imagesStreamOperations.reconcile(namespace, connect.getSourceImageStreamName(), connect.generateSourceImageStream()))
                    .compose(i -> imagesStreamOperations.reconcile(namespace, connect.getName(), connect.generateTargetImageStream()))
                    .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, connect.getName(), connect.generatePodDisruptionBudget()))
                    .compose(i -> buildConfigOperations.reconcile(namespace, connect.getName(), connect.generateBuildConfig()))
                    .compose(i -> deploymentConfigOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()).map((Void) null));
        } else {
            return Future.failedFuture("The OpenShift build, image or apps APIs are not available in this Kubernetes cluster. Kafka Connect S2I deployment cannot be enabled.");
        }
    }

    Future<ReconcileResult<ServiceAccount>> connectServiceAccount(String namespace, KafkaConnectCluster connect) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaConnectCluster.containerServiceAccountName(connect.getCluster()),
                connect.generateServiceAccount());
    }
}
