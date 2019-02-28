/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaConnectAssemblyList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaConnect, KafkaConnectAssemblyList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> {

    private static final Logger log = LogManager.getLogger(KafkaConnectAssemblyOperator.class.getName());
    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "/logging";
    private final ServiceOperator serviceOperations;
    private final DeploymentOperator deploymentOperations;
    private final ConfigMapOperator configMapOperations;
    private final ClusterRoleBindingOperator clusterRoleBindingOperations;
    private final ServiceAccountOperator serviceAccountOperations;
    private final KafkaVersion.Lookup versions;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param deploymentOperations For operating on Deployments
     * @param serviceOperations For operating on Services
     * @param secretOperations For operating on Secrets
     */
    public KafkaConnectAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                        CertManager certManager,
                                        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectAssemblyList, DoneableKafkaConnect> connectOperator,
                                        ConfigMapOperator configMapOperations,
                                        DeploymentOperator deploymentOperations,
                                        ServiceOperator serviceOperations,
                                        SecretOperator secretOperations,
                                        NetworkPolicyOperator networkPolicyOperator,
                                        PodDisruptionBudgetOperator podDisruptionBudgetOperator,
                                        ResourceOperatorSupplier supplier,
                                        KafkaVersion.Lookup versions,
                                        ImagePullPolicy imagePullPolicy) {
        super(vertx, isOpenShift, ResourceType.CONNECT, certManager, connectOperator, secretOperations, networkPolicyOperator, podDisruptionBudgetOperator, imagePullPolicy);
        this.configMapOperations = configMapOperations;
        this.serviceOperations = serviceOperations;
        this.deploymentOperations = deploymentOperations;
        this.clusterRoleBindingOperations = supplier.clusterRoleBindingOperator;
        this.serviceAccountOperations = supplier.serviceAccountOperator;
        this.versions = versions;
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnect) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        KafkaConnectCluster connect;
        if (kafkaConnect.getSpec() == null) {
            log.error("{} spec cannot be null", kafkaConnect.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }
        try {
            connect = KafkaConnectCluster.fromCrd(kafkaConnect, versions);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

        ConfigMap logAndMetricsConfigMap = connect.generateMetricsAndLogConfigMap(connect.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) connect.getLogging()).getName()) :
                null);

        Map<String, String> annotations = new HashMap();
        annotations.put(ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(connect.ANCILLARY_CM_KEY_LOG_CONFIG));

        log.debug("{}: Updating Kafka Connect cluster", reconciliation, name, namespace);
        return  connectInitServiceAccount(namespace, connect)
                .compose(i -> deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas()))
                .compose(scale -> serviceOperations.reconcile(namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> configMapOperations.reconcile(namespace, connect.getAncillaryConfigName(), logAndMetricsConfigMap))
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, connect.getName(), connect.generatePodDisruptionBudget()))
                .compose(i -> deploymentOperations.reconcile(namespace, connect.getName(), connect.generateDeployment(annotations, isOpenShift, imagePullPolicy)))
                .compose(i -> deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()).map((Void) null));
    }

    @Override
    protected Future<Void> delete(Reconciliation reconciliation) {
        return Future.succeededFuture();
    }

    @Override
    protected List<HasMetadata> getResources(String namespace, Labels selector) {
        return Collections.EMPTY_LIST;
    }


    Future connectInitServiceAccount(String namespace, KafkaConnectCluster connect) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaConnectCluster.initContainerServiceAccountName(connect.getCluster()),
                connect.generateServiceAccount());
    }
}
