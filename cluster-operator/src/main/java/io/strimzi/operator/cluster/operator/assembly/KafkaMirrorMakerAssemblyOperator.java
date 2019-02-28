/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaMirrorMakerCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Assembly operator for a "Kafka Mirror Maker" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Mirror Maker Deployment and related Services</li>
 * </ul>
 */
public class KafkaMirrorMakerAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>> {

    private static final Logger log = LogManager.getLogger(KafkaMirrorMakerAssemblyOperator.class.getName());
    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "/logging";

    private final DeploymentOperator deploymentOperations;
    private final ConfigMapOperator configMapOperations;
    private final ServiceOperator serviceOperations;
    private final ClusterRoleBindingOperator clusterRoleBindingOperations;
    private final ServiceAccountOperator serviceAccountOperations;
    private final KafkaVersion.Lookup versions;

    /**
     * @param vertx                      The Vertx instance
     * @param isOpenShift                Whether we're running with OpenShift
     * @param mirrorMakerOperator        For operating on MirrorMakers
     * @param networkPolicyOperator      For operating on NetworkPolicies
     * @param configMapOperations        For operating on ConfigMaps
     * @param secretOperations           For operating on Secrets
     */
    public KafkaMirrorMakerAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                            CertManager certManager,
                                            CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker> mirrorMakerOperator,
                                            SecretOperator secretOperations,
                                            ConfigMapOperator configMapOperations,
                                            NetworkPolicyOperator networkPolicyOperator,
                                            DeploymentOperator deploymentOperations,
                                            ServiceOperator serviceOperations,
                                            PodDisruptionBudgetOperator podDisruptionBudgetOperator,
                                            ResourceOperatorSupplier supplier,
                                            KafkaVersion.Lookup versions,
                                            ImagePullPolicy imagePullPolicy) {
        super(vertx, isOpenShift, ResourceType.MIRRORMAKER, certManager, mirrorMakerOperator, secretOperations, networkPolicyOperator, podDisruptionBudgetOperator, imagePullPolicy);
        this.deploymentOperations = deploymentOperations;
        this.configMapOperations = configMapOperations;
        this.serviceOperations = serviceOperations;
        this.clusterRoleBindingOperations = supplier.clusterRoleBindingOperator;
        this.serviceAccountOperations = supplier.serviceAccountOperator;
        this.versions = versions;
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker assemblyResource) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        KafkaMirrorMakerCluster mirror;
        if (assemblyResource.getSpec() == null) {
            log.error("{} spec cannot be null", assemblyResource.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }
        try {
            mirror = KafkaMirrorMakerCluster.fromCrd(assemblyResource, versions);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

        ConfigMap logAndMetricsConfigMap = mirror.generateMetricsAndLogConfigMap(mirror.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) mirror.getLogging()).getName()) :
                null);

        Map<String, String> annotations = new HashMap();
        annotations.put(ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(mirror.ANCILLARY_CM_KEY_LOG_CONFIG));

        log.debug("{}: Updating Kafka Mirror Maker cluster", reconciliation, name, namespace);
        return mirrorMakerInitServiceAccount(namespace, mirror)
                .compose(i -> deploymentOperations.scaleDown(namespace, mirror.getName(), mirror.getReplicas()))
                .compose(scale -> serviceOperations.reconcile(namespace, mirror.getServiceName(), mirror.generateService()))
                .compose(i -> configMapOperations.reconcile(namespace, mirror.getAncillaryConfigName(), logAndMetricsConfigMap))
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, mirror.getName(), mirror.generatePodDisruptionBudget()))
                .compose(i -> deploymentOperations.reconcile(namespace, mirror.getName(), mirror.generateDeployment(annotations, isOpenShift, imagePullPolicy)))
                .compose(i -> deploymentOperations.scaleUp(namespace, mirror.getName(), mirror.getReplicas()).map((Void) null));
    }

    @Override
    protected Future<Void> delete(Reconciliation reconciliation) {
        return Future.succeededFuture();
    }

    @Override
    protected List<HasMetadata> getResources(String namespace, Labels selector) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(deploymentOperations.list(namespace, selector));
        result.addAll(resourceOperator.list(namespace, selector));
        
        return result;
    }

    Future mirrorMakerInitServiceAccount(String namespace, KafkaMirrorMakerCluster mirror) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaMirrorMakerCluster.initContainerServiceAccountName(mirror.getCluster()),
                mirror.generateServiceAccount());
    }
}
