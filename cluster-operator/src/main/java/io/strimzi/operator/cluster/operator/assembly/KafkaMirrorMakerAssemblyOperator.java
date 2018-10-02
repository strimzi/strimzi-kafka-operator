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
import io.strimzi.operator.cluster.model.KafkaMirrorMakerCluster;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
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

    private final DeploymentOperator deploymentOperations;
    private final ConfigMapOperator configMapOperations;
    private final ServiceOperator serviceOperations;

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
                                            ServiceOperator serviceOperations) {
        super(vertx, isOpenShift, ResourceType.MIRRORMAKER, certManager, mirrorMakerOperator, secretOperations, networkPolicyOperator);
        this.deploymentOperations = deploymentOperations;
        this.configMapOperations = configMapOperations;
        this.serviceOperations = serviceOperations;
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker assemblyResource) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        KafkaMirrorMakerCluster mirror;
        try {
            mirror = KafkaMirrorMakerCluster.fromCrd(assemblyResource);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

        ConfigMap logAndMetricsConfigMap = mirror.generateMetricsAndLogConfigMap(mirror.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) mirror.getLogging()).getName()) :
                null);

        Map<String, String> annotations = new HashMap();
        annotations.put("strimzi.io/logging", logAndMetricsConfigMap.getData().get(mirror.ANCILLARY_CM_KEY_LOG_CONFIG));

        log.debug("{}: Updating Kafka Mirror Maker cluster", reconciliation, name, namespace);
        return deploymentOperations.scaleDown(namespace, mirror.getName(), mirror.getReplicas())
                .compose(scale -> serviceOperations.reconcile(namespace, mirror.getServiceName(), mirror.generateService()))
                .compose(i -> configMapOperations.reconcile(namespace, mirror.getAncillaryConfigName(), logAndMetricsConfigMap))
                .compose(i -> deploymentOperations.reconcile(namespace, mirror.getName(), mirror.generateDeployment(annotations)))
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
}
