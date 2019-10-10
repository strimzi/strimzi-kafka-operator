/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorSpec;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Assembly operator for a "Kafka Connector" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connector Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectorAssemblyOperator extends
        AbstractOperator<KafkaConnector, CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector>> {
    private static final Logger log = LogManager.getLogger(KafkaConnectorAssemblyOperator.class.getName());
    private final LabelSelector selector;
    private KafkaConnectApi apiClient;

    /**
     * @param vertx       The Vertx instance
     * @param selector    The selector for finding connectors
     * @param crdOperator The CRD operator
     * @param apiClient   The Kafka Connect API client
     */
    public KafkaConnectorAssemblyOperator(Vertx vertx,
                                          LabelSelector selector,
                                          CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> crdOperator,
                                          KafkaConnectApi apiClient) {
        super(vertx, KafkaConnector.RESOURCE_KIND, crdOperator);
        Objects.requireNonNull(selector);
        this.selector = selector;
        this.apiClient = apiClient;
    }

    @Override
    public Optional<LabelSelector> selector() {
        return Optional.of(selector);
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnector assemblyResource) {
        try {
            String namespace = reconciliation.namespace();
            String name = reconciliation.name();

            log.debug("{}: Creating/Updating Kafka Connector", reconciliation, name, namespace);

            KafkaConnectorSpec spec = assemblyResource.getSpec();
            JsonObject connectorConfigJson = new JsonObject()
                    .put("connector.class", spec.getClassName())
                    .put("tasks.max", spec.getTasksMax());
            spec.getConfig().forEach(cf -> connectorConfigJson.put(cf.getName(), cf.getValue()));

            return apiClient.createOrUpdatePutRequest(name, connectorConfigJson)
                    .map((Void) null);
        } catch (Throwable t) {
            return Future.failedFuture(t);
        }
    }


    /**
     * Updates the Status field of the Kafka Bridge CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     * <p>
     * //     * @param kafkaBridgeAssembly The CR of Kafka Bridge
     * //     * @param reconciliation Reconciliation information
     * //     * @param desiredStatus The KafkaBridgeStatus which should be set
     * <p>
     * //     * @return
     */
//    Future<Void> updateStatus(KafkaBridge kafkaBridgeAssembly, Reconciliation reconciliation, KafkaBridgeStatus desiredStatus) {
//        Future<Void> updateStatusFuture = Future.future();
//
//        resourceOperator.getAsync(kafkaBridgeAssembly.getMetadata().getNamespace(), kafkaBridgeAssembly.getMetadata().getName()).setHandler(getRes -> {
//            if (getRes.succeeded()) {
//                KafkaBridge kafkaBridge = getRes.result();
//
//                if (kafkaBridge != null) {
//                    KafkaBridgeStatus currentStatus = kafkaBridge.getStatus();
//
//                    StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);
//
//                    if (!ksDiff.isEmpty()) {
//                        KafkaBridge resourceWithNewStatus = new KafkaBridgeBuilder(kafkaBridge).withStatus(desiredStatus).build();
//                        ((CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList, DoneableKafkaBridge>) resourceOperator).updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
//                            if (updateRes.succeeded()) {
//                                log.debug("{}: Completed status update", reconciliation);
//                                updateStatusFuture.complete();
//                            } else {
//                                log.error("{}: Failed to update status", reconciliation, updateRes.cause());
//                                updateStatusFuture.fail(updateRes.cause());
//                            }
//                        });
//                    } else {
//                        log.debug("{}: Status did not change", reconciliation);
//                        updateStatusFuture.complete();
//                    }
//                } else {
//                    log.error("{}: Current Kafka Bridge resource not found", reconciliation);
//                    updateStatusFuture.fail("Current Kafka Bridge resource not found");
//                }
//            } else {
//                log.error("{}: Failed to get the current Kafka Bridge resource and its status", reconciliation, getRes.cause());
//                updateStatusFuture.fail(getRes.cause());
//            }
//        });
//
//        return updateStatusFuture;
//    }

//    Future<ReconcileResult<ServiceAccount>> kafkaBridgeServiceAccount(String namespace, KafkaBridgeCluster bridge) {
//        return serviceAccountOperations.reconcile(namespace,
//                KafkaBridgeResources.serviceAccountName(bridge.getCluster()),
//                bridge.generateServiceAccount());
//    }

    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return apiClient.delete(reconciliation.name()).map(Boolean.TRUE);
    }

    @Override
    public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
        return CompositeFuture.join(super.allResourceNames(namespace),
            apiClient.list()).map(cf -> {
                Set<NamespaceAndName> combined = cf.resultAt(0);
                List<String> runningConnectors = cf.resultAt(1);
                combined.addAll(runningConnectors.stream().map(n -> new NamespaceAndName(namespace, n)).collect(Collectors.toList()));
                return combined;
            });
    }

    // TODO status
    // TODO check common error cases (e.g. missing connector in image) result in understandable logs and statuses
    // TODO check that noop calls to apiClient.createOrUpdatePutRequest() don't cause rebalances


}
