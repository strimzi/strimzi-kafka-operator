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
 * Operator for a {@code KafkaConnector} CRs.
 * This is managed via the {@code KafkaConnectAssemblyOperator}, rather than directly by the {@code ClusterOperator}.
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
