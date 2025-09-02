/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.json.JsonObject;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A Java client for the Kafka Connect REST API.
 */
public interface KafkaConnectApi {
    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/config}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to create or update.
     * @param configJson The connector configuration.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns information about the connector, including its name, config and tasks.
     */
    CompletableFuture<Map<String, Object>> createOrUpdatePutRequest(Reconciliation reconciliation, String host, int port, String connectorName, JsonObject configJson);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/config}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the config of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's config.
     */
    CompletableFuture<Map<String, String>> getConnectorConfig(Reconciliation reconciliation, String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/config}.
     *
     * @param reconciliation    The reconciliation
     * @param backOff           The backoff parameters
     * @param host              The host to make the request to.
     * @param port              The port to make the request to.
     * @param connectorName     The name of the connector to get the config of.
     *
     * @return A Future which completes with the result of the request. If the request was successful, this returns the connector's config.
     */
    CompletableFuture<Map<String, String>> getConnectorConfig(Reconciliation reconciliation, BackOff backOff, String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to create or update.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns information about the connector, including its name, config and tasks.
     */
    CompletableFuture<Map<String, Object>> getConnector(Reconciliation reconciliation, String host, int port, String connectorName);

    /**
     * Make a {@code DELETE} request to {@code /connectors/${connectorName}}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to delete.
     * @return A Future which completes with the result of the request.
     */
    CompletableFuture<Void> delete(Reconciliation reconciliation, String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/status}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the status of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's status.
     */
    CompletableFuture<Map<String, Object>> status(Reconciliation reconciliation, String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/status}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the status of.
     * @param okStatusCodes List of HTTP codes considered as success
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's status.
     */
    CompletableFuture<Map<String, Object>> status(Reconciliation reconciliation, String host, int port, String connectorName, Set<Integer> okStatusCodes);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/status}, retrying according to {@code backoff}.
     * @param reconciliation The reconciliation
     * @param backOff The backoff parameters.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the status of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's status.
     */
    CompletableFuture<Map<String, Object>> statusWithBackOff(Reconciliation reconciliation, BackOff backOff, String host, int port, String connectorName);

    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/pause}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to pause.
     * @return A Future which completes with the result of the request.
     */
    CompletableFuture<Void> pause(Reconciliation reconciliation, String host, int port, String connectorName);

    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/stop}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to pause.
     * @return A Future which completes with the result of the request.
     */
    CompletableFuture<Void> stop(Reconciliation reconciliation, String host, int port, String connectorName);

    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/resume}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to resume.
     * @return A Future which completes with the result of the request.
     */
    CompletableFuture<Void> resume(Reconciliation reconciliation,  String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors}
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connectors.
     */
    CompletableFuture<List<String>> list(Reconciliation reconciliation, String host, int port);

    /**
     * Make a {@code GET} request to {@code /connector-plugins}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connector plugins.
     */
    CompletableFuture<List<ConnectorPlugin>> listConnectorPlugins(Reconciliation reconciliation, String host, int port);

    /**
     * Make a {@code GET} request to {@code /admin/loggers}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connect loggers.
     */
    CompletableFuture<Map<String, String>> listConnectLoggers(Reconciliation reconciliation, String host, int port);

    /**
     * Make a {@code POST} request to {@code /connectors/${connectorName}/restart}.
     *
     * @param host          The host to make the request to.
     * @param port          The port to make the request to.
     * @param connectorName The name of the connector to restart.
     * @param includeTasks  Whether to restart the connector instance and task instances or just the connector.
     * @param onlyFailed    Specifies whether to restart just the instances with a FAILED status or all instances.
     * @return A Future which completes with the result of the request and the new status of the connector
     */
    CompletableFuture<Map<String, Object>> restart(String host, int port, String connectorName, boolean includeTasks, boolean onlyFailed);

    /**
     * Make a {@code POST} request to {@code /connectors/${connectorName}/tasks/${taskID}/restart}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector.
     * @param taskID The ID of the connector task to restart.
     * @return A Future which completes with the result of the request.
     */
    CompletableFuture<Void> restartTask(String host, int port, String connectorName, int taskID);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/topics}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the status of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's topics.
     */
    CompletableFuture<List<String>> getConnectorTopics(Reconciliation reconciliation, String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/offsets}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the offsets of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's offsets.
     */
    CompletableFuture<String> getConnectorOffsets(Reconciliation reconciliation, String host, int port, String connectorName);

    /**
     * Make a {@code PATCH} request to {@code /connectors/${connectorName}/offsets}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to alter the offsets of.
     * @param newOffsets The new offsets for the connector.
     * @return A Future which completes with the result of the request.
     */
    CompletableFuture<Void> alterConnectorOffsets(Reconciliation reconciliation, String host, int port, String connectorName, String newOffsets);

    /**
     * Make a {@code DELETE} request to {@code /connectors/${connectorName}/offsets}.
     * @param reconciliation The reconciliation
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to reset the offsets of.
     * @return A Future which completes with the result of the request.
     */
    CompletableFuture<Void> resetConnectorOffsets(Reconciliation reconciliation, String host, int port, String connectorName);
}

class ConnectRestException extends RuntimeException {
    private final int statusCode;

    ConnectRestException(String method, String path, int statusCode, String statusMessage, String message) {
        super(method + " " + path + " returned " + statusCode + " (" + statusMessage + "): " + message);
        this.statusCode = statusCode;
    }

    public ConnectRestException(HttpResponse response, String message) {
        this(response.request().method(), response.uri().getPath(), response.statusCode(), String.valueOf(response.statusCode()), message);
    }

    ConnectRestException(String method, String path, int statusCode, String statusMessage, String message, Throwable cause) {
        super(method + " " + path + " returned " + statusCode + " (" + statusMessage + "): " + message, cause);
        this.statusCode = statusCode;
    }

    public ConnectRestException(HttpResponse response, String message, Throwable cause) {
        this(response.request().method(), response.uri().getPath(), response.statusCode(), String.valueOf(response.statusCode()), message, cause);
    }

    public int getStatusCode() {
        return statusCode;
    }
}

