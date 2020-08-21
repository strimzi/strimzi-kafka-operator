/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.vertx.core.Future;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;

/**
 * A Java client for the Kafka Connect REST API.
 */
public interface KafkaConnectApi {
    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/config}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to create or update.
     * @param configJson The connectors configuration.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns information about the connector, including its name, config and tasks.
     */
    Future<Map<String, Object>> createOrUpdatePutRequest(String host, int port, String connectorName, JsonObject configJson);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/config}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the config of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's config.
     */
    Future<Map<String, String>> getConnectorConfig(String host, int port, String connectorName);

    public Future<Map<String, String>> getConnectorConfig(BackOff backOff, String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to create or update.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns information about the connector, including its name, config and tasks.
     */
    Future<Map<String, Object>> getConnector(String host, int port, String connectorName);

    /**
     * Make a {@code DELETE} request to {@code /connectors/${connectorName}}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to delete.
     * @return A Future which completes with the result of the request.
     */
    Future<Void> delete(String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/status}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the status of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's status.
     */
    Future<Map<String, Object>> status(String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/status}, retrying according to {@code backoff}.
     * @param backOff The backoff parameters.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the status of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's status.
     */
    Future<Map<String, Object>> statusWithBackOff(BackOff backOff, String host, int port, String connectorName);

    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/pause}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to pause.
     * @return A Future which completes with the result of the request.
     */
    Future<Void> pause(String host, int port, String connectorName);

    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/resume}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to resume.
     * @return A Future which completes with the result of the request.
     */
    Future<Void> resume(String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors}
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connectors.
     */
    Future<List<String>> list(String host, int port);

    /**
     * Make a {@code GET} request to {@code /connector-plugins}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connector plugins.
     */
    Future<List<ConnectorPlugin>> listConnectorPlugins(String host, int port);

    /**
     * Make a {@code GET} request to {@code /admin/loggers/$logger}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param desiredLogging Desired logging.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connector loggers.
     */
    Future<Void> updateConnectLoggers(String host, int port, String desiredLogging);


    /**
     * Make a {@code GET} request to {@code /admin/loggers}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connect loggers.
     */
    Future<Map<String, Map<String, String>>> listConnectLoggers(String host, int port);
}

class ConnectRestException extends RuntimeException {
    private final int statusCode;

    ConnectRestException(String method, String path, int statusCode, String statusMessage, String message) {
        super(method + " " + path + " returned " + statusCode + " (" + statusMessage + "): " + message);
        this.statusCode = statusCode;
    }

    public ConnectRestException(HttpClientResponse response, String message) {
        this(response.request().method().toString(), response.request().path(), response.statusCode(), response.statusMessage(), message);
    }

    ConnectRestException(String method, String path, int statusCode, String statusMessage, String message, Throwable cause) {
        super(method + " " + path + " returned " + statusCode + " (" + statusMessage + "): " + message, cause);
        this.statusCode = statusCode;
    }

    public ConnectRestException(HttpClientResponse response, String message, Throwable cause) {
        this(response.request().method().toString(), response.request().path(), response.statusCode(), response.statusMessage(), message, cause);
    }

    public int getStatusCode() {
        return statusCode;
    }
}

