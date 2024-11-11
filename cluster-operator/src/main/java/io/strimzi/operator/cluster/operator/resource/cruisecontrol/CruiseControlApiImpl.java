/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.CruiseControlUtil;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.TimeoutException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlHeaders;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlRebalanceKeys;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlHeaders.USER_TASK_ID_HEADER;

/**
 * Implementation of the Cruise Control API client
 */
public class CruiseControlApiImpl implements CruiseControlApi {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CruiseControlApiImpl.class);
    /**
     * Default timeout for the HTTP client (-1 means use the clients default)
     */
    public static final int HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS = -1;
    private static final String STATUS_KEY = "Status";
    private final long idleTimeout;
    private final boolean apiSslEnabled;
    private final HTTPHeader authHttpHeader;
    private final PemTrustSet pemTrustSet;
    private final HttpClient httpClient;
    /**
     * Constructor
     *
     * @param idleTimeout       Idle timeout
     * @param ccSecret          Cruise Control Secret
     * @param ccApiSecret       Cruise Control API Secret
     * @param apiAuthEnabled    Flag indicating if authentication is enabled
     * @param apiSslEnabled     Flag indicating if TLS is enabled
     */
    public CruiseControlApiImpl(int idleTimeout, Secret ccSecret, Secret ccApiSecret, Boolean apiAuthEnabled, boolean apiSslEnabled) {
        this.idleTimeout = idleTimeout;
        this.apiSslEnabled = apiSslEnabled;
        this.authHttpHeader = getAuthHttpHeader(apiAuthEnabled, ccApiSecret);
        this.pemTrustSet = new PemTrustSet(ccSecret);
        this.httpClient = buildHttpClient();
    }

    @Override
    public CompletableFuture<CruiseControlResponse> getCruiseControlState(Reconciliation reconciliation, String host, int port, boolean verbose) {
        String path = new PathBuilder(CruiseControlEndpoints.STATE)
                .withParameter(CruiseControlParameters.VERBOSE, String.valueOf(verbose))
                .withParameter(CruiseControlParameters.JSON, "true")
                .build();

        HttpRequest.Builder builder;
        builder = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s://%s:%d%s", apiSslEnabled ? "https" : "http", host, port, path)))
                .GET();


        if (authHttpHeader != null) {
            builder.header(authHttpHeader.getName(), authHttpHeader.getValue());
        }

        if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
            builder.timeout(Duration.ofSeconds(idleTimeout));
        }

        HttpRequest request = builder.build();
        LOGGER.traceOp("Request: {}", request);

        LOGGER.debugCr(reconciliation, "Sending GET request to {}", path);
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    // send request and handle response
                    LOGGER.traceCr(reconciliation, "Response: {}, body: {}", response, response.body());
                    int statusCode = response.statusCode();
                    if (statusCode == 200 || statusCode == 201) {
                        String userTaskID = response.headers().firstValue(CruiseControlHeaders.USER_TASK_ID_HEADER).orElse("");
                        JsonObject json = new JsonObject(response.body());
                        LOGGER.debugCr(reconciliation, "Got {} response to GET request to {} : userTaskID = {}", response.statusCode(), path, userTaskID);
                        if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                            return CompletableFuture.failedFuture(new CruiseControlRestException(
                                    "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                            json.getString(CC_REST_API_ERROR_KEY)));
                        } else {
                            return CompletableFuture.completedFuture(new CruiseControlResponse(userTaskID, json));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new CruiseControlRestException(
                                "Unexpected status code " + response.statusCode() + " for request to " + host + ":" + port + path));
                    }

                })
                .exceptionally(ex -> {
                    throw httpExceptionHandler(ex, request.method(), idleTimeout);
                });
    }

    private static HTTPHeader generateAuthHttpHeader(String user, String password) {
        String headerName = "Authorization";
        String headerValue = CruiseControlUtil.buildBasicAuthValue(user, password);
        return new HTTPHeader(headerName, headerValue);
    }

    protected static HTTPHeader getAuthHttpHeader(boolean apiAuthEnabled, Secret apiSecret) {
        if (apiAuthEnabled) {
            String password = Util.asciiFieldFromSecret(apiSecret, CruiseControlApiProperties.REBALANCE_OPERATOR_PASSWORD_KEY);
            return generateAuthHttpHeader(CruiseControlApiProperties.REBALANCE_OPERATOR_USERNAME, password);
        } else {
            return null;
        }
    }

    private HttpClient buildHttpClient() {
        try {
            HttpClient.Builder builder = HttpClient.newBuilder();
            if (apiSslEnabled) {
                String trustManagerFactoryAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustManagerFactoryAlgorithm);
                trustManagerFactory.init(pemTrustSet.jksTrustStore());

                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustManagerFactory.getTrustManagers(), null);

                builder.sslContext(sslContext);
            }
            return builder.build();
        } catch (Throwable t) {
            throw new RuntimeException(String.format("HTTP client build failed: %s", t.getMessage()));
        }
    }

    private CompletableFuture<CruiseControlRebalanceResponse> internalRebalance(Reconciliation reconciliation, String host, int port, String path, String userTaskId) {
        HttpRequest.Builder builder;
        builder = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s://%s:%d%s", apiSslEnabled ? "https" : "http", host, port, path)))
                .POST(HttpRequest.BodyPublishers.noBody());


        if (authHttpHeader != null) {
            builder.header(authHttpHeader.getName(), authHttpHeader.getValue());
        }

        if (userTaskId != null) {
            builder.header(USER_TASK_ID_HEADER, userTaskId);
        }

        if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
            builder.timeout(Duration.ofSeconds(idleTimeout));
        }

        HttpRequest request = builder.build();
        LOGGER.traceOp("Request: {}", request);

        LOGGER.debugCr(reconciliation, "Sending POST request to {} with userTaskID {}", path, userTaskId);
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    // send request and handle response
                    LOGGER.traceCr(reconciliation, "Response: {}, body: {}", response, response.body());
                    int statusCode = response.statusCode();
                    if (statusCode == 200 || statusCode == 201) {
                        String userTaskID = response.headers().firstValue(CruiseControlHeaders.USER_TASK_ID_HEADER).orElse("");
                        JsonObject json = new JsonObject(response.body());
                        LOGGER.debugCr(reconciliation, "Got {} response to POST request to {} : userTaskID = {}", response.statusCode(), path, userTaskID);
                        if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                            return CompletableFuture.failedFuture(new CruiseControlRestException(
                                    "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                            json.getString(CC_REST_API_ERROR_KEY)));
                        } else {
                            return CompletableFuture.completedFuture(new CruiseControlRebalanceResponse(userTaskID, json));
                        }
                    } else if (statusCode == 202) {
                        String userTaskID = response.headers().firstValue(CruiseControlHeaders.USER_TASK_ID_HEADER).orElse("");
                        JsonObject json = new JsonObject(response.body());
                        LOGGER.debugCr(reconciliation, "Got {} response to POST request to {} : userTaskID = {}", response.statusCode(), path, userTaskID);
                        CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                        if (json.containsKey(CC_REST_API_PROGRESS_KEY)) {
                            // If the response contains a "progress" key then the rebalance proposal has not yet completed processing
                            ccResponse.setProposalStillCalculating(true);
                        } else {
                            return CompletableFuture.failedFuture(new CruiseControlRestException(
                                    "Error for request: " + host + ":" + port + path +
                                            ". 202 Status code did not contain progress key. Server returned: " +
                                            ccResponse.getJson().toString()));
                        }
                        return CompletableFuture.completedFuture(ccResponse);
                    } else if (statusCode == 500) {
                        String userTaskID = response.headers().firstValue(CruiseControlHeaders.USER_TASK_ID_HEADER).orElse("");
                        JsonObject json = new JsonObject(response.body());
                        LOGGER.debugCr(reconciliation, "Got {} response to POST request to {} : userTaskID = {}", response.statusCode(), path, userTaskID);
                        if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                            // If there was a client side error, check whether it was due to not enough data being available ...
                            if (json.getString(CC_REST_API_ERROR_KEY).contains("NotEnoughValidWindowsException")) {
                                CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                                ccResponse.setNotEnoughDataForProposal(true);
                                return CompletableFuture.completedFuture(ccResponse);
                                // ... or one or more brokers doesn't exist on a add/remove brokers rebalance request
                            } else if (json.getString(CC_REST_API_ERROR_KEY).contains("IllegalArgumentException") &&
                                    json.getString(CC_REST_API_ERROR_KEY).contains("does not exist.")) {
                                return CompletableFuture.failedFuture(new IllegalArgumentException("Some/all brokers specified don't exist"));
                            } else {
                                // If there was any other kind of error propagate this to the operator
                                return CompletableFuture.failedFuture(new CruiseControlRestException(
                                        "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                json.getString(CC_REST_API_ERROR_KEY)));
                            }
                        } else {
                            return CompletableFuture.failedFuture(new CruiseControlRestException(
                                    "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                            json));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new CruiseControlRestException(
                                "Unexpected status code " + response.statusCode() + " for request to " + host + ":" + port + path));
                    }
                })
                .exceptionally(ex -> {
                    throw httpExceptionHandler(ex, request.method(), idleTimeout);
                });
    }

    @Override
    public CompletableFuture<CruiseControlRebalanceResponse> rebalance(Reconciliation reconciliation, String host, int port, RebalanceOptions options, String userTaskId) {

        if (options == null && userTaskId == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Either rebalance options or user task ID should be supplied, both were null"));
        }

        String path = new PathBuilder(CruiseControlEndpoints.REBALANCE)
                .withParameter(CruiseControlParameters.JSON, "true")
                .withRebalanceParameters(options)
                .build();

        return internalRebalance(reconciliation, host, port, path, userTaskId);
    }

    @Override
    public CompletableFuture<CruiseControlRebalanceResponse> addBroker(Reconciliation reconciliation, String host, int port, AddBrokerOptions options, String userTaskId) {
        if (options == null && userTaskId == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Either add broker options or user task ID should be supplied, both were null"));
        }

        String path = new PathBuilder(CruiseControlEndpoints.ADD_BROKER)
                .withParameter(CruiseControlParameters.JSON, "true")
                .withAddBrokerParameters(options)
                .build();

        return internalRebalance(reconciliation, host, port, path, userTaskId);
    }

    @Override
    public CompletableFuture<CruiseControlRebalanceResponse> removeBroker(Reconciliation reconciliation, String host, int port, RemoveBrokerOptions options, String userTaskId) {
        if (options == null && userTaskId == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Either remove broker options or user task ID should be supplied, both were null"));
        }

        String path = new PathBuilder(CruiseControlEndpoints.REMOVE_BROKER)
                .withParameter(CruiseControlParameters.JSON, "true")
                .withRemoveBrokerParameters(options)
                .build();

        return internalRebalance(reconciliation, host, port, path, userTaskId);
    }

    @Override
    public CompletableFuture<CruiseControlRebalanceResponse> removeDisks(Reconciliation reconciliation, String host, int port, RemoveDisksOptions options, String userTaskId) {
        if (options == null && userTaskId == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Either remove disks options or user task ID should be supplied, both were null"));
        }

        String path = new PathBuilder(CruiseControlEndpoints.REMOVE_DISKS)
                .withParameter(CruiseControlParameters.JSON, "true")
                .withRemoveBrokerDisksParameters(options)
                .build();

        return internalRebalance(reconciliation, host, port, path, userTaskId);
    }

    @Override
    @SuppressWarnings("deprecation")
    public CompletableFuture<CruiseControlUserTasksResponse> getUserTaskStatus(Reconciliation reconciliation, String host, int port, String userTaskId) {

        PathBuilder pathBuilder = new PathBuilder(CruiseControlEndpoints.USER_TASKS)
                        .withParameter(CruiseControlParameters.JSON, "true")
                        .withParameter(CruiseControlParameters.FETCH_COMPLETE, "true");

        if (userTaskId != null) {
            pathBuilder.withParameter(CruiseControlParameters.USER_TASK_IDS, userTaskId);
        }

        String path = pathBuilder.build();

        HttpRequest.Builder builder;
        builder = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s://%s:%d%s", apiSslEnabled ? "https" : "http", host, port, path)))
                .GET();

        if (authHttpHeader != null) {
            builder.header(authHttpHeader.getName(), authHttpHeader.getValue());
        }

        if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
            builder.timeout(Duration.ofSeconds(idleTimeout));
        }

        HttpRequest request = builder.build();
        LOGGER.traceOp("Request: {}", request);

        LOGGER.debugCr(reconciliation, "Sending GET request to {}", path);
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    // send request and handle response
                    LOGGER.traceCr(reconciliation, "Response: {}, body: {}", response, response.body());
                    int statusCode = response.statusCode();
                    if (statusCode == 200 || statusCode == 201) {
                        String userTaskID = response.headers().firstValue(CruiseControlHeaders.USER_TASK_ID_HEADER).orElse("");
                        JsonObject json = new JsonObject(response.body());
                        JsonArray userTasks = json.getJsonArray("userTasks");
                        JsonObject statusJson = new JsonObject();
                        if (userTasks.isEmpty()) {
                            // This may happen if:
                            // 1. Cruise Control restarted so resetting the state because the tasks queue is not persisted
                            // 2. Task's retention time expired, or the cache has become full
                            return CompletableFuture.completedFuture(new CruiseControlUserTasksResponse(userTaskID, statusJson));
                        } else {
                            JsonObject jsonUserTask = userTasks.getJsonObject(0);
                            String taskStatusStr = jsonUserTask.getString(STATUS_KEY);
                            LOGGER.debugCr(reconciliation, "Got {} response to GET request to {} : userTaskID = {}, status = {}", response.statusCode(), path, userTaskID, taskStatusStr);
                            // This should not be an error with a 200 status but we play it safe
                            if (jsonUserTask.containsKey(CC_REST_API_ERROR_KEY)) {
                                return CompletableFuture.failedFuture(new CruiseControlRestException(
                                        "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                json.getString(CC_REST_API_ERROR_KEY)));
                            }
                            statusJson.put(STATUS_KEY, taskStatusStr);
                            CruiseControlUserTaskStatus taskStatus = CruiseControlUserTaskStatus.lookup(taskStatusStr);
                            switch (taskStatus) {
                                case ACTIVE:
                                    // If the status is ACTIVE there will not be a "summary" so we skip pulling the summary key
                                    break;
                                case IN_EXECUTION:
                                    // Tasks in execution will be rebalance tasks, so their original response will contain the summary of the rebalance they are executing
                                    // We handle these in the same way as COMPLETED tasks so we drop down to that case.
                                case COMPLETED:
                                    // Completed tasks will have the original rebalance proposal summary in their original response
                                    JsonObject originalResponse = (JsonObject) Json.decodeValue(jsonUserTask.getString(
                                            CruiseControlRebalanceKeys.ORIGINAL_RESPONSE.getKey()));
                                    statusJson.put(CruiseControlRebalanceKeys.SUMMARY.getKey(),
                                            originalResponse.getJsonObject(CruiseControlRebalanceKeys.SUMMARY.getKey()));
                                    // Extract the load before/after information for the brokers
                                    JsonObject loadBeforeOptJsonObject = originalResponse.getJsonObject(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey());
                                    if (loadBeforeOptJsonObject != null) {
                                        statusJson.put(
                                                CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey(),
                                                loadBeforeOptJsonObject);
                                    }
                                    statusJson.put(
                                            CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey(),
                                            originalResponse.getJsonObject(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey()));
                                    break;
                                case COMPLETED_WITH_ERROR:
                                    // Completed with error tasks will have "CompletedWithError" as their original response, which is not Json.
                                    statusJson.put(CruiseControlRebalanceKeys.SUMMARY.getKey(), jsonUserTask.getString(CruiseControlRebalanceKeys.ORIGINAL_RESPONSE.getKey()));
                                    break;
                                default:
                                    throw new IllegalStateException("Unexpected user task status: " + taskStatus);
                            }
                            return CompletableFuture.completedFuture(new CruiseControlUserTasksResponse(userTaskID, statusJson));
                        }
                    } else if (statusCode == 500) {
                        String userTaskID = response.headers().firstValue(CruiseControlHeaders.USER_TASK_ID_HEADER).orElse("");
                        JsonObject json = new JsonObject(response.body());
                        LOGGER.debugCr(reconciliation, "Got {} response to GET request to {} : userTaskID = {}", response.statusCode(), path, userTaskID);
                        String errorString;
                        if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                            errorString = json.getString(CC_REST_API_ERROR_KEY);
                        } else {
                            errorString = json.toString();
                        }
                        
                        if (errorString.matches(".*" + "There are already \\d+ active user tasks, which has reached the servlet capacity." + ".*")) {
                            LOGGER.debugCr(reconciliation, errorString);
                            CruiseControlUserTasksResponse ccResponse = new CruiseControlUserTasksResponse(userTaskID, json);
                            ccResponse.setMaxActiveUserTasksReached(true);
                            return CompletableFuture.completedFuture(ccResponse);
                        } else {
                            return CompletableFuture.failedFuture(new CruiseControlRestException(
                                    "Error for request: " + host + ":" + port + path + ". Server returned: " + errorString));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new CruiseControlRestException(
                                "Unexpected status code " + response.statusCode() + " for GET request to " +
                                        host + ":" + port + path));
                    }
                })
                .exceptionally(ex -> {
                    throw httpExceptionHandler(ex, request.method(), idleTimeout);
                });

    }

    @Override
    @SuppressWarnings("deprecation")
    public CompletableFuture<CruiseControlResponse> stopExecution(Reconciliation reconciliation, String host, int port) {

        String path = new PathBuilder(CruiseControlEndpoints.STOP)
                        .withParameter(CruiseControlParameters.JSON, "true").build();
        HttpRequest.Builder builder;
        builder = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s://%s:%d%s", apiSslEnabled ? "https" : "http", host, port, path)))
                .POST(HttpRequest.BodyPublishers.noBody());


        if (authHttpHeader != null) {
            builder.header(authHttpHeader.getName(), authHttpHeader.getValue());
        }

        if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
            builder.timeout(Duration.ofSeconds(idleTimeout));
        }

        HttpRequest request = builder.build();
        LOGGER.traceOp("Request: {}", request);

        LOGGER.debugCr(reconciliation, "Sending POST request to {}", path);
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    // send request and handle response
                    LOGGER.traceCr(reconciliation, "Response: {}, body: {}", response, response.body());
                    int statusCode = response.statusCode();
                    if (statusCode == 200 || statusCode == 201) {
                        String userTaskID = response.headers().firstValue(CruiseControlHeaders.USER_TASK_ID_HEADER).orElse("");
                        JsonObject json = new JsonObject(response.body());
                        LOGGER.debugCr(reconciliation, "Got {} response to POST request to {} : userTaskID = {}", response.statusCode(), path, userTaskID);
                        if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                            return CompletableFuture.failedFuture(new CruiseControlRestException(
                                    "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                            json.getString(CC_REST_API_ERROR_KEY)));
                        } else {
                            return CompletableFuture.completedFuture(new CruiseControlResponse(userTaskID, json));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new CruiseControlRestException(
                                "Unexpected status code " + response.statusCode() + " for request to " + host + ":" + port + path));
                    }

                })
                .exceptionally(ex -> {
                    throw httpExceptionHandler(ex, request.method(), idleTimeout);
                });

    }

    private RuntimeException httpExceptionHandler(Throwable ex, String requestMethod, long timeout) {
        if (ex.getCause() instanceof HttpTimeoutException) {
            return new TimeoutException("The timeout period of " + timeout * 1000 + "ms has been exceeded while executing " + requestMethod);
        } else if (ex.getCause() instanceof NoRouteToHostException || ex.getCause() instanceof ConnectException) {
            return new CruiseControlRetriableConnectionException(ex.getCause());
        } else {
            return (RuntimeException) ex.getCause();
        }
    }
}
