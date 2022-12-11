/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.operator.cluster.operator.resource.HttpClientUtils;
import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.common.Util;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PemTrustOptions;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of the Cruise Control API client
 */
public class CruiseControlApiImpl implements CruiseControlApi {
    /**
     * Default timeout for the HTTP client (-1 means use the clients default)
     */
    public static final int HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS = -1;
    private static final boolean HTTP_CLIENT_ACTIVITY_LOGGING = false;
    private static final String STATUS_KEY = "Status";

    private final Vertx vertx;
    private final long idleTimeout;
    private boolean apiSslEnabled;
    private HTTPHeader authHttpHeader;
    private PemTrustOptions pto;

    /**
     * Constructor
     *
     * @param vertx             Vert.x instance
     * @param idleTimeout       Idle timeout
     * @param ccSecret          Cruise Control Secret
     * @param ccApiSecret       Cruise Control API Secret
     * @param apiAuthEnabled    Flag indicating if authentication is enabled
     * @param apiSslEnabled     Flag indicating if TLS is enabled
     */
    public CruiseControlApiImpl(Vertx vertx, int idleTimeout, Secret ccSecret, Secret ccApiSecret, Boolean apiAuthEnabled, boolean apiSslEnabled) {
        this.vertx = vertx;
        this.idleTimeout = idleTimeout;
        this.apiSslEnabled = apiSslEnabled;
        this.authHttpHeader = getAuthHttpHeader(apiAuthEnabled, ccApiSecret);
        this.pto = new PemTrustOptions().addCertValue(Buffer.buffer(Util.decodeFromSecret(ccSecret, "cruise-control.crt")));
    }

    @Override
    public Future<CruiseControlResponse> getCruiseControlState(String host, int port, boolean verbose) {
        return getCruiseControlState(host, port, verbose, null);
    }

    private HttpClientOptions getHttpClientOptions() {
        if (apiSslEnabled) {
            return new HttpClientOptions()
                .setLogActivity(HTTP_CLIENT_ACTIVITY_LOGGING)
                .setSsl(true)
                .setVerifyHost(true)
                .setPemTrustOptions(
                    new PemTrustOptions(pto)
                );
        } else {
            return new HttpClientOptions()
                    .setLogActivity(HTTP_CLIENT_ACTIVITY_LOGGING);
        }
    }

    private static HTTPHeader generateAuthHttpHeader(String user, String password) {
        String headerName = "Authorization";
        String headerValue = "Basic " + Util.encodeToBase64(String.join(":", user, password));

        return new HTTPHeader(headerName, headerValue);
    }

    protected static HTTPHeader getAuthHttpHeader(boolean apiAuthEnabled, Secret apiSecret) {
        if (apiAuthEnabled) {
            String password = new String(Util.decodeFromSecret(apiSecret, CruiseControl.API_ADMIN_PASSWORD_KEY), StandardCharsets.US_ASCII);
            HTTPHeader header = generateAuthHttpHeader(CruiseControl.API_ADMIN_NAME, password);
            return header;
        } else {
            return null;
        }
    }

    @SuppressWarnings("deprecation")
    private Future<CruiseControlResponse> getCruiseControlState(String host, int port, boolean verbose, String userTaskId) {

        String path = new PathBuilder(CruiseControlEndpoints.STATE)
                .withParameter(CruiseControlParameters.JSON, "true")
                .withParameter(CruiseControlParameters.VERBOSE, String.valueOf(verbose))
                .build();

        HttpClientOptions options = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, options, (httpClient, result) -> {
            httpClient.request(HttpMethod.GET, port, host, path, request -> {
                if (request.succeeded()) {

                    if (authHttpHeader != null) {
                        request.result().putHeader(authHttpHeader.getName(), authHttpHeader.getValue());
                    }

                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                                String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                response.result().bodyHandler(buffer -> {
                                    JsonObject json = buffer.toJsonObject();
                                    if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                        result.fail(new CruiseControlRestException(
                                                "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                        json.getString(CC_REST_API_ERROR_KEY)));
                                    } else {
                                        CruiseControlResponse ccResponse = new CruiseControlResponse(userTaskID, json);
                                        result.complete(ccResponse);
                                    }
                                });

                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Unexpected status code " + response.result().statusCode() + " for request to " + host + ":" + port + path));
                            }
                        } else {
                            httpExceptionHandler(result, response.cause());
                        }
                    });
                } else {
                    result.fail(request.cause());
                }

                if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
                    request.result().setTimeout(idleTimeout * 1000);
                }

                if (userTaskId != null) {
                    request.result().putHeader(CC_REST_API_USER_ID_HEADER, userTaskId);
                }
            });
        });
    }

    private void internalRebalance(String host, int port, String path, String userTaskId,
                                   AsyncResult<HttpClientRequest> request, Promise<CruiseControlRebalanceResponse> result) {
        if (request.succeeded()) {
            if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
                request.result().setTimeout(idleTimeout * 1000);
            }

            if (userTaskId != null) {
                request.result().putHeader(CC_REST_API_USER_ID_HEADER, userTaskId);
            }

            if (authHttpHeader != null) {
                request.result().putHeader(authHttpHeader.getName(), authHttpHeader.getValue());
            }

            request.result().send(response -> {
                if (response.succeeded()) {
                    if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                        response.result().bodyHandler(buffer -> {
                            String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                            JsonObject json = buffer.toJsonObject();
                            CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                            result.complete(ccResponse);
                        });
                    } else if (response.result().statusCode() == 202) {
                        response.result().bodyHandler(buffer -> {
                            String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                            JsonObject json = buffer.toJsonObject();
                            CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                            if (json.containsKey(CC_REST_API_PROGRESS_KEY)) {
                                // If the response contains a "progress" key then the rebalance proposal has not yet completed processing
                                ccResponse.setProposalStillCalculating(true);
                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Error for request: " + host + ":" + port + path +
                                                ". 202 Status code did not contain progress key. Server returned: " +
                                                ccResponse.getJson().toString()));
                            }
                            result.complete(ccResponse);
                        });
                    } else if (response.result().statusCode() == 500) {
                        response.result().bodyHandler(buffer -> {
                            String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                            JsonObject json = buffer.toJsonObject();
                            if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                // If there was a client side error, check whether it was due to not enough data being available ...
                                if (json.getString(CC_REST_API_ERROR_KEY).contains("NotEnoughValidWindowsException")) {
                                    CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                                    ccResponse.setNotEnoughDataForProposal(true);
                                    result.complete(ccResponse);
                                // ... or one or more brokers doesn't exist on a add/remove brokers rebalance request
                                } else if (json.getString(CC_REST_API_ERROR_KEY).contains("IllegalArgumentException") &&
                                            json.getString(CC_REST_API_ERROR_KEY).contains("does not exist.")) {
                                    result.fail(new IllegalArgumentException("Some/all brokers specified don't exist"));
                                } else {
                                    // If there was any other kind of error propagate this to the operator
                                    result.fail(new CruiseControlRestException(
                                            "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                    json.getString(CC_REST_API_ERROR_KEY)));
                                }
                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                json.toString()));
                            }
                        });
                    } else {
                        result.fail(new CruiseControlRestException(
                                "Unexpected status code " + response.result().statusCode() + " for request to " + host + ":" + port + path));
                    }
                } else {
                    result.fail(response.cause());
                }
            });
        } else {
            httpExceptionHandler(result, request.cause());
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlRebalanceResponse> rebalance(String host, int port, RebalanceOptions options, String userTaskId) {

        if (options == null && userTaskId == null) {
            return Future.failedFuture(
                    new IllegalArgumentException("Either rebalance options or user task ID should be supplied, both were null"));
        }

        String path = new PathBuilder(CruiseControlEndpoints.REBALANCE)
                .withParameter(CruiseControlParameters.JSON, "true")
                .withRebalanceParameters(options)
                .build();

        HttpClientOptions httpOptions = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, httpOptions, (httpClient, result) -> {
            httpClient.request(HttpMethod.POST, port, host, path, request -> internalRebalance(host, port, path, userTaskId, request, result));
        });
    }

    @Override
    public Future<CruiseControlRebalanceResponse> addBroker(String host, int port, AddBrokerOptions options, String userTaskId) {
        if (options == null && userTaskId == null) {
            return Future.failedFuture(
                    new IllegalArgumentException("Either add broker options or user task ID should be supplied, both were null"));
        }

        String path = new PathBuilder(CruiseControlEndpoints.ADD_BROKER)
                .withParameter(CruiseControlParameters.JSON, "true")
                .withAddBrokerParameters(options)
                .build();

        HttpClientOptions httpOptions = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, httpOptions, (httpClient, result) -> {
            httpClient.request(HttpMethod.POST, port, host, path, request -> internalRebalance(host, port, path, userTaskId, request, result));
        });
    }

    @Override
    public Future<CruiseControlRebalanceResponse> removeBroker(String host, int port, RemoveBrokerOptions options, String userTaskId) {
        if (options == null && userTaskId == null) {
            return Future.failedFuture(
                    new IllegalArgumentException("Either remove broker options or user task ID should be supplied, both were null"));
        }

        String path = new PathBuilder(CruiseControlEndpoints.REMOVE_BROKER)
                .withParameter(CruiseControlParameters.JSON, "true")
                .withRemoveBrokerParameters(options)
                .build();

        HttpClientOptions httpOptions = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, httpOptions, (httpClient, result) -> {
            httpClient.request(HttpMethod.POST, port, host, path, request -> internalRebalance(host, port, path, userTaskId, request, result));
        });
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlResponse> getUserTaskStatus(String host, int port, String userTaskId) {

        PathBuilder pathBuilder = new PathBuilder(CruiseControlEndpoints.USER_TASKS)
                        .withParameter(CruiseControlParameters.JSON, "true")
                        .withParameter(CruiseControlParameters.FETCH_COMPLETE, "true");

        if (userTaskId != null) {
            pathBuilder.withParameter(CruiseControlParameters.USER_TASK_IDS, userTaskId);
        }

        String path = pathBuilder.build();

        HttpClientOptions options = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, options, (httpClient, result) -> {
            httpClient.request(HttpMethod.GET, port, host, path, request -> {
                if (request.succeeded()) {

                    if (authHttpHeader != null) {
                        request.result().putHeader(authHttpHeader.getName(), authHttpHeader.getValue());
                    }

                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                                String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                response.result().bodyHandler(buffer -> {
                                    JsonObject json = buffer.toJsonObject();
                                    JsonObject jsonUserTask = json.getJsonArray("userTasks").getJsonObject(0);
                                    // This should not be an error with a 200 status but we play it safe
                                    if (jsonUserTask.containsKey(CC_REST_API_ERROR_KEY)) {
                                        result.fail(new CruiseControlRestException(
                                                "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                        json.getString(CC_REST_API_ERROR_KEY)));
                                    }
                                    JsonObject statusJson = new JsonObject();
                                    String taskStatusStr = jsonUserTask.getString(STATUS_KEY);
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
                                            statusJson.put(
                                                    CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey(),
                                                    originalResponse.getJsonObject(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey()));
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
                                    result.complete(new CruiseControlResponse(userTaskID, statusJson));
                                });
                            } else if (response.result().statusCode() == 500) {
                                response.result().bodyHandler(buffer -> {
                                    JsonObject json = buffer.toJsonObject();
                                    String errorString;
                                    if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                        errorString = json.getString(CC_REST_API_ERROR_KEY);
                                    } else {
                                        errorString = json.toString();
                                    }
                                    result.fail(new CruiseControlRestException(
                                            "Error for request: " + host + ":" + port + path + ". Server returned: " + errorString));
                                });
                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Unexpected status code " + response.result().statusCode() + " for GET request to " +
                                                host + ":" + port + path));
                            }
                        } else {
                            result.fail(response.cause());
                        }
                    });

                    if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
                        request.result().setTimeout(idleTimeout * 1000);
                    }

                } else {
                    httpExceptionHandler(result, request.cause());
                }
            });
        });
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlResponse> stopExecution(String host, int port) {

        String path = new PathBuilder(CruiseControlEndpoints.STOP)
                        .withParameter(CruiseControlParameters.JSON, "true").build();

        HttpClientOptions options = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, options, (httpClient, result) -> {
            httpClient.request(HttpMethod.POST, port, host, path, request -> {
                if (request.succeeded()) {

                    if (authHttpHeader != null) {
                        request.result().putHeader(authHttpHeader.getName(), authHttpHeader.getValue());
                    }

                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                                String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                response.result().bodyHandler(buffer -> {
                                    JsonObject json = buffer.toJsonObject();
                                    if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                        result.fail(json.getString(CC_REST_API_ERROR_KEY));
                                    } else {
                                        CruiseControlResponse ccResponse = new CruiseControlResponse(userTaskID, json);
                                        result.complete(ccResponse);
                                    }
                                });

                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Unexpected status code " + response.result().statusCode() + " for GET request to " +
                                                host + ":" + port + path));
                            }
                        } else {
                            result.fail(response.cause());
                        }
                    });
                } else {
                    httpExceptionHandler(result, request.cause());
                }
                if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
                    request.result().setTimeout(idleTimeout * 1000);
                }
            });
        });
    }

    private void httpExceptionHandler(Promise<? extends CruiseControlResponse> result, Throwable t) {
        if (t instanceof TimeoutException) {
            // Vert.x throws a NoStackTraceTimeoutException (inherits from TimeoutException) when the request times out
            // so we catch and raise a TimeoutException instead
            result.fail(new TimeoutException(t.getMessage()));
        } else if (t instanceof NoRouteToHostException || t instanceof ConnectException) {
            // Netty throws a AnnotatedNoRouteToHostException (inherits from NoRouteToHostException) when it cannot resolve the host
            // Vert.x throws a AnnotatedConnectException (inherits from ConnectException) when the request times out
            // so we catch and raise a CruiseControlRetriableConnectionException instead
            result.fail(new CruiseControlRetriableConnectionException(t));
        } else {
            result.fail(t);
        }
    }
}
