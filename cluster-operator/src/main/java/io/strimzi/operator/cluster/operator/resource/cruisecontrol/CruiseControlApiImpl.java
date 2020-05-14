/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

public class CruiseControlApiImpl implements CruiseControlApi {

    private static final boolean HTTP_CLIENT_ACTIVITY_LOGGING = false;
    private static final int HTTP_DEFAULT_IDLE_TIMEOUT = 0; // seconds

    private final Vertx vertx;
    private final int idleTimeout;

    public CruiseControlApiImpl(Vertx vertx) {
        this(vertx, HTTP_DEFAULT_IDLE_TIMEOUT);
    }

    public CruiseControlApiImpl(Vertx vertx, int idleTimeout) {
        this.vertx = vertx;
        this.idleTimeout = idleTimeout;
    }

    @Override
    public Future<CruiseControlResponse> getCruiseControlState(String host, int port, boolean verbose) {
        return getCruiseControlState(host, port, verbose, null);
    }

    @SuppressWarnings("deprecation")
    public Future<CruiseControlResponse> getCruiseControlState(String host, int port, boolean verbose, String userTaskId) {

        Promise<CruiseControlResponse> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions()
                .setIdleTimeout(idleTimeout)
                .setLogActivity(HTTP_CLIENT_ACTIVITY_LOGGING);

        String path = new PathBuilder(CruiseControlEndpoints.STATE)
                .addParameter(CruiseControlParameters.JSON, "true")
                .addParameter(CruiseControlParameters.VERBOSE, String.valueOf(verbose))
                .build();

        HttpClientRequest request = vertx.createHttpClient(options)
                .get(port, host, path, response -> {
                    response.exceptionHandler(result::fail);
                    if (response.statusCode() == 200 || response.statusCode() == 201) {
                        String userTaskID = response.getHeader(CC_REST_API_USER_ID_HEADER);
                        response.bodyHandler(buffer -> {
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
                                "Unexpected status code " + response.statusCode() + " for request to " + host + ":" + port + path));
                    }
                })
                .exceptionHandler(t -> httpExceptionHandler(result, t));

        if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT) {
            request.setTimeout(idleTimeout * 1000);
        }

        if (userTaskId != null) {
            request.putHeader(CC_REST_API_USER_ID_HEADER, userTaskId);
        }

        request.end();

        return result.future();
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlRebalanceResponse> rebalance(String host, int port, RebalanceOptions rbOptions, String userTaskId) {

        if (rbOptions == null && userTaskId == null) {
            return Future.factory.failedFuture(
                    new IllegalArgumentException("Either rebalance options or user task ID should be supplied, both were null"));
        }

        Promise<CruiseControlRebalanceResponse> result = Promise.promise();
        HttpClientOptions httpOptions = new HttpClientOptions()
                .setIdleTimeout(idleTimeout)
                .setLogActivity(HTTP_CLIENT_ACTIVITY_LOGGING);

        String path = new PathBuilder(CruiseControlEndpoints.REBALANCE)
                .addParameter(CruiseControlParameters.JSON, "true")
                .addRebalanceParameters(rbOptions)
                .build();

        HttpClientRequest request = vertx.createHttpClient(httpOptions)
                .post(port, host, path, response -> {
                    response.exceptionHandler(result::fail);
                    if (response.statusCode() == 200 || response.statusCode() == 201) {
                        response.bodyHandler(buffer -> {
                            String userTaskID = response.getHeader(CC_REST_API_USER_ID_HEADER);
                            JsonObject json = buffer.toJsonObject();
                            CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                            result.complete(ccResponse);
                        });
                    } else if (response.statusCode() == 202) {
                        response.bodyHandler(buffer -> {
                            String userTaskID = response.getHeader(CC_REST_API_USER_ID_HEADER);
                            JsonObject json = buffer.toJsonObject();
                            CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                            if (json.containsKey(CC_REST_API_PROGRESS_KEY)) {
                                // If the response contains a "progress" key then the rebalance proposal has not yet completed processing
                                ccResponse.setProposalIsStillCalculating(true);
                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Error for request: " + host + ":" + port + path +
                                        ". 202 Status code did not contain progress key. Server returned: " +
                                        ccResponse.getJson().toString()));
                            }
                            result.complete(ccResponse);
                        });
                    } else if (response.statusCode() == 500) {
                        response.bodyHandler(buffer -> {
                            String userTaskID = response.getHeader(CC_REST_API_USER_ID_HEADER);
                            JsonObject json = buffer.toJsonObject();
                            if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                // If there was a client side error, check whether it was due to not enough data being available
                                if (json.getString(CC_REST_API_ERROR_KEY).contains("NotEnoughValidWindowsException")) {
                                    CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                                    ccResponse.setNotEnoughDataForProposal(true);
                                    result.complete(ccResponse);
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
                                "Unexpected status code " + response.statusCode() + " for request to " + host + ":" + port + path));
                    }
                })
                .exceptionHandler(t -> httpExceptionHandler(result, t));

        if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT) {
            request.setTimeout(idleTimeout * 1000);
        }

        if (userTaskId != null) {
            request.putHeader(CC_REST_API_USER_ID_HEADER, userTaskId);
        }

        request.end();

        return result.future();
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlUserTaskResponse> getUserTaskStatus(String host, int port, String userTaskId) {

        Promise<CruiseControlUserTaskResponse> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions()
                .setIdleTimeout(idleTimeout)
                .setLogActivity(HTTP_CLIENT_ACTIVITY_LOGGING);

        PathBuilder pathBuilder = new PathBuilder(CruiseControlEndpoints.USER_TASKS)
                        .addParameter(CruiseControlParameters.JSON, "true")
                        .addParameter(CruiseControlParameters.FETCH_COMPLETE, "true");

        if (userTaskId != null) {
            pathBuilder.addParameter(CruiseControlParameters.USER_TASK_IDS, userTaskId);
        }

        String path = pathBuilder.build();

        HttpClientRequest request = vertx.createHttpClient(options)
                .get(port, host, path, response -> {
                    response.exceptionHandler(result::fail);
                    if (response.statusCode() == 200 || response.statusCode() == 201) {
                        String userTaskID = response.getHeader(CC_REST_API_USER_ID_HEADER);
                        response.bodyHandler(buffer -> {
                            JsonObject json = buffer.toJsonObject();
                            JsonObject jsonUserTask = json.getJsonArray("userTasks").getJsonObject(0);
                            // This should not be an error with a 200 status but we play it safe
                            if (jsonUserTask.containsKey(CC_REST_API_ERROR_KEY)) {
                                result.fail(new CruiseControlRestException(
                                        "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                json.getString(CC_REST_API_ERROR_KEY)));
                            }
                            JsonObject statusJson = new JsonObject();
                            String taskStatus = jsonUserTask.getString("Status");
                            statusJson.put("Status", taskStatus);
                            // The status could be ACTIVE in which case there will not be a "summary" so we check that we are
                            // in a state that actually has that key.
                            if (taskStatus.equals(CruiseControlUserTaskStatus.IN_EXECUTION.toString()) ||
                                    taskStatus.equals(CruiseControlUserTaskStatus.COMPLETED.toString())) {
                                // We now need to extract the original response which is in a raw string (not nicely formatted JSON)
                                statusJson.put("summary", ((JsonObject) Json.decodeValue(jsonUserTask.getString("originalResponse"))).getJsonObject("summary"));
                            }
                            CruiseControlUserTaskResponse ccResponse = new CruiseControlUserTaskResponse(userTaskID, statusJson);
                            result.complete(ccResponse);
                        });
                    } else if (response.statusCode() == 500) {
                        response.bodyHandler(buffer -> {
                            JsonObject json = buffer.toJsonObject();
                            if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                if (json.getString(CC_REST_API_ERROR_KEY).contains("Error happened in fetching response for task")) {
                                    // This is to deal with a bug in the CC rest API that will error out if you ask for fetch_completed_task=true
                                    // for a task that has COMPLETED_WITH_ERROR. Upstream Bug: https://github.com/linkedin/cruise-control/issues/1187
                                    CruiseControlUserTaskResponse ccResponse = new CruiseControlUserTaskResponse(null, json);
                                    ccResponse.setCompletedWithError(true);
                                    result.complete(ccResponse);
                                } else {
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
                                "Unexpected status code " + response.statusCode() + " for GET request to " +
                                host + ":" + port + path));
                    }
                })
                .exceptionHandler(t -> httpExceptionHandler(result, t));

        if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT) {
            request.setTimeout(idleTimeout * 1000);
        }

        request.end();

        return result.future();
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlResponse> stopExecution(String host, int port) {

        Promise<CruiseControlResponse> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions()
                .setIdleTimeout(idleTimeout)
                .setLogActivity(HTTP_CLIENT_ACTIVITY_LOGGING);

        String path = new PathBuilder(CruiseControlEndpoints.STOP)
                        .addParameter(CruiseControlParameters.JSON, "true").build();

        HttpClientRequest request = vertx.createHttpClient(options)
                .post(port, host, path, response -> {
                    response.exceptionHandler(result::fail);
                    if (response.statusCode() == 200 || response.statusCode() == 201) {
                        String userTaskID = response.getHeader(CC_REST_API_USER_ID_HEADER);
                        response.bodyHandler(buffer -> {
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
                                "Unexpected status code " + response.statusCode()  + " for GET request to " +
                                host + ":" + port + path));
                    }
                })
                .exceptionHandler(t -> httpExceptionHandler(result, t));

        if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT) {
            request.setTimeout(idleTimeout * 1000);
        }

        request.end();

        return result.future();
    }

    private void httpExceptionHandler(Promise<? extends CruiseControlResponse> result, Throwable t) {
        if (t instanceof TimeoutException) {
            // Vert.x throws a NoStackTraceTimeoutException (inherits from TimeoutException) when the request times out
            // goint to catch and raise a TimeoutException instead
            result.fail(new TimeoutException(t.getMessage()));
        } else if (t instanceof ConnectException) {
            // Vert.x throws a AnnotatedConnectException (inherits from ConnectException) when the request times out
            // goint to catch and raise a ConnectException instead
            result.fail(new ConnectException(t.getMessage()));
        } else {
            result.fail(t);
        }
    }
}
