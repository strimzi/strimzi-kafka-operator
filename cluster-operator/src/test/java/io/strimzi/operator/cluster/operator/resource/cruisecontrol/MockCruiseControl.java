/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.ReadWriteUtils;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.Parameter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlHeaders.USER_TASK_ID_HEADER;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.ACTIVE;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.COMPLETED;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.COMPLETED_WITH_ERROR;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.IN_EXECUTION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockserver.model.Header.header;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * Cruise Control mock.
 */
public class MockCruiseControl {
    private static final String CC_JSON_ROOT = "io/strimzi/operator/cluster/operator/assembly/CruiseControlJSON/";

    private static final String SEP = "-";
    private static final String REBALANCE = "rebalance";
    private static final String STATE = "state";
    private static final String NO_GOALS = "no-goals";
    private static final String VERBOSE = "verbose";
    private static final String USER_TASK = "user-task";
    private static final String RESPONSE = "response";
    
    public static final String REBALANCE_NO_GOALS_RESPONSE_UTID = REBALANCE + SEP + NO_GOALS + SEP + RESPONSE;
    public static final String REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID = REBALANCE + SEP + NO_GOALS + SEP + VERBOSE + SEP + RESPONSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS = USER_TASK + SEP + REBALANCE + SEP + NO_GOALS;
    public static final String USER_TASK_REBALANCE_NO_GOALS_VERBOSE_UTID = USER_TASK_REBALANCE_NO_GOALS + SEP + VERBOSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID = USER_TASK_REBALANCE_NO_GOALS + SEP + RESPONSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID = USER_TASK_REBALANCE_NO_GOALS_VERBOSE_UTID + SEP + RESPONSE;
    public static final String REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR = REBALANCE + SEP + "error";
    public static final String REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR_RESPONSE_UTID = REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR + SEP + RESPONSE;
    public static final String BROKERS_NOT_EXIST_ERROR = REBALANCE + SEP + "error";
    public static final String BROKERS_NOT_EXIST_ERROR_RESPONSE_UTID = BROKERS_NOT_EXIST_ERROR + SEP + RESPONSE;
    public static final String STATE_PROPOSAL_NOT_READY = STATE + SEP + "proposal" + SEP + "not" + SEP + "ready";
    public static final String STATE_PROPOSAL_NOT_READY_RESPONSE = STATE_PROPOSAL_NOT_READY + SEP + RESPONSE;

    private static final String CLUSTER = "my-cluster";
    private static final String NAMESPACE = "my-project";

    public static final Secret CC_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName(CruiseControlResources.secretName(CLUSTER))
                .withNamespace(NAMESPACE)
            .endMetadata()
            .addToData("cruise-control.crt", MockCertManager.clusterCaCert())
            .build();

    private static Map<String, String> apiSecretData = Map.of(CruiseControlApiProperties.REBALANCE_OPERATOR_PASSWORD_KEY, "password");
    public static final Secret CC_API_SECRET = ModelUtils.createSecret(CruiseControlResources.apiSecretName(CLUSTER), NAMESPACE, Labels.EMPTY, null,
           apiSecretData, Collections.emptyMap(), Collections.emptyMap());
    private static final Header AUTH_HEADER = convertToHeader(CruiseControlApiImpl.getAuthHttpHeader(true, CC_API_SECRET));

    private ClientAndServer server;

    /**
     * Sets up and returns a Cruise Control mock server.
     *
     * @param serverPort   The port number the server should listen on.
     * @param tlsKeyFile   File containing the CA key.
     * @param tlsCrtFile   File containing the CA crt.
     * @return             The mock CruiseControl instance.
     */
    public MockCruiseControl(int serverPort, File tlsKeyFile, File tlsCrtFile) {
        try {
            ConfigurationProperties.logLevel("WARN");
            ConfigurationProperties.certificateAuthorityPrivateKey(tlsKeyFile.getAbsolutePath());
            ConfigurationProperties.certificateAuthorityCertificate(tlsCrtFile.getAbsolutePath());

            String loggingConfiguration = "handlers=org.mockserver.logging.StandardOutConsoleHandler\n" +
                "org.mockserver.logging.StandardOutConsoleHandler.level=WARNING\n" +
                "org.mockserver.logging.StandardOutConsoleHandler.formatter=java.util.logging.SimpleFormatter\n" +
                "java.util.logging.SimpleFormatter.format=%1$tF %1$tT  %3$s  %4$s  %5$s %6$s%n\n" +
                ".level=" + ConfigurationProperties.javaLoggerLogLevel() + "\n" +
                "io.netty.handler.ssl.SslHandler.level=WARNING";
            LogManager.getLogManager().readConfiguration(new ByteArrayInputStream(loggingConfiguration.getBytes(UTF_8)));

            this.server = new ClientAndServer(serverPort);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        server.reset();
    }

    public void stop() {
        server.stop();
    }

    public boolean isRunning() {
        return server.isRunning();
    }

    private static HttpRequest requestMatcher(List<Header> headers, CruiseControlEndpoints endpoint, String json, String verbose) {
        HttpRequest request = new HttpRequest()
                .withMethod("GET")
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), json))
                .withPath(endpoint.toString())
                .withHeaders(headers)
                .withSecure(true);

        switch (endpoint) {
            case USER_TASKS:
                boolean isVerbose = Boolean.parseBoolean(verbose);
                String userTaskId = isVerbose
                        ? REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID
                        : REBALANCE_NO_GOALS_RESPONSE_UTID;

                request.withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.toString(), "true"))
                        .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.toString(), userTaskId));
                break;
            case STATE:
                request.withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.toString(), verbose));
                break;
        }
        return request;
    }

    private HttpResponse buildStateResponse(String jsonFileName, String userTaskId, int statusCode) {
        JsonBody jsonBody = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + jsonFileName));
        return response()
                .withStatusCode(statusCode)
                .withBody(jsonBody)
                .withHeaders(header(USER_TASK_ID_HEADER, userTaskId))
                .withDelay(TimeUnit.SECONDS, 0);
    }

    /**
     * Sets up mocked response of the Cruise Control "State" endpoint.
     *
     * <p>
     * This method configures the MockServer to return specific responses based on the provided task status
     * and fetch error flag.
     *
     * @param taskStatus The current {@link CruiseControlUserTaskStatus} of the simulated Cruise Control task.
     *                   This determines whether the state response should reflect no task (when "null"), an
     *                   active proposal generation task, an ongoing executing task, or a completed task.
     * @param fetchError If {@code true}, the mock will return a 500 error response to simulate a fetch failure
     *                   from the Cruise Control "State" endpoint, regardless of task status.
     */
    public void mockStateEndpoint(CruiseControlUserTaskStatus taskStatus, boolean fetchError) {
        List<Header> authHeader = List.of(AUTH_HEADER);
        List<Header> proposalNotReadyHeaders = List.of(header(USER_TASK_ID_HEADER, STATE_PROPOSAL_NOT_READY), authHeader.get(0));

        if (fetchError) {
            server.when(requestMatcher(authHeader, CruiseControlEndpoints.STATE, "true|false", "true|false"))
                    .respond(buildStateResponse("CC-State-fetch-error.json", "cruise-control-state-error", 500));
            return;
        }

        if (taskStatus == null) {
            server.when(requestMatcher(authHeader, CruiseControlEndpoints.STATE, "true", "false"))
                    .respond(buildStateResponse("CC-State.json", "cruise-control-state", 200));
            return;
        }

        switch (taskStatus) {
            case ACTIVE:
                server.when(requestMatcher(proposalNotReadyHeaders, CruiseControlEndpoints.STATE, "true|false", "true|false"))
                        .respond(buildStateResponse("CC-State-proposal-not-ready.json", STATE_PROPOSAL_NOT_READY_RESPONSE, 200));
                return;

            case IN_EXECUTION:
                server.when(requestMatcher(authHeader, CruiseControlEndpoints.STATE, "true|false", "true|false"))
                        .respond(buildStateResponse("CC-State-inExecution.json", "cruise-control-state", 200));
                return;

            default:
                server.when(requestMatcher(authHeader, CruiseControlEndpoints.STATE, "true", "false"))
                        .respond(buildStateResponse("CC-State.json", "cruise-control-state", 200));
        }
    }

    private HttpResponse userTaskResponse(CruiseControlUserTaskStatus state, boolean verbose) {
        String fileName = "CC-User-task-rebalance-no-goals-" +
                (verbose ? "verbose-" : "") + state.toString() + ".json";

        String userTaskId = verbose
                ? USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID
                : USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID;

        JsonBody jsonBody = new JsonBody(ReadWriteUtils.readFileFromResources(
                getClass(), "/" + CC_JSON_ROOT + fileName));

        return response()
                .withBody(jsonBody)
                .withStatusCode(200)
                .withHeaders(header("User-Task-ID", userTaskId))
                .withDelay(TimeUnit.SECONDS, 0);
    }

    /**
     * Sets up mocked response of the Cruise Control "User Tasks" endpoint based on the provided task status.
     *
     * @param taskStatus The current {@link CruiseControlUserTaskStatus} of the simulated Cruise Control task.
     *                   This determines whether the state response should reflect no task (when "null"), an
     *                   active proposal generation task, an ongoing executing task, or a completed task.
     */
    private void mockUserTasksEndpoint(CruiseControlUserTaskStatus taskStatus) {
        List<Header> authHeader = List.of(AUTH_HEADER);
        for (boolean verbose : List.of(false, true)) {
            HttpRequest request = requestMatcher(authHeader, CruiseControlEndpoints.USER_TASKS, "true", String.valueOf(verbose));

            if (taskStatus == null) {
                server.when(request)
                        .respond(userTaskResponse(COMPLETED, verbose)); // or some other sensible fallback
                continue;
            }

            switch (taskStatus) {
                case ACTIVE:
                    server.when(request)
                            .respond(userTaskResponse(ACTIVE, verbose));
                    break;
                case IN_EXECUTION:
                    server.when(request)
                            .respond(userTaskResponse(IN_EXECUTION, verbose));
                    break;
                case COMPLETED_WITH_ERROR:
                    server.when(request)
                            .respond(userTaskResponse(COMPLETED_WITH_ERROR, false));
                    break;
                case COMPLETED:
                    server.when(request)
                            .respond(userTaskResponse(COMPLETED, verbose));
                    break;
            }
        }
    }

    private HttpRequest rebalanceRequestMatcher(Boolean verbose) {
        HttpRequest request = request();

        if (verbose) {
            request.withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.toString(), String.valueOf(verbose)));
        }

        return request
                .withMethod("POST")
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "true|false"))
                .withQueryStringParameter(buildBrokerParameters(CruiseControlEndpoints.REBALANCE))
                .withPath(CruiseControlEndpoints.REBALANCE.toString())
                .withHeader(AUTH_HEADER)
                .withSecure(true);
    }

    private HttpResponse rebalanceResponse(CruiseControlUserTaskStatus taskStatus, boolean verbose) {
        String fileName;
        String userTaskId;
        HttpResponse response = response();

        if (taskStatus == ACTIVE) {
            fileName = "CC-Rebalance-no-goals-in-progress.json";
            userTaskId = REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID;
            response.withStatusCode(202);
        } else {
            if (verbose) {
                // Rebalance response with no goals set - verbose
                fileName = "CC-Rebalance-no-goals-verbose.json";
                userTaskId = REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID;
            } else {
                // Rebalance response with no goals set - non-verbose parameter
                fileName = "CC-Rebalance-no-goals.json";
                userTaskId = REBALANCE_NO_GOALS_RESPONSE_UTID;
            }
        }
        JsonBody body = new JsonBody(ReadWriteUtils.readFileFromResources(
                getClass(), "/" + CC_JSON_ROOT + fileName));

        return response
                .withBody(body)
                .withHeaders(header("User-Task-ID", userTaskId))
                .withDelay(TimeUnit.SECONDS, 0);
    }

    /**
     * Sets up mocked response of the Cruise Control "Rebalance" endpoint based on the provided task status.
     *
     * @param taskStatus The current {@link CruiseControlUserTaskStatus} of the simulated Cruise Control task.
     *                   This determines whether the state response should reflect no task (when "null"), an
     *                   active proposal generation task, an ongoing executing task, or a completed task.
     */
    public void mockRebalanceEndpoint(CruiseControlUserTaskStatus taskStatus) {
        for (boolean verbose : List.of(false, true)) {
            server.when(rebalanceRequestMatcher(verbose))
                    .respond(rebalanceResponse(taskStatus, verbose));

        }
    }

    /**
     * Sets up mocked responses from Cruise Control REST API endpoint based on the provided task status
     * and fetch error flag.
     *
     * @param taskStatus The current {@link CruiseControlUserTaskStatus} of the simulated Cruise Control task.
     *                   This determines whether the state response should reflect no task (when "null"), an
     *                   active proposal generation task, an ongoing executing task, or a completed task.
     * @param stateEndpointFetchError If {@code true}, the mock will return a 500 error response to simulate a fetch failure
     *                   from the Cruise Control "State" endpoint, regardless of task status.
     */
    public void mockTask(CruiseControlUserTaskStatus taskStatus, boolean stateEndpointFetchError) {
        server.clear(request());
        mockUserTasksEndpoint(taskStatus);
        mockStateEndpoint(taskStatus, stateEndpointFetchError);
        mockRebalanceEndpoint(taskStatus);
    }

    /**
     * Setup NotEnoughValidWindows error rebalance/add/remove broker response.
     */
    public void setupCCRebalanceNotEnoughDataError(CruiseControlEndpoints endpoint, String verbose) {
        // Rebalance response with no goal that returns an error
        JsonBody jsonError = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Rebalance-NotEnoughValidWindows-error.json"));
        HttpRequest request = request();

        if (verbose != null) {
            request.withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.toString(), verbose));
        }

        request
                .withMethod("POST")
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "true|false"))
                .withQueryStringParameter(buildBrokerParameters(endpoint))
                .withPath(endpoint.toString())
                .withHeader(AUTH_HEADER)
                .withSecure(true);

        server
                .when(request)
                .respond(
                        response()
                                .withStatusCode(500)
                                .withBody(jsonError)
                                .withHeaders(header(USER_TASK_ID_HEADER, REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, 0));
    }

    /**
     * Setup broker doesn't exist error for add/remove broker response.
     */
    public void setupCCBrokerDoesNotExist(CruiseControlEndpoints endpoint, String verbose) {
        // Add/remove broker response with no goal that returns an error
        JsonBody jsonError = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Broker-not-exist.json"));
        HttpRequest request = request();

        if (verbose != null) {
            request.withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.toString(), verbose));
        }

        request
                .withMethod("POST")
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "true|false"))
                .withQueryStringParameter(buildBrokerParameters(endpoint))
                .withPath(endpoint.toString())
                .withHeader(AUTH_HEADER)
                .withSecure(true);

        server
                .when(request)
                .respond(
                        response()
                                .withStatusCode(500)
                                .withBody(jsonError)
                                .withHeaders(header(USER_TASK_ID_HEADER, BROKERS_NOT_EXIST_ERROR_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, 0));
    }

    /**
     * Setup rebalance response with no response delay (for quicker tests).
     */
    public void setupCCRebalanceResponse(int pendingCalls, CruiseControlEndpoints endpoint, String verbose) throws IOException, URISyntaxException {
        setupCCRebalanceResponse(pendingCalls, 0, endpoint, verbose);
    }

    /**
     * Setup rebalance response.
     */
    public void setupCCRebalanceResponse(int pendingCalls, int responseDelay, CruiseControlEndpoints endpoint, String verbose) {

        HttpRequest request = request();
        JsonBody json = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Rebalance-no-goals-in-progress.json"));

        if (verbose != null) {
            request.withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.toString(), verbose));
        }

        request
                .withMethod("POST")
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "true|false"))
                .withQueryStringParameter(buildBrokerParameters(endpoint))
                .withPath(endpoint.toString())
                .withHeader(AUTH_HEADER)
                .withSecure(true);

        server
                .when(request,
                        Times.exactly(pendingCalls))
                .respond(
                        response()
                                .withBody(json)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withStatusCode(202)
                                .withDelay(TimeUnit.SECONDS, responseDelay));

        HttpResponse response = response();

        if (verbose == null) {
            json = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Rebalance-no-goals.json"));
            response.withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID));
        } else if (verbose.equals("true")) {
            // Rebalance response with no goals set - verbose
            json = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Rebalance-no-goals-verbose.json"));
            response.withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
        } else if (verbose.equals("false") || verbose.equals("true|false")) {
            // Rebalance response with no goals set - non-verbose parameter
            json = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Rebalance-no-goals.json"));
            response.withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID));

        }

        response
                .withBody(json)
                .withDelay(TimeUnit.SECONDS, responseDelay);

        server
                .when(request, Times.unlimited())
                .respond(response);
    }

    /**
     * Setup responses for various bad goal configurations possible on a rebalance request.
     */
    public void setupCCRebalanceBadGoalsError(CruiseControlEndpoints endpoint) {
        // Response if the user has set custom goals which do not include all configured hard.goals
        JsonBody jsonError = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Rebalance-bad-goals-error.json"));


        server
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.toString(), "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.GOALS.toString(), ".+"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.toString(), "false"))
                                .withPath(endpoint.toString())
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonError)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withStatusCode(500)
                                .withDelay(TimeUnit.SECONDS, 0));

        // Response if the user has set custom goals which do not include all configured hard.goals
        // Note: This uses the no-goals example response but the difference between custom goals and default goals is not tested here
        JsonBody jsonSummary = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Rebalance-no-goals-verbose.json"));

        server
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.toString(), "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.GOALS.toString(), ".+"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.toString(), "true"))
                                .withPath(endpoint.toString())
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonSummary)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withStatusCode(200)
                                .withDelay(TimeUnit.SECONDS, 0));
    }

    /**
     * Sets up the User Tasks endpoint. These endpoints expect the query to contain the user-task-id returned in the header of the response from
     * the rebalance endpoints.
     *
     * @param activeCalls The number of calls to the User Tasks endpoint that should return "Active" before "inExecution" is returned as the status.
     * @param inExecutionCalls The number of calls to the User Tasks endpoint that should return "InExecution" before "Completed" is returned as the status.
     * @throws IOException If there are issues connecting to the network port.
     * @throws URISyntaxException If any of the configured end points are invalid.
     */
    public void setupCCUserTasksResponseNoGoals(int activeCalls, int inExecutionCalls) throws IOException, URISyntaxException {
        List<Header> authHeader = List.of(AUTH_HEADER);
        for (boolean verbose : List.of(false, true)) {
            HttpRequest request = requestMatcher(authHeader, CruiseControlEndpoints.USER_TASKS, "true", String.valueOf(verbose));

            // The first activeCalls times respond that with a status of "Active"
            server.when(request, Times.exactly(activeCalls))
                    .respond(userTaskResponse(ACTIVE, verbose));

            // The next inExecutionCalls times respond that with a status of "InExecution"
            server.when(request, Times.exactly(inExecutionCalls))
                    .respond(userTaskResponse(IN_EXECUTION, verbose));

            // On the N+1 call, respond with a status of "Completed".
            server.when(request, Times.unlimited())
                    .respond(userTaskResponse(COMPLETED, verbose));

        }
    }

    /**
     * Setup response of task completed with error.
     */
    public void setupCCUserTasksCompletedWithError() throws IOException, URISyntaxException {
        // This simulates asking for the status of a task that has Complete with error and fetch_completed_task=true
        JsonBody compWithErrorJson = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-User-task-rebalance-no-goals-CompletedWithError.json"));

        server
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.toString(), "true"))
                                .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(compWithErrorJson)
                                .withStatusCode(200)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, 0));
    }

    public void setupCCUserTasksToReturnError(int statusCode, String errorMessage) throws IOException, URISyntaxException {
        // This simulates asking for the status of a task that has Complete with error and fetch_completed_task=true
        JsonBody errorJson = new JsonBody("{\"errorMessage\":\"" + errorMessage + "\"}");
        server
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.toString(), "true"))
                                .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(errorJson)
                                .withStatusCode(statusCode)
                                .withDelay(TimeUnit.SECONDS, 0));
    }

    /**
     * Setup response when user task is not found
     */
    public void setupUserTasktoEmpty() {
        // This simulates asking for the status with empty user task
        JsonBody jsonEmptyUserTask = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-User-task-status-empty.json"));

        server
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.toString(), "true"))
                                .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonEmptyUserTask)
                                .withStatusCode(200)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, 0));
    }

    /**
     * Setup response of task being stopped.
     */
    public void setupCCStopResponse() {
        JsonBody jsonStop = new JsonBody(ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + "CC-Stop.json"));

        server
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true|false"))
                                .withPath(CruiseControlEndpoints.STOP.toString())
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonStop)
                                .withHeaders(header("User-Task-ID", "stopped"))
                                .withDelay(TimeUnit.SECONDS, 0));

    }

    private Parameter buildBrokerParameters(CruiseControlEndpoints endpoint) {
        if (CruiseControlEndpoints.ADD_BROKER.equals(endpoint) || CruiseControlEndpoints.REMOVE_BROKER.equals(endpoint)) {
            return Parameter.param(CruiseControlParameters.BROKER_ID.toString(), "[0-9]+(,[0-9]+)*");
        } else if (CruiseControlEndpoints.REMOVE_DISKS.equals(endpoint)) {
            return Parameter.param(CruiseControlParameters.BROKER_ID_AND_LOG_DIRS.toString(), "[0-9]+-/var/lib/kafka/data-[0-9]+/kafka-log[0-9]+(,[0-9]+-/var/lib/kafka/data-[0-9]+/kafka-log[0-9]+)*");
        } else {
            return null;
        }
    }

    private static Header convertToHeader(HTTPHeader httpHeader) {
        if (httpHeader == null) {
            return null;
        } else {
            return new Header(httpHeader.getName(), httpHeader.getValue());
        }
    }
}
