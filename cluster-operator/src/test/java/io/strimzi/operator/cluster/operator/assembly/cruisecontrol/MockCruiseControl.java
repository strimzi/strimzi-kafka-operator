/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly.cruisecontrol;

import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.Parameter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Header.header;

public class MockCruiseControl {

    private static final int RESPONSE_DELAY_SEC = 2;

    private static final String SEP =  "-";
    private static final String REBALANCE =  "rebalance";
    private static final String NO_GOALS =  "no-goals";
    private static final String VERBOSE =  "verbose";
    private static final String USER_TASK =  "user-task";
    private static final String RESPONSE = "response";

    public static final String REBALANCE_NO_GOALS_RESPONSE_UTID = REBALANCE + SEP + NO_GOALS + SEP + RESPONSE;
    public static final String REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID = REBALANCE + SEP + NO_GOALS + SEP + VERBOSE + SEP + RESPONSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS = USER_TASK + SEP + REBALANCE + SEP + NO_GOALS;
    public static final String USER_TASK_REBALANCE_NO_GOALS_VERBOSE_UTID = USER_TASK_REBALANCE_NO_GOALS + SEP + VERBOSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID = USER_TASK_REBALANCE_NO_GOALS + SEP + RESPONSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID = USER_TASK_REBALANCE_NO_GOALS_VERBOSE_UTID + SEP + RESPONSE;

    /**
     * Sets up and returns the Cruise Control MockSever
     * @param port The port number the MockServer instance should listen on
     * @param pendingCalls The number of calls to the User Tasks endpoint that should return "InExecution" before "Completed" is returned as the status.
     * @return The configured ClientAndServer instance.
     * @throws IOException If there are issues connecting to the network port.
     * @throws URISyntaxException If any of the configured end points are invalid.
     */
    public static ClientAndServer getCCServer(int port, int pendingCalls) throws IOException, URISyntaxException {
        ClientAndServer ccServer = new ClientAndServer(port);
        setupTestResponse(ccServer);
        setupCCStateResponse(ccServer);
        setupCCRebalanceResponse(ccServer);
        setupCCUserTasksResponse(ccServer, pendingCalls);
        return ccServer;
    }

    private static String getJsonFromResource(String resource) throws URISyntaxException, IOException {

        URI jsonURI = Objects.requireNonNull(MockCruiseControl.class.getClassLoader().getResource(resource)).toURI();

        Optional<String> json = Files.lines(Paths.get(jsonURI), StandardCharsets.UTF_8).reduce((x, y) -> x + y);

        if (json.isPresent()) {
            return json.get();
        } else {
            throw new IOException("File " + resource + " from resources was empty");
        }

    }

    private static void setupTestResponse(ClientAndServer ccServer) {
        ccServer.when(request().withMethod("GET").withPath("/test"))
                .respond(response().withBody("Was this what you wanted?"));
    }

    private static void setupCCStateResponse(ClientAndServer ccServer) throws IOException, URISyntaxException {

        // Non-verbose response
        String json = getJsonFromResource("CC-State.json");

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withPath(CruiseControlEndpoints.STATE.path))
                .respond(
                        response()
                                .withBody(json)
                                .withHeaders(header("User-Task-ID", "cruise-control-state"))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        // Verbose response
        String jsonVerbose = getJsonFromResource("CC-State-verbose.json");

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true"))
                                .withPath(CruiseControlEndpoints.STATE.path))
                .respond(
                        response()
                                .withBody(jsonVerbose)
                                .withHeaders(header("User-Task-ID", "cruise-control-state-verbose"))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

    }

    private static void setupCCRebalanceResponse(ClientAndServer ccServer) throws IOException, URISyntaxException {

        // Rebalance response with no goals set - non-verbose
        String json = getJsonFromResource("CC-Rebalance-no-goals.json");

        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "false"))
                                .withPath(CruiseControlEndpoints.REBALANCE.path))
                .respond(
                        response()
                                .withBody(json)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        // Rebalance response with no goals set - verbose
        String jsonVerbose = getJsonFromResource("CC-Rebalance-no-goals-verbose.json");

        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true"))
                                .withPath(CruiseControlEndpoints.REBALANCE.path))
                .respond(
                        response()
                                .withBody(jsonVerbose)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

    }

    /**
     * Sets up the User Tasks endpoint. These endpoints expect the query to contain the user-task-id returned in the header of the response from
     * the rebalance endpoints.
     * @param ccServer The ClientAndServer instance that this endpoint will be added too.
     * @param pendingCalls The number of calls to the User Tasks endpoint that should return "InExecution" before "Completed" is returned as the status.
     * @throws IOException If there are issues connecting to the network port.
     * @throws URISyntaxException If any of the configured end points are invalid.
     */
    private static void setupCCUserTasksResponse(ClientAndServer ccServer, int pendingCalls) throws IOException, URISyntaxException {

        // User tasks response for the rebalance request with no goals set (non-verbose)
        String jsonInExecution = getJsonFromResource("CC-User-task-rebalance-no-goals-inExecution.json");
        String jsonCompleted = getJsonFromResource("CC-User-task-rebalance-no-goals-completed.json");

        // The first N time respond that with a status of "InExecution"
        if (pendingCalls > 0) {
            ccServer
                    .when(
                            request()
                                    .withMethod("GET")
                                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_RESPONSE_UTID))
                                    .withPath(CruiseControlEndpoints.USER_TASKS.path),
                            Times.exactly(pendingCalls))
                    .respond(
                            response()
                                    .withBody(jsonInExecution)
                                    .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID))
                                    .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));
        }

        // On the N+1 call respond with a completed rebalance task.
        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path),
                        Times.unlimited())
                .respond(
                        response()
                                .withBody(jsonCompleted)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        // User tasks response for the rebalance request with no goals set (verbose)
        String jsonInExecutionVerbose = getJsonFromResource("CC-User-task-rebalance-no-goals-verbose-inExecution.json");
        String jsonCompletedVerbose = getJsonFromResource("CC-User-task-rebalance-no-goals-verbose-completed.json");

        if (pendingCalls > 0) {
            ccServer
                    .when(
                            request()
                                    .withMethod("GET")
                                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                    .withQueryStringParameter(
                                            Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                    .withPath(CruiseControlEndpoints.USER_TASKS.path),
                            Times.exactly(pendingCalls))
                    .respond(
                            response()
                                    .withBody(jsonInExecutionVerbose)
                                    .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                    .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));
        }

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withQueryStringParameter(
                                        Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path),
                        Times.unlimited())
                .respond(
                        response()
                                .withBody(jsonCompletedVerbose)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));
    }
}
