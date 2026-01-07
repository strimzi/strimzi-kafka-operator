/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.ReadWriteUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlHeaders.USER_TASK_ID_HEADER;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.ACTIVE;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.COMPLETED;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.COMPLETED_WITH_ERROR;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.IN_EXECUTION;

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

    // Scenario names for stateful mocking
    private static final String USER_TASKS_SCENARIO = "user-tasks-scenario";
    private static final String USER_TASKS_VERBOSE_SCENARIO = "user-tasks-verbose-scenario";

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
    private static final String AUTH_HEADER_VALUE = CruiseControlApiImpl.getAuthHttpHeader(true, CC_API_SECRET).getValue();

    private static final String KEYSTORE_PASSWORD = "changeit";
    private final WireMockServer server;

    /**
     * Sets up and returns a Cruise Control mock server.
     *
     * @param serverPort   The port number the server should listen on.
     * @param tlsKeyFile   File containing the private key in PEM format.
     * @param tlsCrtFile   File containing the certificate in PEM format.
     */
    public MockCruiseControl(int serverPort, File tlsKeyFile, File tlsCrtFile) {
        try {
            // Create a PKCS12 keystore from PEM files for WireMock
            File keystoreFile = createKeystoreFromPem(tlsKeyFile, tlsCrtFile);

            WireMockConfiguration config = WireMockConfiguration.options()
                    .httpsPort(serverPort)
                    .keystorePath(keystoreFile.getAbsolutePath())
                    .keystorePassword(KEYSTORE_PASSWORD)
                    .keyManagerPassword(KEYSTORE_PASSWORD)
                    .keystoreType("PKCS12");

            this.server = new WireMockServer(config);
            this.server.start();
            WireMock.configureFor("https", "localhost", serverPort);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a PKCS12 keystore with a server certificate that includes localhost as a SAN.
     * The certificate is signed by the CA from the provided PEM files.
     */
    private static File createKeystoreFromPem(File caKeyFile, File caCertFile)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InvalidKeySpecException {
        OpenSslCertManager certManager = new OpenSslCertManager();

        // Create subject with localhost SANs
        Subject subject = new Subject.Builder()
                .withCommonName("localhost")
                .addDnsName("localhost")
                .addIpAddress("127.0.0.1")
                .build();

        // Create temp files for the server key, CSR, and certificate
        File serverKeyFile = Files.createTempFile("server", ".key").toFile();
        File csrFile = Files.createTempFile("server", ".csr").toFile();
        File serverCertFile = Files.createTempFile("server", ".crt").toFile();
        File keystoreFile = Files.createTempFile("wiremock-keystore", ".p12").toFile();

        serverKeyFile.deleteOnExit();
        csrFile.deleteOnExit();
        serverCertFile.deleteOnExit();
        keystoreFile.deleteOnExit();

        try {
            // Generate CSR with the server key
            certManager.generateCsr(serverKeyFile, csrFile, subject);

            // Sign the CSR with the CA to create the server certificate
            certManager.generateCert(csrFile, caKeyFile, caCertFile, serverCertFile, subject, 365);

            // Create PKCS12 keystore with the server key and certificate
            certManager.addKeyAndCertToKeyStore(serverKeyFile, serverCertFile, "server", keystoreFile, KEYSTORE_PASSWORD);

            return keystoreFile;
        } finally {
            // Clean up temp files (except keystore which is returned)
            Files.deleteIfExists(serverKeyFile.toPath());
            Files.deleteIfExists(csrFile.toPath());
            Files.deleteIfExists(serverCertFile.toPath());
        }
    }

    public void reset() {
        server.resetAll();
        server.resetScenarios();
    }

    public void stop() {
        server.stop();
    }

    public boolean isRunning() {
        return server.isRunning();
    }

    private String readJsonResource(String fileName) {
        return ReadWriteUtils.readFileFromResources(getClass(), "/" + CC_JSON_ROOT + fileName);
    }

    private MappingBuilder stateRequestMatcher(boolean requireAuth) {
        MappingBuilder builder = get(urlPathEqualTo(CruiseControlEndpoints.STATE.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), matching("true|false"));

        if (requireAuth) {
            builder.withHeader("Authorization", equalTo(AUTH_HEADER_VALUE));
        }
        return builder;
    }

    /**
     * Sets up mocked response of the Cruise Control "State" endpoint.
     */
    public void mockStateEndpoint(CruiseControlUserTaskStatus taskStatus, boolean fetchError) {
        if (fetchError) {
            server.stubFor(stateRequestMatcher(true)
                    .willReturn(aResponse()
                            .withStatus(500)
                            .withHeader(USER_TASK_ID_HEADER, "cruise-control-state-error")
                            .withBody(readJsonResource("CC-State-fetch-error.json"))));
            return;
        }

        if (taskStatus == null) {
            server.stubFor(stateRequestMatcher(true)
                    .withQueryParam(CruiseControlParameters.VERBOSE.toString(), equalTo("false"))
                    .willReturn(aResponse()
                            .withStatus(200)
                            .withHeader(USER_TASK_ID_HEADER, "cruise-control-state")
                            .withBody(readJsonResource("CC-State.json"))));
            return;
        }

        switch (taskStatus) {
            case ACTIVE:
                server.stubFor(stateRequestMatcher(true)
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader(USER_TASK_ID_HEADER, STATE_PROPOSAL_NOT_READY_RESPONSE)
                                .withBody(readJsonResource("CC-State-proposal-not-ready.json"))));
                return;

            case IN_EXECUTION:
                server.stubFor(stateRequestMatcher(true)
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader(USER_TASK_ID_HEADER, "cruise-control-state")
                                .withBody(readJsonResource("CC-State-inExecution.json"))));
                return;

            default:
                server.stubFor(stateRequestMatcher(true)
                        .withQueryParam(CruiseControlParameters.VERBOSE.toString(), equalTo("false"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader(USER_TASK_ID_HEADER, "cruise-control-state")
                                .withBody(readJsonResource("CC-State.json"))));
        }
    }

    private String userTaskResponseJson(CruiseControlUserTaskStatus state, boolean verbose) {
        String fileName = "CC-User-task-rebalance-no-goals-" +
                (verbose ? "verbose-" : "") + state.toString() + ".json";
        return readJsonResource(fileName);
    }

    private MappingBuilder userTasksRequestMatcher(boolean verbose) {
        String userTaskId = verbose
                ? REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID
                : REBALANCE_NO_GOALS_RESPONSE_UTID;

        return get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withQueryParam(CruiseControlParameters.FETCH_COMPLETE.toString(), equalTo("true"))
                .withQueryParam(CruiseControlParameters.USER_TASK_IDS.toString(), equalTo(userTaskId))
                .withHeader("Authorization", equalTo(AUTH_HEADER_VALUE));
    }

    /**
     * Sets up mocked response of the Cruise Control "User Tasks" endpoint based on the provided task status.
     */
    private void mockUserTasksEndpoint(CruiseControlUserTaskStatus taskStatus) {
        for (boolean verbose : new boolean[]{false, true}) {
            MappingBuilder request = userTasksRequestMatcher(verbose);
            String userTaskId = verbose
                    ? USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID
                    : USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID;

            CruiseControlUserTaskStatus effectiveStatus = taskStatus != null ? taskStatus : COMPLETED;
            boolean effectiveVerbose = (effectiveStatus == COMPLETED_WITH_ERROR) ? false : verbose;

            server.stubFor(request
                    .willReturn(aResponse()
                            .withStatus(200)
                            .withHeader("User-Task-ID", userTaskId)
                            .withBody(userTaskResponseJson(effectiveStatus, effectiveVerbose))));
        }
    }

    private MappingBuilder rebalanceRequestMatcher(Boolean verbose, CruiseControlEndpoints endpoint) {
        MappingBuilder request = post(urlPathEqualTo(endpoint.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), matching("true|false"))
                .withHeader("Authorization", equalTo(AUTH_HEADER_VALUE));

        if (verbose != null) {
            request.withQueryParam(CruiseControlParameters.VERBOSE.toString(), equalTo(String.valueOf(verbose)));
        }

        // Add broker parameters for add/remove broker endpoints
        if (CruiseControlEndpoints.ADD_BROKER.equals(endpoint) || CruiseControlEndpoints.REMOVE_BROKER.equals(endpoint)) {
            request.withQueryParam(CruiseControlParameters.BROKER_ID.toString(), matching("[0-9]+(,[0-9]+)*"));
        } else if (CruiseControlEndpoints.REMOVE_DISKS.equals(endpoint)) {
            request.withQueryParam(CruiseControlParameters.BROKER_ID_AND_LOG_DIRS.toString(),
                    matching("[0-9]+-/var/lib/kafka/data-[0-9]+/kafka-log[0-9]+(,[0-9]+-/var/lib/kafka/data-[0-9]+/kafka-log[0-9]+)*"));
        }

        return request;
    }

    /**
     * Sets up mocked response of the Cruise Control "Rebalance" endpoint based on the provided task status.
     */
    public void mockRebalanceEndpoint(CruiseControlUserTaskStatus taskStatus) {
        for (boolean verbose : new boolean[]{false, true}) {
            String fileName;
            String userTaskId;
            int statusCode = 200;

            if (taskStatus == ACTIVE) {
                fileName = "CC-Rebalance-no-goals-in-progress.json";
                userTaskId = REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID;
                statusCode = 202;
            } else if (verbose) {
                fileName = "CC-Rebalance-no-goals-verbose.json";
                userTaskId = REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID;
            } else {
                fileName = "CC-Rebalance-no-goals.json";
                userTaskId = REBALANCE_NO_GOALS_RESPONSE_UTID;
            }

            server.stubFor(rebalanceRequestMatcher(verbose, CruiseControlEndpoints.REBALANCE)
                    .willReturn(aResponse()
                            .withStatus(statusCode)
                            .withHeader("User-Task-ID", userTaskId)
                            .withBody(readJsonResource(fileName))));
        }
    }

    /**
     * Sets up mocked responses from Cruise Control REST API endpoint based on the provided task status
     * and fetch error flag.
     */
    public void mockTask(CruiseControlUserTaskStatus taskStatus, boolean stateEndpointFetchError) {
        server.resetAll();
        mockUserTasksEndpoint(taskStatus);
        mockStateEndpoint(taskStatus, stateEndpointFetchError);
        mockRebalanceEndpoint(taskStatus);
    }

    /**
     * Setup NotEnoughValidWindows error rebalance/add/remove broker response.
     */
    public void setupCCRebalanceNotEnoughDataError(CruiseControlEndpoints endpoint, String verbose) {
        String jsonError = readJsonResource("CC-Rebalance-NotEnoughValidWindows-error.json");

        MappingBuilder request = rebalanceRequestMatcher(verbose != null ? Boolean.parseBoolean(verbose) : null, endpoint);

        server.stubFor(request
                .willReturn(aResponse()
                        .withStatus(500)
                        .withHeader(USER_TASK_ID_HEADER, REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR_RESPONSE_UTID)
                        .withBody(jsonError)));
    }

    /**
     * Setup broker doesn't exist error for add/remove broker response.
     */
    public void setupCCBrokerDoesNotExist(CruiseControlEndpoints endpoint, String verbose) {
        String jsonError = readJsonResource("CC-Broker-not-exist.json");

        MappingBuilder request = rebalanceRequestMatcher(verbose != null ? Boolean.parseBoolean(verbose) : null, endpoint);

        server.stubFor(request
                .willReturn(aResponse()
                        .withStatus(500)
                        .withHeader(USER_TASK_ID_HEADER, BROKERS_NOT_EXIST_ERROR_RESPONSE_UTID)
                        .withBody(jsonError)));
    }

    /**
     * Setup rebalance response with no response delay (for quicker tests).
     */
    public void setupCCRebalanceResponse(int pendingCalls, CruiseControlEndpoints endpoint, String verbose) {
        setupCCRebalanceResponse(pendingCalls, 0, endpoint, verbose);
    }

    /**
     * Setup rebalance response using WireMock scenarios for stateful behavior.
     */
    public void setupCCRebalanceResponse(int pendingCalls, int responseDelay, CruiseControlEndpoints endpoint, String verbose) {
        String inProgressJson = readJsonResource("CC-Rebalance-no-goals-in-progress.json");
        String scenarioName = "rebalance-scenario-" + endpoint.toString();

        Boolean verboseParam = verbose != null ? Boolean.parseBoolean(verbose) : null;
        if (verbose != null && verbose.equals("true|false")) {
            verboseParam = null; // Match any
        }

        MappingBuilder baseRequest = rebalanceRequestMatcher(verboseParam, endpoint);

        // Setup pending calls (return 202 with in-progress)
        for (int i = 0; i < pendingCalls; i++) {
            String currentState = i == 0 ? Scenario.STARTED : "pending-" + (i - 1);
            String nextState = "pending-" + i;

            server.stubFor(baseRequest
                    .withId(UUID.randomUUID())
                    .inScenario(scenarioName)
                    .whenScenarioStateIs(currentState)
                    .willSetStateTo(nextState)
                    .willReturn(aResponse()
                            .withStatus(202)
                            .withHeader("User-Task-ID", REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID)
                            .withFixedDelay(responseDelay * 1000)
                            .withBody(inProgressJson)));
        }

        // Setup final response (return 200 with completed)
        String finalJson;
        String userTaskId;
        if (verbose == null) {
            finalJson = readJsonResource("CC-Rebalance-no-goals.json");
            userTaskId = REBALANCE_NO_GOALS_RESPONSE_UTID;
        } else if (verbose.equals("true")) {
            finalJson = readJsonResource("CC-Rebalance-no-goals-verbose.json");
            userTaskId = REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID;
        } else {
            finalJson = readJsonResource("CC-Rebalance-no-goals.json");
            userTaskId = REBALANCE_NO_GOALS_RESPONSE_UTID;
        }

        String finalState = pendingCalls > 0 ? "pending-" + (pendingCalls - 1) : Scenario.STARTED;

        server.stubFor(baseRequest
                .withId(UUID.randomUUID())
                .inScenario(scenarioName)
                .whenScenarioStateIs(finalState)
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", userTaskId)
                        .withFixedDelay(responseDelay * 1000)
                        .withBody(finalJson)));
    }

    /**
     * Setup responses for various bad goal configurations possible on a rebalance request.
     */
    public void setupCCRebalanceBadGoalsError(CruiseControlEndpoints endpoint) {
        String jsonError = readJsonResource("CC-Rebalance-bad-goals-error.json");
        String jsonSummary = readJsonResource("CC-Rebalance-no-goals-verbose.json");

        // Response for skip_hard_goal_check=false (error)
        server.stubFor(post(urlPathEqualTo(endpoint.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.VERBOSE.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.GOALS.toString(), matching(".+"))
                .withQueryParam(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.toString(), equalTo("false"))
                .withHeader("Authorization", equalTo(AUTH_HEADER_VALUE))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withHeader("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID)
                        .withBody(jsonError)));

        // Response for skip_hard_goal_check=true (success)
        server.stubFor(post(urlPathEqualTo(endpoint.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.VERBOSE.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.GOALS.toString(), matching(".+"))
                .withQueryParam(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(AUTH_HEADER_VALUE))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID)
                        .withBody(jsonSummary)));
    }

    /**
     * Sets up the User Tasks endpoint with stateful responses using WireMock scenarios.
     */
    public void setupCCUserTasksResponseNoGoals(int activeCalls, int inExecutionCalls) {
        for (boolean verbose : new boolean[]{false, true}) {
            String scenarioName = verbose ? USER_TASKS_VERBOSE_SCENARIO : USER_TASKS_SCENARIO;
            String userTaskId = verbose
                    ? REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID
                    : REBALANCE_NO_GOALS_RESPONSE_UTID;

            MappingBuilder baseRequest = userTasksRequestMatcher(verbose);
            String responseUserTaskId = verbose
                    ? USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID
                    : USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID;

            int stateCounter = 0;

            // Setup active calls
            for (int i = 0; i < activeCalls; i++) {
                String currentState = stateCounter == 0 ? Scenario.STARTED : "state-" + (stateCounter - 1);
                String nextState = "state-" + stateCounter;

                server.stubFor(baseRequest
                        .withId(UUID.randomUUID())
                        .inScenario(scenarioName)
                        .whenScenarioStateIs(currentState)
                        .willSetStateTo(nextState)
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("User-Task-ID", responseUserTaskId)
                                .withBody(userTaskResponseJson(ACTIVE, verbose))));
                stateCounter++;
            }

            // Setup in-execution calls
            for (int i = 0; i < inExecutionCalls; i++) {
                String currentState = stateCounter == 0 ? Scenario.STARTED : "state-" + (stateCounter - 1);
                String nextState = "state-" + stateCounter;

                server.stubFor(baseRequest
                        .withId(UUID.randomUUID())
                        .inScenario(scenarioName)
                        .whenScenarioStateIs(currentState)
                        .willSetStateTo(nextState)
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("User-Task-ID", responseUserTaskId)
                                .withBody(userTaskResponseJson(IN_EXECUTION, verbose))));
                stateCounter++;
            }

            // Setup final completed response
            String finalState = stateCounter == 0 ? Scenario.STARTED : "state-" + (stateCounter - 1);

            server.stubFor(baseRequest
                    .withId(UUID.randomUUID())
                    .inScenario(scenarioName)
                    .whenScenarioStateIs(finalState)
                    .willReturn(aResponse()
                            .withStatus(200)
                            .withHeader("User-Task-ID", responseUserTaskId)
                            .withBody(userTaskResponseJson(COMPLETED, verbose))));
        }
    }

    /**
     * Setup response of task completed with error.
     */
    public void setupCCUserTasksCompletedWithError() {
        String compWithErrorJson = readJsonResource("CC-User-task-rebalance-no-goals-CompletedWithError.json");

        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withQueryParam(CruiseControlParameters.FETCH_COMPLETE.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(AUTH_HEADER_VALUE))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID)
                        .withBody(compWithErrorJson)));
    }

    public void setupCCUserTasksToReturnError(int statusCode, String errorMessage) {
        String errorJson = "{\"errorMessage\":\"" + errorMessage + "\"}";

        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withQueryParam(CruiseControlParameters.FETCH_COMPLETE.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(AUTH_HEADER_VALUE))
                .willReturn(aResponse()
                        .withStatus(statusCode)
                        .withBody(errorJson)));
    }

    /**
     * Setup response when user task is not found
     */
    public void setupUserTasktoEmpty() {
        String jsonEmptyUserTask = readJsonResource("CC-User-task-status-empty.json");

        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withQueryParam(CruiseControlParameters.FETCH_COMPLETE.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(AUTH_HEADER_VALUE))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID)
                        .withBody(jsonEmptyUserTask)));
    }

    /**
     * Setup response of task being stopped.
     */
    public void setupCCStopResponse() {
        String jsonStop = readJsonResource("CC-Stop.json");

        server.stubFor(post(urlPathEqualTo(CruiseControlEndpoints.STOP.toString()))
                .withQueryParam(CruiseControlParameters.JSON.toString(), matching("true|false"))
                .withHeader("Authorization", equalTo(AUTH_HEADER_VALUE))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", "stopped")
                        .withBody(jsonStop)));
    }

    private static String convertToHeaderValue(HTTPHeader httpHeader) {
        if (httpHeader == null) {
            return null;
        }
        return httpHeader.getValue();
    }
}