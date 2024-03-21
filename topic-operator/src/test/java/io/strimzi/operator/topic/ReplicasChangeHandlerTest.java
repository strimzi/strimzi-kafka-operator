/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.MockCertManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.topic.KafkaTopic.RESOURCE_KIND;
import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.ONGOING;
import static io.strimzi.test.TestUtils.getFreePort;
import static java.util.Map.entry;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ReplicasChangeHandlerTest {
    private static final String TEST_PREFIX = "cruise-control-";
    private static final String TEST_NAMESPACE = "replicas-change";
    private static final String TEST_NAME = "my-topic";
    
    private static int serverPort;
    private static File sslCrtFile;
    private static File apiUserFile;
    private static File apiPassFile;
    private static MockCruiseControl server;

    @BeforeAll
    public static void beforeAll() throws IOException {
        serverPort = getFreePort();

        MockCertManager certManager = new MockCertManager();
        File tlsKeyFile = Files.createTempFile(TEST_PREFIX, ".key").toFile();
        sslCrtFile = Files.createTempFile(TEST_PREFIX, "-valid.crt").toFile();
        sslCrtFile.deleteOnExit();
        new MockCertManager().generateSelfSignedCert(tlsKeyFile, sslCrtFile, 
            new Subject.Builder().withCommonName("Trusted Test CA").build(), 365);

        apiUserFile = Files.createTempFile(TEST_PREFIX, ".username").toFile();
        apiUserFile.deleteOnExit();
        try (PrintWriter out = new PrintWriter(apiUserFile.getAbsolutePath())) {
            out.print("topic-operator-admin");
        }
        apiPassFile = Files.createTempFile(TEST_PREFIX, ".password").toFile();
        apiPassFile.deleteOnExit();
        try (PrintWriter out = new PrintWriter(apiPassFile.getAbsolutePath())) {
            out.print("changeit");
        }
        
        server = new MockCruiseControl(serverPort, tlsKeyFile, sslCrtFile);
    }

    @AfterAll
    public static void afterAll() {
        if (server != null && server.isRunning()) {
            server.stop();
        }
    }

    @BeforeEach
    public void beforeEach() {
        server.reset();
    }

    @ParameterizedTest
    @MethodSource("validConfigs")
    public void shouldSucceedWithValidConfig(TopicOperatorConfig config) {
        var handler = new ReplicasChangeHandler(config);

        server.expectTopicConfigSuccessResponse(apiUserFile, apiPassFile);
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertOngoing(pending, pendingAndOngoing);

        server.expectUserTasksSuccessResponse(apiUserFile, apiPassFile);
        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertCompleted(completedAndFailed);
    }
    
    @Test
    public void shouldFailWithSslEnabledAndMissingCrtFile() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), "/invalid/ca.crt")
        ));
        
        var handler = new ReplicasChangeHandler(config);
        
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pendingAndOngoing, "Replicas change failed, File not found: /invalid/ca.crt");

        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(completedAndFailed, "Replicas change failed, File not found: /invalid/ca.crt");
    }

    @Test
    public void shouldFailWithAuthEnabledAndUsernameFileNotFound() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), "/invalid/username"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
        ));
        
        var handler = new ReplicasChangeHandler(config);
        
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pendingAndOngoing, "Replicas change failed, File not found: /invalid/username");

        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(completedAndFailed, "Replicas change failed, File not found: /invalid/username");
    }

    @Test
    public void shouldFailWithAuthEnabledAndPasswordFileNotFound() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), "/invalid/password")
        ));
        
        var handler = new ReplicasChangeHandler(config);
        
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pendingAndOngoing, "Replicas change failed, File not found: /invalid/password");

        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(completedAndFailed, "Replicas change failed, File not found: /invalid/password");
    }

    @Test
    public void shouldFailWhenCruiseControlEndpointNotReachable() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "invalid"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort))
        ));
        
        var handler = new ReplicasChangeHandler(config);
        
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pendingAndOngoing, "Replicas change failed, java.net.ConnectException");

        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(completedAndFailed, "Replicas change failed, java.net.ConnectException");
    }

    @Test
    public void shouldFailWhenCruiseControlReturnsErrorResponse() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), sslCrtFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
        ));

        var handler = new ReplicasChangeHandler(config);

        server.expectTopicConfigErrorResponse(apiUserFile, apiPassFile);
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pendingAndOngoing, "Replicas change failed (500), Cluster model not ready");

        server.expectUserTasksErrorResponse(apiUserFile, apiPassFile);
        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(completedAndFailed, "Replicas change failed (500), Error processing GET " +
            "request '/user_tasks' due to: 'Error happened in fetching response for task 9730e4fb-ea41-4e2d-b053-9be2310589b5'.");
    }

    @Test
    public void shouldFailWhenTheRequestTimesOut() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), sslCrtFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
        ));

        var handler = new ReplicasChangeHandler(config);

        server.expectTopicConfigRequestTimeout(apiUserFile, apiPassFile);
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pendingAndOngoing, "Replicas change failed (408)");

        server.expectUserTasksRequestTimeout(apiUserFile, apiPassFile);
        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(completedAndFailed, "Replicas change failed (408)");
    }

    @Test
    public void shouldFailWhenTheRequestIsUnauthorized() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), sslCrtFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
        ));

        var handler = new ReplicasChangeHandler(config);

        server.expectTopicConfigRequestUnauthorized(apiUserFile, apiPassFile);
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pendingAndOngoing, "Replicas change failed (401)");

        server.expectUserTasksRequestUnauthorized(apiUserFile, apiPassFile);
        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(completedAndFailed, "Replicas change failed (401)");
    }

    private static void assertOngoing(List<ReconcilableTopic> input, List<ReconcilableTopic> output) {
        assertThat(output.isEmpty(), is(false));
        var outputKt = output.stream().findFirst().get().kt();
        assertThat(outputKt.getStatus().getReplicasChange(), is(notNullValue()));
        assertThat(outputKt.getStatus().getReplicasChange().getMessage(), is(nullValue()));
        assertThat(outputKt.getStatus().getReplicasChange().getSessionId(), is(notNullValue()));
        assertThat(outputKt.getStatus().getReplicasChange().getState(), is(ONGOING));
        var inputKt = input.stream().findFirst().get().kt();
        assertThat(outputKt.getStatus().getReplicasChange().getTargetReplicas(), is(inputKt.getSpec().getReplicas()));
    }

    private static void assertCompleted(List<ReconcilableTopic> output) {
        assertThat(output.isEmpty(), is(false));
        var kt = output.stream().findFirst().get().kt();
        assertThat(kt.getStatus().getReplicasChange(), is(nullValue()));
    }

    private static void assertFailedWithMessage(List<ReconcilableTopic> output, String message) {
        assertThat(output.isEmpty(), is(false));
        var outputKt = output.stream().findFirst().get().kt();
        assertThat(outputKt.getStatus().getReplicasChange(), is(notNullValue()));
        assertThat(outputKt.getStatus().getReplicasChange().getMessage(), is(message));
    }

    private List<ReconcilableTopic> buildPendingReconcilableTopics() {
        int replicationFactor = 2;
        var status = new KafkaTopicStatusBuilder()
            .withConditions(List.of(new ConditionBuilder()
                .withType("Ready")
                .withStatus("True")
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .build()))
            .build();
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(TEST_NAME)
                .withNamespace(TEST_NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(++replicationFactor)
            .endSpec()
            .withStatus(status)
            .build();
        return List.of(new ReconcilableTopic(
            new Reconciliation("test", RESOURCE_KIND, TEST_NAMESPACE, TEST_NAME), 
            kafkaTopic, TopicOperatorUtil.topicName(kafkaTopic)));
    }

    private List<ReconcilableTopic> buildOngoingReconcilableTopics() {
        int replicationFactor = 3;
        var status = new KafkaTopicStatusBuilder()
            .withConditions(List.of(new ConditionBuilder()
                .withType("Ready")
                .withStatus("True")
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .build()))
            .withReplicasChange(new ReplicasChangeStatusBuilder()
                .withSessionId("8911ca89-351f-888-8d0f-9aade00e098h")
                .withState(ONGOING)
                .withTargetReplicas(replicationFactor)
                .build())
            .build();
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(TEST_NAME)
                .withNamespace(TEST_NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(3)
            .endSpec()
            .withStatus(status)
            .build();
        return List.of(new ReconcilableTopic(
            new Reconciliation("test", RESOURCE_KIND, TEST_NAMESPACE, TEST_NAME), 
            kafkaTopic, TopicOperatorUtil.topicName(kafkaTopic)));
    }

    private static List<TopicOperatorConfig> validConfigs() {
        return Arrays.asList(
            // encryption and authentication disabled
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
                entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "false"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "false")
            )),

            // encryption and authentication enabled
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
                entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), sslCrtFile.getAbsolutePath()),
                entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
                entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
            )),

            // rack enabled
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_RACK_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort))
            )),

            // encryption only
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
                entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "false"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), sslCrtFile.getAbsolutePath())
            )),

            // authentication only
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), TEST_NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
                entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "false"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
                entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
            ))
        );
    }
}
