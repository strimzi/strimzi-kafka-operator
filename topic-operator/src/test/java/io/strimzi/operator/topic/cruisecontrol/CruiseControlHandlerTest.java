/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.api.kafka.model.topic.ReplicasChangeState;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.topic.TestUtil;
import io.strimzi.operator.topic.TopicOperatorConfig;
import io.strimzi.operator.topic.TopicOperatorUtil;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.Results;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;

public class CruiseControlHandlerTest {
    private static final String NAMESPACE = TestUtil.namespaceName(CruiseControlHandlerTest.class);

    private static TopicOperatorMetricsHolder metricsHolder;
    private static int serverPort;
    private static File tlsCrtFile;
    private static File apiUserFile;
    private static File apiPassFile;
    private static MockCruiseControl server;

    @BeforeAll
    public static void beforeAll() throws IOException {
        metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null,
            new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        
        serverPort = TestUtils.getFreePort();
        File tlsKeyFile = ReadWriteUtils.tempFile(CruiseControlHandlerTest.class.getSimpleName(), ".key");
        tlsCrtFile = ReadWriteUtils.tempFile(CruiseControlHandlerTest.class.getSimpleName(), ".crt");
        new MockCertManager().generateSelfSignedCert(tlsKeyFile, tlsCrtFile,
            new Subject.Builder().withCommonName("Trusted Test CA").build(), 365);
        apiUserFile = ReadWriteUtils.tempFile(CruiseControlHandlerTest.class.getSimpleName(), ".username");
        try (PrintWriter out = new PrintWriter(apiUserFile.getAbsolutePath())) {
            out.print("topic-operator-admin");
        }
        apiPassFile = ReadWriteUtils.tempFile(CruiseControlHandlerTest.class.getSimpleName(), ".password");
        try (PrintWriter out = new PrintWriter(apiPassFile.getAbsolutePath())) {
            out.print("changeit");
        }
        server = new MockCruiseControl(serverPort, tlsKeyFile, tlsCrtFile);
    }

    @AfterAll
    public static void afterAll() {
        if (server != null && server.isRunning()) {
            server.stop();
        }
    }

    @BeforeEach
    public void beforeEach() {
        if (server != null && server.isRunning()) {
            server.reset();
        }
    }

    @ParameterizedTest
    @MethodSource("operatorConfigs")
    public void replicasChangeShouldShouldCompleteWithValidConfig(TopicOperatorConfig config) {
        var handler = new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config));

        server.expectTopicConfigSuccessResponse(apiUserFile, apiPassFile);
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertOngoing(pending, pendingAndOngoing);

        server.expectUserTasksSuccessResponse(apiUserFile, apiPassFile);
        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertCompleted(ongoing, completedAndFailed);
    }

    @Test
    public void replicasChangeShouldFailWhenCruiseControlEndpointNotReachable() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "invalid"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort))
        ));
        
        var handler = new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config));
        
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pending, pendingAndOngoing, "Replicas change failed, Connection failed");

        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(ongoing, completedAndFailed, "Replicas change failed, Connection failed");
    }

    @Test
    public void replicasChangeShouldFailWhenCruiseControlReturnsErrorResponse() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), tlsCrtFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
        ));

        var handler = new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config));

        server.expectTopicConfigErrorResponse(apiUserFile, apiPassFile);
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pending, pendingAndOngoing, "Replicas change failed, Request failed (500), Cluster model not ready");

        server.expectUserTasksErrorResponse(apiUserFile, apiPassFile);
        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(ongoing, completedAndFailed, "Replicas change failed, Request failed (500), " +
            "Error processing GET request '/user_tasks' due to: 'Error happened in fetching response for task 9730e4fb-ea41-4e2d-b053-9be2310589b5'.");
    }

    @Test
    public void replicasChangeShouldFailWhenTheRequestTimesOut() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), tlsCrtFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
        ));

        var handler = new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config));

        server.expectTopicConfigRequestTimeout(apiUserFile, apiPassFile);
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pending, pendingAndOngoing, "Replicas change failed, Request failed (408)");

        server.expectUserTasksRequestTimeout(apiUserFile, apiPassFile);
        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(ongoing, completedAndFailed, "Replicas change failed, Request failed (408)");
    }

    @Test
    public void replicasChangeShouldFailWhenTheRequestIsUnauthorized() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
            entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
            entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
            entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
            entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
            entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), tlsCrtFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
            entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
        ));

        var handler = new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config));

        server.expectTopicConfigRequestUnauthorized(apiUserFile, apiPassFile);
        var pending = buildPendingReconcilableTopics();
        var pendingAndOngoing = handler.requestPendingChanges(pending);
        assertFailedWithMessage(pending, pendingAndOngoing, "Replicas change failed, Request failed (401), Authorization error");

        server.expectUserTasksRequestUnauthorized(apiUserFile, apiPassFile);
        var ongoing = buildOngoingReconcilableTopics();
        var completedAndFailed = handler.requestOngoingChanges(ongoing);
        assertFailedWithMessage(ongoing, completedAndFailed, "Replicas change failed, Request failed (401), Authorization error");
    }

    private static void assertOngoing(List<ReconcilableTopic> input, Results output) {
        assertThat(output.size(), greaterThan(0));
        var inputRt = input.get(0);
        var outputRcs = output.getReplicasChange(inputRt);
        assertThat(outputRcs, is(notNullValue()));
        assertThat(outputRcs.getMessage(), is(nullValue()));
        assertThat(outputRcs.getSessionId(), is(notNullValue()));
        assertThat(outputRcs.getState(), is(ReplicasChangeState.ONGOING));
        assertThat(outputRcs.getTargetReplicas(), is(inputRt.kt().getSpec().getReplicas()));
    }

    private static void assertCompleted(List<ReconcilableTopic> input, Results output) {
        assertThat(output.size(), greaterThan(0));
        var inputRt = input.get(0);
        var outputRcs = output.getReplicasChange(inputRt);
        assertThat(outputRcs, is(nullValue()));
    }

    private static void assertFailedWithMessage(List<ReconcilableTopic> input, Results output, String message) {
        assertThat(output.size(), greaterThan(0));
        var inputRt = input.get(0);
        var outputRcs = output.getReplicasChange(inputRt);
        assertThat(outputRcs, is(notNullValue()));
        assertThat(outputRcs.getMessage(), is(message));
    }

    private List<ReconcilableTopic> buildPendingReconcilableTopics() {
        var topicName = "my-topic";
        var replicationFactor = 2;
        var status = new KafkaTopicStatusBuilder()
            .withConditions(List.of(new ConditionBuilder()
                .withType("Ready")
                .withStatus("True")
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .build()))
            .build();
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(++replicationFactor)
            .endSpec()
            .withStatus(status)
            .build();
        return List.of(new ReconcilableTopic(
            new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, topicName), 
            kafkaTopic, topicName));
    }

    private List<ReconcilableTopic> buildOngoingReconcilableTopics() {
        var topicName = "my-topic";
        var replicationFactor = 3;
        var status = new KafkaTopicStatusBuilder()
            .withConditions(List.of(new ConditionBuilder()
                .withType("Ready")
                .withStatus("True")
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .build()))
            .withReplicasChange(new ReplicasChangeStatusBuilder()
                .withSessionId("8911ca89-351f-888-8d0f-9aade00e098h")
                .withState(ReplicasChangeState.ONGOING)
                .withTargetReplicas(replicationFactor)
                .build())
            .build();
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(3)
            .endSpec()
            .withStatus(status)
            .build();
        return List.of(new ReconcilableTopic(
            new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, topicName), 
            kafkaTopic, topicName));
    }

    // this is used as method source in parameterized tests
    private static List<TopicOperatorConfig> operatorConfigs() {
        return Arrays.asList(
            // encryption and authentication disabled
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
                entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "false"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "false")
            )),

            // encryption and authentication enabled
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
                entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), tlsCrtFile.getAbsolutePath()),
                entry(TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), apiUserFile.getAbsolutePath()),
                entry(TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), apiPassFile.getAbsolutePath())
            )),

            // rack enabled
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_RACK_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort))
            )),

            // encryption only
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
                entry(TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "localhost"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), String.valueOf(serverPort)),
                entry(TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "false"),
                entry(TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), tlsCrtFile.getAbsolutePath())
            )),

            // authentication only
            TopicOperatorConfig.buildFromMap(Map.ofEntries(
                entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092"),
                entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
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
