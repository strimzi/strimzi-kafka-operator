/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper.plugin;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class DebugGatekeeperPluginTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String NAME = "my-user";
    private static final KafkaUser USER = new KafkaUserBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(NAME)
            .endMetadata()
            .withNewStatus()
                .withUsername(NAME)
            .endStatus()
            .build();

    private final DebugGatekeeperPlugin plugin = new DebugGatekeeperPlugin();

    private ListAppender appender;
    private Logger logger;

    @BeforeEach
    public void beforeEach() {
        // Attach an in-memory appender to the plugin's logger so the emitted messages can be asserted
        appender = new ListAppender();
        appender.start();
        logger = (Logger) LogManager.getLogger(DebugGatekeeperPlugin.class);
        logger.addAppender(appender);
    }

    @AfterEach
    public void afterEach() {
        logger.removeAppender(appender);
        appender.stop();
    }

    //////////////////////////////////////////////////
    // Tests for the message-building helpers
    //////////////////////////////////////////////////

    @Test
    public void testEventMessageStartsWithPrefixAndDescribesTheEvent() {
        String message = DebugGatekeeperPlugin.eventMessage(kafkaUser(Map.of()), "entered the reconciliation", "");

        assertThat(message, allOf(
                startsWith(DebugGatekeeperPlugin.LOG_PREFIX),
                containsString("KafkaUser"),
                containsString(NAMESPACE + "/" + NAME),
                containsString("entered the reconciliation")));
    }

    @Test
    public void testEventMessageAppendsTheYaml() {
        String message = DebugGatekeeperPlugin.eventMessage(kafkaUser(Map.of()), "entered the reconciliation", " with resource:\nfoo");

        assertThat(message, allOf(
                containsString("entered the reconciliation with resource:"),
                containsString("foo")));
    }

    @Test
    public void testDeletionMessageStartsWithPrefixAndDescribesTheEvent() {
        String message = DebugGatekeeperPlugin.deletionMessage("KafkaUser", NAMESPACE, NAME);

        assertThat(message, allOf(
                startsWith(DebugGatekeeperPlugin.LOG_PREFIX),
                containsString(NAMESPACE + "/" + NAME),
                containsString("is being deleted")));
    }

    @Test
    public void testYamlMessageSerializesTheObject() {
        String message = DebugGatekeeperPlugin.yamlMessage("resource", kafkaUser(Map.of()));

        assertThat(message, allOf(
                startsWith(" with resource:"),
                // The object is serialized as YAML, so the kind and metadata name appear in the message
                containsString("kind"),
                containsString(NAME)));
    }

    //////////////////////////////////////////////////
    // Tests for the message-building helpers
    //////////////////////////////////////////////////

    @Test
    public void testAllOperandHooksCompleteWithoutModifyingTheResources() {
        Kafka kafka = new KafkaBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        KafkaNodePool pool = new KafkaNodePoolBuilder().withNewMetadata().withNamespace(NAMESPACE).withName("pool").endMetadata().build();
        plugin.kafkaEntry(null, kafka, List.of(pool)).toCompletableFuture().join();
        plugin.kafkaExit(null, kafka, List.of(pool), new KafkaStatus()).toCompletableFuture().join();
        plugin.kafkaNodePoolExit(null, kafka, pool, new KafkaNodePoolStatus()).toCompletableFuture().join();
        plugin.kafkaDeletion(null, NAMESPACE, NAME).toCompletableFuture().join();
        assertThat(kafka.getMetadata().getName(), is(NAME));

        KafkaConnect connect = new KafkaConnectBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        KafkaConnector connector = new KafkaConnectorBuilder().withNewMetadata().withNamespace(NAMESPACE).withName("connector").endMetadata().build();
        plugin.kafkaConnectEntry(null, connect, List.of(connector)).toCompletableFuture().join();
        plugin.kafkaConnectExit(null, connect, List.of(connector), new KafkaConnectStatus()).toCompletableFuture().join();
        plugin.kafkaConnectorExit(null, connect, connector, new KafkaConnectorStatus()).toCompletableFuture().join();
        plugin.kafkaConnectDeletion(null, NAMESPACE, NAME).toCompletableFuture().join();
        assertThat(connect.getMetadata().getName(), is(NAME));

        KafkaMirrorMaker2 mirrorMaker2 = new KafkaMirrorMaker2Builder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        plugin.kafkaMirrorMaker2Entry(null, mirrorMaker2).toCompletableFuture().join();
        plugin.kafkaMirrorMaker2Exit(null, mirrorMaker2, new KafkaMirrorMaker2Status()).toCompletableFuture().join();
        plugin.kafkaMirrorMaker2Deletion(null, NAMESPACE, NAME).toCompletableFuture().join();

        KafkaBridge bridge = new KafkaBridgeBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        plugin.kafkaBridgeEntry(null, bridge).toCompletableFuture().join();
        plugin.kafkaBridgeExit(null, bridge, new KafkaBridgeStatus()).toCompletableFuture().join();
        plugin.kafkaBridgeDeletion(null, NAMESPACE, NAME).toCompletableFuture().join();

        KafkaRebalance rebalance = new KafkaRebalanceBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        plugin.kafkaRebalanceEntry(null, rebalance).toCompletableFuture().join();
        plugin.kafkaRebalanceExit(null, rebalance, new KafkaRebalanceStatus()).toCompletableFuture().join();
        plugin.kafkaRebalanceDeletion(null, NAMESPACE, NAME).toCompletableFuture().join();

        KafkaTopic topic = new KafkaTopicBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        plugin.kafkaTopicEntry(null, topic).toCompletableFuture().join();
        plugin.kafkaTopicExit(null, topic, new KafkaTopicStatus()).toCompletableFuture().join();
        plugin.kafkaTopicDeletion(null, NAMESPACE, NAME).toCompletableFuture().join();
    }

    //////////////////////////////////////////////////
    // Tests for the actual logging behaviour of the hooks
    //////////////////////////////////////////////////

    @Test
    public void testEntryAlwaysLogsTheEventMessage() {
        plugin.kafkaUserEntry(null, kafkaUser(Map.of())).toCompletableFuture().join();

        // The short event message is always logged ...
        assertThat(loggedEventMessages(), is(List.of(DebugGatekeeperPlugin.LOG_PREFIX + "KafkaUser " + NAMESPACE + "/" + NAME + " entered the reconciliation")));
        // ... but the resource YAML is not logged without the annotation
        assertThat(loggedYaml("resource"), is(Collections.emptyList()));
    }

    @Test
    public void testEntryLogsTheResourceYamlWhenAnnotated() {
        plugin.kafkaUserEntry(null, kafkaUser(Map.of(DebugGatekeeperPlugin.ENTRY_DEBUG_ANNOTATION, "true"))).toCompletableFuture().join();

        List<String> yaml = loggedYaml("resource");
        assertThat(yaml.size(), is(1));
        // The event message and the dumped resource YAML are logged as a single message
        assertThat(yaml.get(0), allOf(containsString("entered the reconciliation"), containsString("kind"), containsString(NAME)));
    }

    @Test
    public void testEntryDoesNotLogTheResourceYamlWhenTheExitAnnotationIsUsed() {
        // Only the entry annotation enables the entry YAML dump
        plugin.kafkaUserEntry(null, kafkaUser(Map.of(DebugGatekeeperPlugin.EXIT_DEBUG_ANNOTATION, "true"))).toCompletableFuture().join();

        assertThat(loggedYaml("resource"), is(Collections.emptyList()));
    }

    @Test
    public void testExitAlwaysLogsTheEventMessage() {
        plugin.kafkaUserExit(null, kafkaUser(Map.of()), USER.getStatus()).toCompletableFuture().join();

        assertThat(loggedEventMessages(), is(List.of(DebugGatekeeperPlugin.LOG_PREFIX + "KafkaUser " + NAMESPACE + "/" + NAME + " exited the reconciliation")));
        assertThat(loggedYaml("status"), is(Collections.emptyList()));
    }

    @Test
    public void testExitLogsTheStatusYamlWhenAnnotated() {
        plugin.kafkaUserExit(null, kafkaUser(Map.of(DebugGatekeeperPlugin.EXIT_DEBUG_ANNOTATION, "true")), USER.getStatus()).toCompletableFuture().join();

        List<String> yaml = loggedYaml("status");
        assertThat(yaml.size(), is(1));
        // The event message and the dumped status YAML are logged as a single message
        assertThat(yaml.get(0), allOf(containsString("exited the reconciliation"), containsString("username")));
    }

    @Test
    public void testDeletionLogsTheEventMessage() {
        plugin.kafkaUserDeletion(null, NAMESPACE, NAME).toCompletableFuture().join();

        assertThat(loggedEventMessages(), is(List.of(DebugGatekeeperPlugin.LOG_PREFIX + " KafkaUser " + NAMESPACE + "/" + NAME + " is being deleted")));
    }

    //////////////////////////////////////////////////
    // Helper methods
    //////////////////////////////////////////////////

    private List<String> loggedEventMessages() {
        return appender.messages.stream().filter(message -> !message.contains(System.lineSeparator())).toList();
    }

    private List<String> loggedYaml(String what) {
        return appender.messages.stream().filter(message -> message.contains(" " + what + ":" + System.lineSeparator())).toList();
    }

    private static KafkaUser kafkaUser(Map<String, String> annotations) {
        return new KafkaUserBuilder(USER)
                .editMetadata()
                    .withAnnotations(annotations)
                .endMetadata()
                .build();
    }


    /**
     * Minimal in-memory Log4j2 appender collecting the formatted messages of the events it receives.
     */
    private static final class ListAppender extends AbstractAppender {
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

        private ListAppender() {
            super("DebugGatekeeperPluginTestAppender", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
