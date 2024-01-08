/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.mockkube2.MockKube2;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static io.strimzi.test.TestUtils.waitFor;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class TopicOperatorMockIT {
    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorMockIT.class);
    private static final String NAMESPACE = "my-namespace";

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;
    private Session session;
    private StrimziKafkaCluster kafkaCluster;
    private static Vertx vertx;
    private String deploymentId;
    private AdminClient adminClient;
    private TopicConfigsWatcher topicsConfigWatcher;
    private ZkTopicWatcher topicWatcher;
    private PrometheusMeterRegistry metrics;
    private ZkTopicsWatcher topicsWatcher;

    // TODO this is all in common with TOIT, so factor out a common base class

    @BeforeAll
    public static void beforeAll() {
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true));
        vertx = Vertx.vertx(options);
    }

    @AfterAll
    public static void afterAll() throws InterruptedException, ExecutionException {
        vertx.close().toCompletionStage().toCompletableFuture().get();
    }

    @BeforeEach
    public void setup(VertxTestContext context) throws Exception {
        //Create cluster in @BeforeEach instead of @BeforeAll as once the checkpoints causing premature success were fixed,
        //tests were failing due to topic "my-topic" already existing, and trying to delete the topics at the end of the test was timing out occasionally.
        //So works best when the cluster is recreated for each test to avoid shared state
        Map<String, String> config = new HashMap<>();
        config.put("zookeeper.connect", "zookeeper:2181");
        kafkaCluster = new StrimziKafkaCluster(1, 1, config);
        kafkaCluster.start();

        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaTopicCrd()
                .build();
        mockKube.start();

        // Configure the namespace
        client.getConfiguration().setNamespace(NAMESPACE);

        adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()));

        Config topicConfig = new Config(Map.of(
            Config.KAFKA_BOOTSTRAP_SERVERS.key, kafkaCluster.getBootstrapServers(),
            Config.ZOOKEEPER_CONNECT.key, kafkaCluster.getZookeeper().getHost() + ":" + kafkaCluster.getZookeeper().getFirstMappedPort(),
            Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS.key, "30000",
            Config.NAMESPACE.key, NAMESPACE,
            Config.CLIENT_ID.key, "myproject-client-id",
            Config.FULL_RECONCILIATION_INTERVAL_MS.key, "10000"
        ));

        session = new Session(client, topicConfig);

        Checkpoint async = context.checkpoint();
        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                deploymentId = ar.result();
                topicsConfigWatcher = session.topicConfigsWatcher;
                topicWatcher = session.topicWatcher;
                topicsWatcher = session.topicsWatcher;
                metrics = session.metricsRegistry;
                metrics.forEachMeter(meter -> metrics.remove(meter));
                async.flag();
            } else {
                ar.cause().printStackTrace();
                context.failNow(new Throwable("Failed to deploy session"));
            }
        });
        if (!context.awaitCompletion(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        int timeout = 30_000;

        waitFor("Topic watcher not started",  1_000, timeout,
            () -> this.topicWatcher.started());
        waitFor("Topic configs watcher not started", 1_000, timeout,
            () -> this.topicsConfigWatcher.started());
        waitFor("Topic watcher not started", 1_000, timeout,
            () -> this.topicsWatcher.started());
    }

    @AfterEach
    public void tearDown(VertxTestContext context) {
        if (vertx != null && deploymentId != null) {
            vertx.undeploy(deploymentId, undeployResult -> {
                mockKube.stop();
                topicWatcher.stop();
                topicsWatcher.stop();
                topicsConfigWatcher.stop();
                metrics.close();
                waitFor("Topic watcher stopped",  1_000, 30_000,
                    () -> !this.topicWatcher.started());
                waitFor("Topic configs watcher stopped", 1_000, 30_000,
                    () -> !this.topicsConfigWatcher.started());
                waitFor("Topic watcher stopped", 1_000, 30_000,
                    () -> !this.topicsWatcher.started());
                waitFor("Metrics watcher stopped", 1_000, 30_000,
                    () -> this.metrics.isClosed());
                if (adminClient != null) {
                    adminClient.close();
                }

                kafkaCluster.stop();
                
                context.completeNow();
            });
        } else {
            context.completeNow();
        }
    }

    private void createInKube(KafkaTopic topic) {
        Crds.topicOperation(client).resource(topic).create();
    }

    private void updateInKube(KafkaTopic topic) {
        LOGGER.info("Updating topic {} in kube", topic.getMetadata().getName());
        Crds.topicOperation(client).resource(topic).update();
    }

    @Test
    public void testCreatedWithoutTopicNameInKube(VertxTestContext context) throws InterruptedException, ExecutionException {
        LOGGER.info("Test started");

        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                    .addToLabels(Labels.KUBERNETES_NAME_LABEL, "topic-operator")
                .endMetadata()
                .withNewSpec()
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

    void testCreatedInKube(VertxTestContext context, KafkaTopic kt) throws InterruptedException, ExecutionException {
        String kubeName = kt.getMetadata().getName();
        String kafkaName = kt.getSpec().getTopicName() != null ? kt.getSpec().getTopicName() : kubeName;
        int retention = (Integer) kt.getSpec().getConfig().get("retention.bytes");

        createInKube(kt);

        // Check created in Kafka
        waitUntilTopicExistsInKafka(kafkaName);
        LOGGER.info("Topic has been created");
        Topic fromKafka = getFromKafka(kafkaName);
        context.verify(() -> assertThat(fromKafka.getTopicName().toString(), is(kafkaName)));

        // Reconcile after no changes
        reconcile(context);
        // Check things still the same
        context.verify(() -> assertThat(fromKafka, is(getFromKafka(kafkaName))));

        // Config change + reconcile
        updateInKube(new KafkaTopicBuilder(kt).editSpec().addToConfig("retention.bytes", retention + 1).endSpec().build());
        waitUntilTopicInKafka(kafkaName, config -> Integer.toString(retention + 1).equals(config.get("retention.bytes").value()));
        // Another reconciliation
        reconcile(context);

        // Check things still the same
        context.verify(() -> {
            assertThat(getFromKafka(kafkaName), is(new Topic.Builder(fromKafka)
                    .withConfigEntry("retention.bytes", Integer.toString(retention + 1))
                    .build()));
            context.completeNow();
        });
    }

    Topic getFromKafka(String topicName) throws InterruptedException, ExecutionException {
        Future<TopicMetadata> kafkaMetadata = session.kafka.topicMetadata(Reconciliation.DUMMY_RECONCILIATION, new TopicName(topicName));
        return kafkaMetadata.map(TopicSerialization::fromTopicMetadata).toCompletionStage().toCompletableFuture().get();
    }

    private void waitUntilTopicExistsInKafka(String topicName) {
        waitUntilTopicInKafka(topicName, Objects::nonNull);
    }

    private void waitUntilTopicInKafka(String topicName, Predicate<org.apache.kafka.clients.admin.Config> p) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        waitFor("Creation of topic " + topicName, 1_000, 60_000, () -> {
            try {
                var descriptionMap = adminClient.describeConfigs(List.of(configResource)).all().get();
                var desc = descriptionMap.get(configResource);
                return p.test(desc);
            } catch (Exception e) {
                return false;
            }
        });
    }

    void reconcile(VertxTestContext context) throws InterruptedException, ExecutionException {
        //Block on Java future.get() to ensure blocking behaviour that appears required
        session.topicOperator.reconcileAllTopics("test").onComplete(ar -> {
            if (!ar.succeeded()) {
                context.failNow(ar.cause());
            }
        }).toCompletionStage().toCompletableFuture().get();
    }


    @Test
    public void testCreatedWithSameTopicNameInKube(VertxTestContext context) throws InterruptedException, ExecutionException {

        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                .endMetadata()
                .withNewSpec()
                    .withTopicName("my-topic") // the same as metadata.name
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

    @Test
    public void testCreatedWithDifferentTopicNameInKube(VertxTestContext context) throws InterruptedException, ExecutionException {
        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                .endMetadata()
                .withNewSpec()
                    .withTopicName("DIFFERENT") // different to metadata.name
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

    @Test
    public void testCreatedWithDefaultsInKube(VertxTestContext context) throws InterruptedException, ExecutionException {
        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                .endMetadata()
                .withNewSpec()
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();
    
        testCreatedInKube(context, kt);
    }

    @Test
    public void testReconciliationPaused(VertxTestContext context) throws InterruptedException, ExecutionException {
        LOGGER.info("Test started");

        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                    .addToLabels(Labels.KUBERNETES_NAME_LABEL, "topic-operator")
                    .withAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"))
                .endMetadata()
                .withNewSpec()
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec()
                .build();

        testNotCreatedInKube(context, kt);
    }

    void testNotCreatedInKube(VertxTestContext context, KafkaTopic kt) throws InterruptedException, ExecutionException {
        String kubeName = kt.getMetadata().getName();
        String kafkaName = kt.getSpec().getTopicName() != null ? kt.getSpec().getTopicName() : kubeName;
        int retention = (Integer) kt.getSpec().getConfig().get("retention.bytes");

        createInKube(kt);

        Thread.sleep(2000);
        LOGGER.info("Topic has not been created");
        Topic fromKafka = getFromKafka(kafkaName);
        context.verify(() -> assertThat(fromKafka, is(nullValue())));
        // Reconcile after no changes
        reconcile(context);
        // Check things still the same
        context.verify(() -> assertThat(fromKafka, is(nullValue())));

        // Config change + reconcile
        updateInKube(new KafkaTopicBuilder(kt).editSpec().addToConfig("retention.bytes", retention + 1).endSpec().build());
        // Another reconciliation
        reconcile(context);

        // Check things still the same
        context.verify(() -> {
            assertThat(getFromKafka(kafkaName), is(nullValue()));
            context.completeNow();
        });
    }

}
