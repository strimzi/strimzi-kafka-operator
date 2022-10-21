/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoSession;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SessionStartupDoesNotBlockMainThreadTest {

    @Mock
    KubernetesClient kubeClient;

    @Mock
    MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> mixedOp;

    @Mock
    NonNamespaceOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> nonNsOp;

    @Mock
    FilterWatchListDeletable<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> filterWatchListDeletable;

    @Mock
    Watch watch;

    //Turn down max block time to get test results faster
    private final Duration maxBlock = Duration.ofMillis(200);

    private final Map<String, String> mandatoryConfig = Map.of(
            Config.ZOOKEEPER_CONNECT.key, "localhost:2181",
            Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092",
            Config.NAMESPACE.key, "default",
            Config.CLIENT_ID.key, "default-client-id"
    );

    private MockitoSession mockitoSession;
    private Vertx vertx;

    @BeforeEach
    void setup() {
        mockitoSession = Mockito.mockitoSession().initMocks(this).startMocking();

        // Set max block low, and check interval to half of that,
        // so that we can verify that blocking behaviour happens (and then is fixed) without making the test really slow
        VertxOptions options = new VertxOptions()
                .setMaxEventLoopExecuteTime(maxBlock.toMillis())
                .setMaxEventLoopExecuteTimeUnit(TimeUnit.MILLISECONDS)
                .setBlockedThreadCheckInterval(maxBlock.toMillis() / 2)
                .setBlockedThreadCheckIntervalUnit(TimeUnit.MILLISECONDS)
                .setMetricsOptions(new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setJvmMetricsEnabled(true)
                .setEnabled(true));
        vertx = Vertx.vertx(options);
        // Look on my mocks returning mocks ye mighty and despair! I'm only doing this because Mockito's ANSWERS_DEEP_STUBS
        // failed to answer all the way through the chain of fabric8 method calls, for reasons I can't yet determine
        // And I want the start-up session to complete for this unit test
        when(kubeClient.resources(KafkaTopic.class, KafkaTopicList.class)).thenReturn(mixedOp);
        when(mixedOp.inNamespace(any())).thenReturn(nonNsOp);
        when(nonNsOp.withLabels(any())).thenReturn(filterWatchListDeletable);
        when(filterWatchListDeletable.watch(any())).thenReturn(watch);
    }

    @AfterEach
    void teardown() throws ExecutionException, InterruptedException {
        mockitoSession.finishMocking();
        vertx.close().toCompletionStage().toCompletableFuture().get();
    }


    @Test
    void ensureSlowTopicStoreCreationDoesNotBlockThreads() throws ExecutionException, InterruptedException {
        AtomicInteger warnings = installBlockedThreadHandler();
        Config config = new Config(mandatoryConfig);
        BiFunction<Zk, Config, TopicStore> slowStore = (zk, conf) -> {
            try {
                // Make sure we block long enough for blocked thread checker to run at least once or twice
                Thread.sleep(maxBlock.toMillis() * 5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return mock(TopicStore.class);
        };

        TopicOperatorState tos = new TopicOperatorState();
        Promise<Void> startupPromise = Promise.promise();

        Session operatorSession = new Session(kubeClient, config, slowStore, (v, conf) -> Future.succeededFuture(new MockZk()), tos);
        operatorSession.init(vertx, vertx.getOrCreateContext());

        try (MockedStatic<AdminClient> ignored = Mockito.mockStatic(AdminClient.class)) {
            operatorSession.start(startupPromise);
        }

        startupPromise.future().toCompletionStage().toCompletableFuture().get();

        assertEquals(0, warnings.get(), "If the BlockedThreadChecker logged anything at WARN, then start-up is still blocking");
    }

    @Test
    @SuppressWarnings("unchecked")
    void ensureSlowK8sWatcherCreationDoesNotBlockThreads() throws ExecutionException, InterruptedException {
        AtomicInteger warnings = installBlockedThreadHandler();
        //Deliberately cause the k8s watcher startup to be slow
        Mockito.reset(filterWatchListDeletable);
        when(filterWatchListDeletable.watch(any())).thenAnswer(invocation -> {
            try {
                // Make sure we block long enough for blocked thread checker to run at least once or twice
                Thread.sleep(maxBlock.toMillis() * 5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return watch;
        });

        BiFunction<Zk, Config, TopicStore> topicStore = (zk, conf) -> mock(TopicStore.class);
        BiFunction<Vertx, Config, Future<Zk>> zk = (v, conf) -> Future.succeededFuture(new MockZk());

        TopicOperatorState tos = new TopicOperatorState();
        Promise<Void> startupPromise = Promise.promise();

        Session operatorSession = new Session(kubeClient, new Config(mandatoryConfig), topicStore, zk, tos);
        operatorSession.init(vertx, vertx.getOrCreateContext());

        try (MockedStatic<AdminClient> ignored = Mockito.mockStatic(AdminClient.class)) {
            operatorSession.start(startupPromise);
        }

        startupPromise.future().toCompletionStage().toCompletableFuture().get();

        assertEquals(0, warnings.get(), "If the BlockedThreadChecker logged anything at WARN, then start-up is still blocking");
    }

    private AtomicInteger installBlockedThreadHandler() {
        AtomicInteger warnings = new AtomicInteger();
        ((VertxInternal) vertx).blockedThreadChecker().setThreadBlockedHandler(event -> warnings.incrementAndGet());
        return warnings;
    }
}
