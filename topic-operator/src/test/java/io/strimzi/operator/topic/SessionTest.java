/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import kafka.zookeeper.ZooKeeperClientAuthFailedException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoSession;

import java.util.Map;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(VertxExtension.class)
class SessionTest {

    @Mock
    KubernetesClient kubeClient;

    Map<String, String> mandatoryConfig = Map.of(
            Config.ZOOKEEPER_CONNECT.key, "localhost:2181",
            Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092",
            Config.NAMESPACE.key, "default",
            Config.CLIENT_ID.key, "default-client-id"
    );

    MockitoSession mockitoSession;

    @BeforeEach
    void setup() {
        mockitoSession = Mockito.mockitoSession().initMocks(this).startMocking();
    }

    @AfterEach
    void teardown() {
        mockitoSession.finishMocking();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testZKFailureEndsStartup(Vertx vertx, VertxTestContext testContext) {

        Config config = new Config(mandatoryConfig);
        BiFunction<Zk, Config, TopicStore> mockStoreCreator = Mockito.mock(BiFunction.class);

        // Unfortunately, Mockito's capacity to mock statics is limited to the current thread, and as Vert.x calls
        // the static ZK::create method in another thread, I have introduced a level of indirection to be able to
        // test the session
        BiFunction<Vertx, Config, Future<Zk>> failingZkCreation = (v, conf) -> Future.failedFuture(new ZooKeeperClientAuthFailedException("bad"));

        TopicOperatorState tos = new TopicOperatorState();
        Promise<Void> startupPromise = Promise.promise();

        Session operatorSession = new Session(kubeClient, config, mockStoreCreator, failingZkCreation, tos);
        operatorSession.init(vertx, vertx.getOrCreateContext());

        // Luckily, the admin client's static factory method _is_ called in this thread
        try (MockedStatic<AdminClient> ignored = Mockito.mockStatic(AdminClient.class)) {
            operatorSession.start(startupPromise);
        }

        startupPromise.future().onComplete(startupResult -> testContext.verify(() -> {
            assertTrue(startupResult.failed(), "Startup promise is set to failed if ZK client creation failed");
            assertFalse(tos.isAlive(), "Topic Operator isn't alive if ZK creation failed");
            assertDoesNotThrow(() -> verify(mockStoreCreator, never()).apply(any(Zk.class), any(Config.class)), "store creator was never interacted with as ZK failed first");
            testContext.completeNow();
        }));
    }

    @Test
    public void testTopicStoreFailureEndsStartup(Vertx vertx, VertxTestContext testContext) {

        Config config = new Config(mandatoryConfig);
        BiFunction<Vertx, Config, Future<Zk>> zkCreation = (v, conf) -> Future.succeededFuture(mock(Zk.class));
        BiFunction<Zk, Config, TopicStore> failingStoreCreator = (z, c) -> {
            throw new InvalidStateStoreException("I couldn't find your store");
        };

        TopicOperatorState tos = new TopicOperatorState();
        Promise<Void> startupPromise = Promise.promise();

        Session operatorSession = new Session(kubeClient, config, failingStoreCreator, zkCreation, tos);
        operatorSession.init(vertx, vertx.getOrCreateContext());

        try (MockedStatic<AdminClient> ignored = Mockito.mockStatic(AdminClient.class)) {
            operatorSession.start(startupPromise);
        }

        startupPromise.future().onComplete(startupResult -> testContext.verify(() -> {
            assertTrue(startupResult.failed(), "Startup promise is set to failed if topic store creation failed");
            assertFalse(tos.isAlive(), "Topic Operator isn't alive if topic store creation failed");
            testContext.completeNow();
        }));
    }

}