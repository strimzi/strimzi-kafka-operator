/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;

import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaImplTest {

    private final Vertx vertx = Vertx.vertx();

    static class Either<Left, Right> {
        private final boolean isLeft;
        private final Left left;
        private final Right right;

        private Either(boolean isLeft, Left left, Right right) {
            this.isLeft = isLeft;
            this.left = left;
            this.right = right;
        }

        public static <Left, Right> Either<Left, Right> ofLeft(Left left) {
            return new Either<>(true, left, null);
        }

        public static <Left, Right> Either<Left, Right> ofRight(Right right) {
            return new Either<>(false, null, right);
        }

        public boolean isLeft() {
            return isLeft;
        }

        public Left left() {
            if (isLeft) {
                return left;
            } else {
                throw new RuntimeException();
            }
        }

        public Right right() {
            if (isLeft) {
                throw new RuntimeException();
            } else {
                return right;
            }
        }
    }

    private void mockDescribeConfigs(Admin admin, Map<ConfigResource, Either<Config, Exception>> result) {
        DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
        when(describeConfigsResult.values()).thenReturn(result.entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            entry -> {
                KafkaFutureImpl<Config> kafkaFuture = new KafkaFutureImpl<>();
                if (entry.getValue().isLeft()) {
                    kafkaFuture.complete(entry.getValue().left());
                } else {
                    kafkaFuture.completeExceptionally(entry.getValue().right());
                }
                return kafkaFuture;
            })));
        when(admin.describeConfigs(result.keySet())).thenReturn(describeConfigsResult);
    }

    private void mockCreateTopicsValidateOnly(Admin admin, NewTopic topic, Exception result) {
        CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        when(createTopicsResult.all()).then(invocation -> {
            KafkaFutureImpl<Void> kafkaFuture1 = new KafkaFutureImpl<>();
            if (result == null) {
                kafkaFuture1.complete(null);
            } else {
                kafkaFuture1.completeExceptionally(result);
            }
            return kafkaFuture1;
        });
        when(admin.createTopics(eq(singleton(topic)),
                argThat(isValidateOnly()))).thenReturn(createTopicsResult);
    }

    private ArgumentMatcher<CreateTopicsOptions> isValidateOnly() {
        return CreateTopicsOptions::shouldValidateOnly;
    }

    private void mockDescribeTopics(Admin admin, Map<String, Either<TopicDescription, Exception>> result) {
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.topicNameValues()).thenReturn(result.entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            entry1 -> {
                KafkaFutureImpl<TopicDescription> kafkaFuture1 = new KafkaFutureImpl<>();
                if (entry1.getValue().isLeft()) {
                    kafkaFuture1.complete(entry1.getValue().left());
                } else {
                    kafkaFuture1.completeExceptionally(entry1.getValue().right());
                }
                return kafkaFuture1;
            })));
        Optional<Either<TopicDescription, Exception>> first = result.values().stream().filter(either -> !either.isLeft()).findFirst();
        if (first.isPresent()) {
            KafkaFutureImpl<Map<String, TopicDescription>> kafkaFuture = new KafkaFutureImpl<>();
            kafkaFuture.completeExceptionally(first.get().right());
            when(describeTopicsResult.allTopicNames()).thenReturn(kafkaFuture);
        } else {
            when(describeTopicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                result.entrySet().stream().collect(toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().left()))
            ));
        }
        when(admin.describeTopics(result.keySet())).thenReturn(describeTopicsResult);
    }

    private void mockDeleteTopics(Admin admin, Map<String, Either<Void, Exception>> result) {
        DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        when(deleteTopicsResult.topicNameValues()).thenReturn(result.entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            entry -> {
                KafkaFutureImpl<Void> kafkaFuture = new KafkaFutureImpl<>();
                if (entry.getValue().isLeft()) {
                    kafkaFuture.complete(null);
                } else {
                    kafkaFuture.completeExceptionally(entry.getValue().right());
                }
                return kafkaFuture;
            })));
        when(admin.deleteTopics(result.keySet())).thenReturn(deleteTopicsResult);
    }


    @Test
    public void testTopicMetadataBothNotFound(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockCreateTopicsValidateOnly(admin, new NewTopic("test", 1, (short) 1), null);
        mockDescribeTopics(admin, singletonMap("test", Either.ofRight(new UnknownTopicOrPartitionException())));
        mockDescribeConfigs(admin, singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                Either.ofRight(new UnknownTopicOrPartitionException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(Reconciliation.DUMMY_RECONCILIATION, new TopicName("test")).onComplete(testContext.succeeding(topicMetadata -> testContext.verify(() -> {
            assertNull(topicMetadata);
            testContext.completeNow();
        })));
    }

    @Test
    public void testTopicMetadataDescribeTopicNotFound(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockCreateTopicsValidateOnly(admin, new NewTopic("test", 1, (short) 1), null);
        mockDescribeTopics(admin, singletonMap("test", Either.ofRight(new UnknownTopicOrPartitionException())));
        mockDescribeConfigs(admin, singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                Either.ofLeft(mock(Config.class))));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(Reconciliation.DUMMY_RECONCILIATION, new TopicName("test")).onComplete(testContext.succeeding(topicMetadata -> testContext.verify(() -> {
            assertNull(topicMetadata);
            testContext.completeNow();
        })));
    }

    @Test
    public void testTopicMetadataDescribeConfigsNotFound(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockCreateTopicsValidateOnly(admin, new NewTopic("test", 1, (short) 1), null);
        mockDescribeTopics(admin, singletonMap("test", Either.ofLeft(mock(TopicDescription.class))));
        mockDescribeConfigs(admin, singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                Either.ofRight(new UnknownTopicOrPartitionException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(Reconciliation.DUMMY_RECONCILIATION, new TopicName("test")).onComplete(testContext.succeeding(topicMetadata -> testContext.verify(() -> {
            assertNull(topicMetadata);
            testContext.completeNow();
        })));
    }

    @Test
    public void testTopicMetadataBothFound(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockCreateTopicsValidateOnly(admin, new NewTopic("test", 1, (short) 1), new TopicExistsException(""));
        mockDescribeTopics(admin, singletonMap("test", Either.ofLeft(mock(TopicDescription.class))));
        mockDescribeConfigs(admin, singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                Either.ofLeft(mock(Config.class))));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(Reconciliation.DUMMY_RECONCILIATION, new TopicName("test")).onComplete(testContext.succeeding(topicMetadata -> testContext.verify(() -> {
            assertNotNull(topicMetadata);
            assertNotNull(topicMetadata.getDescription());
            assertNotNull(topicMetadata.getConfig());
            testContext.completeNow();
        })));
    }

    @Test
    public void testTopicMetadataDescribeTimeout(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockCreateTopicsValidateOnly(admin, new NewTopic("test", 1, (short) 1), new TopicExistsException(""));
        mockDescribeTopics(admin, singletonMap("test", Either.ofLeft(mock(TopicDescription.class))));
        mockDescribeConfigs(admin, singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                    Either.ofRight(new TimeoutException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(Reconciliation.DUMMY_RECONCILIATION, new TopicName("test")).onComplete(testContext.failing(error -> testContext.verify(() -> {
            assertTrue(error instanceof TimeoutException);
            testContext.completeNow();
        })));
    }

    @Test
    public void testDelete(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockDescribeTopics(admin, singletonMap("test", Either.ofRight(new UnknownTopicOrPartitionException())));
        mockDeleteTopics(admin, singletonMap("test", Either.ofLeft(null)));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.deleteTopic(Reconciliation.DUMMY_RECONCILIATION, new TopicName("test"))
                .onComplete(testContext.succeeding(error ->
                        testContext.verify(testContext::completeNow)));
    }

    @Test
    public void testDeleteDeleteTimeout(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockDescribeTopics(admin, singletonMap("test", Either.ofRight(new UnknownTopicOrPartitionException())));
        mockDeleteTopics(admin, singletonMap("test", Either.ofRight(new TimeoutException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.deleteTopic(Reconciliation.DUMMY_RECONCILIATION, new TopicName("test")).onComplete(testContext.failing(error -> testContext.verify(() -> {
            assertTrue(error instanceof TimeoutException);
            testContext.completeNow();
        })));
    }
}

