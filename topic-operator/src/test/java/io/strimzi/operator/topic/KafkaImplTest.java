/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.util.Map;
import java.util.Optional;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
            return new Either(true, left, null);
        }

        public static <Left, Right> Either<Left, Right> ofRight(Right right) {
            return new Either(false, null, right);
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
            entry -> entry.getKey(),
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

    private void mockDescribeTopics(Admin admin, Map<String, Either<TopicDescription, Exception>> result) {
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.values()).thenReturn(result.entrySet().stream().collect(toMap(
            entry1 -> entry1.getKey(),
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
            when(describeTopicsResult.all()).thenReturn(kafkaFuture);
        } else {
            when(describeTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(
                result.entrySet().stream().collect(toMap(
                    entry -> entry.getKey(),
                    entry -> entry.getValue().left()))
            ));
        }
        when(admin.describeTopics(result.keySet())).thenReturn(describeTopicsResult);
    }

    private void mockDeleteTopics(Admin admin, Map<String, Either<Void, Exception>> result) {
        DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        when(deleteTopicsResult.values()).thenReturn(result.entrySet().stream().collect(toMap(
            entry -> entry.getKey(),
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
        mockDescribeTopics(admin, singletonMap("test", Either.ofRight(new UnknownTopicOrPartitionException())));
        mockDescribeConfigs(admin, singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                Either.ofRight(new UnknownTopicOrPartitionException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(new TopicName("test")).onComplete(testContext.succeeding(topicMetadata -> testContext.verify(() -> {
            assertNull(topicMetadata);
            testContext.completeNow();
        })));
    }

    @Test
    public void testTopicMetadataDescribeTopicNotFound(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockDescribeTopics(admin, singletonMap("test", Either.ofRight(new UnknownTopicOrPartitionException())));
        mockDescribeConfigs(admin, singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                Either.ofLeft(mock(Config.class))));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(new TopicName("test")).onComplete(testContext.succeeding(topicMetadata -> testContext.verify(() -> {
            assertNull(topicMetadata);
            testContext.completeNow();
        })));
    }

    @Test
    public void testTopicMetadataDescribeConfigsNotFound(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockDescribeTopics(admin, singletonMap("test", Either.ofLeft(mock(TopicDescription.class))));
        mockDescribeConfigs(admin, singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                Either.ofRight(new UnknownTopicOrPartitionException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(new TopicName("test")).onComplete(testContext.succeeding(topicMetadata -> testContext.verify(() -> {
            assertNull(topicMetadata);
            testContext.completeNow();
        })));
    }

    @Test
    public void testTopicMetadataBothFound(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockDescribeTopics(admin, singletonMap("test", Either.ofLeft(mock(TopicDescription.class))));
        mockDescribeConfigs(admin, singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                Either.ofLeft(mock(Config.class))));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(new TopicName("test")).onComplete(testContext.succeeding(topicMetadata -> testContext.verify(() -> {
            assertNotNull(topicMetadata);
            assertNotNull(topicMetadata.getDescription());
            assertNotNull(topicMetadata.getConfig());
            testContext.completeNow();
        })));
    }

    @Test
    public void testTopicMetadataDescribeTimeout(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockDescribeTopics(admin, singletonMap("test", Either.ofLeft(mock(TopicDescription.class))));
        mockDescribeConfigs(admin, singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, "test"),
                    Either.ofRight(new TimeoutException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.topicMetadata(new TopicName("test")).onComplete(testContext.failing(error -> testContext.verify(() -> {
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
        impl.deleteTopic(new TopicName("test")).onComplete(testContext.succeeding(error -> testContext.verify(() -> {
            testContext.completeNow();
        })));
    }

    @Test
    public void testDeleteDeleteTimeout(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockDescribeTopics(admin, singletonMap("test", Either.ofRight(new UnknownTopicOrPartitionException())));
        mockDeleteTopics(admin, singletonMap("test", Either.ofRight(new TimeoutException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.deleteTopic(new TopicName("test")).onComplete(testContext.failing(error -> testContext.verify(() -> {
            assertTrue(error instanceof TimeoutException);
            testContext.completeNow();
        })));
    }

    @Test
    public void testDeleteDescribeTimeout(VertxTestContext testContext) {
        Admin admin = mock(Admin.class);
        mockDeleteTopics(admin, singletonMap("test", Either.ofLeft(null)));
        mockDescribeTopics(admin, singletonMap("test", Either.ofRight(new TimeoutException())));

        KafkaImpl impl = new KafkaImpl(admin, vertx);
        impl.deleteTopic(new TopicName("test")).onComplete(testContext.failing(error -> testContext.verify(() -> {
            assertTrue(error instanceof TimeoutException);
            testContext.completeNow();
        })));
    }
}

