/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.stores;

import io.strimzi.operator.topic.Topic;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaPersistentKeyValueStoreTest {

    private final String persistenceTopic = "x";
    @Mock
    Admin admin;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    Consumer<String, Topic> loadingConsumer;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    Producer<String, Topic> storeWriteProducer;

    //Signifies when topic is completely read from start to end
    private final ConsumerRecords<String, Topic> emptyResult = new ConsumerRecords<>(Map.of());
    private final String groupId = "mock-loader";

    private final Topic topicA = new Topic.Builder("topic-a", 1).build();
    private final Topic topicB = new Topic.Builder("topic-b", 2).build();
    private final Topic topicC = new Topic.Builder("topic-c", 3).build();

    @BeforeEach
    void setup() {
        when(loadingConsumer.groupMetadata().groupId()).thenReturn(groupId);
        when(loadingConsumer.poll(any(Duration.class))).thenReturn(emptyResult);
    }

    @Test
    void testInitialLoad() {
        List<ConsumerRecord<String, Topic>> storedTopics = List.of(
                new ConsumerRecord<>(persistenceTopic, 1, 0, "topic-a", topicA),
                new ConsumerRecord<>(persistenceTopic, 1, 1, "topic-b", topicB),
                new ConsumerRecord<>(persistenceTopic, 1, 2, "topic-c", topicC)
        );

        ConsumerRecords<String, Topic> initialRecords = new ConsumerRecords<>(Map.of(new TopicPartition(persistenceTopic, 1), storedTopics));

        when(loadingConsumer.poll(any(Duration.class))).thenReturn(initialRecords).thenReturn(emptyResult);
        KafkaPersistentKeyValueStore store = new KafkaPersistentKeyValueStore(persistenceTopic, admin, x -> loadingConsumer, x -> storeWriteProducer, Map.of());

        assertEquals(topicA, store.get("topic-a"));
        assertEquals(topicB, store.get("topic-b"));
        assertEquals(topicC, store.get("topic-c"));
        assertNull(store.get("unknown-topic"));

        //Load clears up after itself
        verify(loadingConsumer).close();
        verify(admin).deleteConsumerGroups(List.of(groupId));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testPut() {
        KafkaPersistentKeyValueStore store = new KafkaPersistentKeyValueStore(persistenceTopic, admin, x -> loadingConsumer, x -> storeWriteProducer, Map.of());
        store.put("topic-a", topicA);
        store.put("topic-b", topicB);

        List<ProducerRecord<String, Topic>> expectedRecordWrites = List.of(
                new ProducerRecord<>(persistenceTopic, "topic-a", topicA),
                new ProducerRecord<>(persistenceTopic, "topic-b", topicB)
        );

        ArgumentCaptor<ProducerRecord<String, Topic>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(storeWriteProducer, times(2)).send(captor.capture());

        assertEquals(expectedRecordWrites, captor.getAllValues());
        assertEquals(topicA, store.get("topic-a"));
        assertEquals(topicB, store.get("topic-b"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testPutIfAbsent() {
        ConsumerRecords<String, Topic> initialRecords = new ConsumerRecords<>(Map.of(new TopicPartition(persistenceTopic, 1), List.of(
                new ConsumerRecord<>(persistenceTopic, 1, 0, "topic-a", topicA)
        )));
        when(loadingConsumer.poll(any(Duration.class))).thenReturn(initialRecords).thenReturn(emptyResult);

        List<ProducerRecord<String, Topic>> expectedRecordWrites = List.of(new ProducerRecord<>(persistenceTopic, "topic-b", topicB));
        ArgumentCaptor<ProducerRecord<String, Topic>> captor = ArgumentCaptor.forClass(ProducerRecord.class);

        KafkaPersistentKeyValueStore store = new KafkaPersistentKeyValueStore(persistenceTopic, admin, x -> loadingConsumer, x -> storeWriteProducer, Map.of());

        //Assert contract of putIfAbsent
        assertEquals(topicA, store.putIfAbsent("topic-a", topicA));
        assertNull(store.putIfAbsent("topic-b", topicB));

        verify(storeWriteProducer, times(1)).send(captor.capture());
        assertEquals(expectedRecordWrites, captor.getAllValues());

        assertEquals(topicA, store.get("topic-a"));
        assertEquals(topicB, store.get("topic-b"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testDelete() {
        List<ProducerRecord<String, Topic>> expectedRecordWrites = List.of(
                new ProducerRecord<>(persistenceTopic, "topic-a", topicA),
                new ProducerRecord<>(persistenceTopic, "topic-a", null) //tombstone record written on delete
        );

        ArgumentCaptor<ProducerRecord<String, Topic>> captor = ArgumentCaptor.forClass(ProducerRecord.class);

        KafkaPersistentKeyValueStore store = new KafkaPersistentKeyValueStore(persistenceTopic, admin, x -> loadingConsumer, x -> storeWriteProducer, Map.of());

        store.put("topic-a", topicA);
        assertEquals(topicA, store.get("topic-a"));

        store.delete("topic-a");
        assertNull(store.get("topic-a"));

        verify(storeWriteProducer, times(2)).send(captor.capture());
        assertEquals(expectedRecordWrites, captor.getAllValues());
    }

}