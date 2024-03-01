/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import io.strimzi.api.kafka.model.AbstractCrdTest;
import io.strimzi.api.kafka.model.common.template.MetadataTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
/**
 * The purpose of this test is to ensure:
 *
 * 1. we get a correct tree of POJOs when reading a JSON/YAML `Kafka` resource.
 */
public class KafkaConnectTest extends AbstractCrdTest<KafkaConnect> {

    public KafkaConnectTest() {
        super(KafkaConnect.class);
    }

    @Test
    public void testStorage() {
        KafkaConnectSpec kafkaConnectSpec = new KafkaConnectSpec();
        SingleVolumeStorage storage = new SingleVolumeStorage() {
            @Override
            public String getType() {
                return "ephemeral";
            }
        };
        storage.setId(1);

        kafkaConnectSpec.setStorage(storage);
        assertNotNull(kafkaConnectSpec.getStorage());
        assertSame(storage, kafkaConnectSpec.getStorage());
        assertEquals(1, kafkaConnectSpec.getStorage().getId());
        assertEquals("ephemeral", kafkaConnectSpec.getStorage().getType());
    }

    @Test
    public void testPersistentVolumeClaim() {
        KafkaConnectTemplate kafkaConnectTemplate = new KafkaConnectTemplate();
        ResourceTemplate resourceTemplate = new ResourceTemplate();

        MetadataTemplate metadataTemplate = new MetadataTemplate();

        Map<String, String> labels = new HashMap<>();
        labels.put("label1", "value1");
        labels.put("label2", "value2");
        metadataTemplate.setLabels(labels);

        Map<String, String> annotations = new HashMap<>();
        annotations.put("annotation1", "value1");
        annotations.put("annotation2", "value2");
        metadataTemplate.setAnnotations(annotations);

        resourceTemplate.setMetadata(metadataTemplate);

        kafkaConnectTemplate.setPersistentVolumeClaim(resourceTemplate);

        ResourceTemplate result = kafkaConnectTemplate.getPersistentVolumeClaim();

        assertNotNull(result);
        assertSame(resourceTemplate, result);

        assertEquals(metadataTemplate, result.getMetadata());
        assertEquals(labels, result.getMetadata().getLabels());
        assertEquals(annotations, result.getMetadata().getAnnotations());
    }
}
