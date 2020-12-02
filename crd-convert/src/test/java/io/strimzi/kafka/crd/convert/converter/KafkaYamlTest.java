/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.Kafka;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;

class KafkaYamlTest {

    private static final boolean DEBUG = Boolean.getBoolean("crd.debug");

    KafkaConverter kafkaConverter() {
        return new KafkaConverter();
    }

    @Test
    public void testYaml() throws Exception {
        URL oldYaml = getClass().getResource("old-config.yaml");
        Assertions.assertNotNull(oldYaml);

        ObjectMapper mapper = new YAMLMapper();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = new PrintWriter(baos);
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);

        JsonNode root = mapper.readTree(oldYaml);

        if (DEBUG) {
            System.out.println("Before JSON ----\n\n");
            mapper.writeTree(generator, root);
            System.out.println(baos);
        }

        kafkaConverter().convertTo(root, ApiVersion.V1BETA2);

        if (DEBUG) {
            baos.reset();
            System.out.println("\n\nAfter JSON ----\n\n");
            mapper.writeTree(generator, root);
            System.out.println(baos);
        }

        URL newYaml = getClass().getResource("new-config.yaml");
        Assertions.assertNotNull(newYaml);
        baos.reset();
        Files.copy(new File(newYaml.toURI()).toPath(), baos);

        if (DEBUG) {
            System.out.println("\n\nExpected ----\n\n");
            System.out.println(baos);
        }

        Kafka kafka = mapper.readValue(oldYaml, Kafka.class);
        kafkaConverter().convertTo(kafka, ApiVersion.V1BETA2);
        byte[] bytes = mapper.writeValueAsBytes(kafka);
        JsonNode node = mapper.readTree(bytes);

        // compare root (JSON) vs. node (Kafka instance)
        Assertions.assertEquals(root, node);

        node = mapper.readTree(baos.toByteArray());
        // compare root (JSON) vs. new-config.yaml file
        Assertions.assertEquals(root, node);
    }
}