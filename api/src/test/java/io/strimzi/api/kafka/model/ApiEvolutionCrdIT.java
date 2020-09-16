/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.KafkaListenersBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;

public class ApiEvolutionCrdIT extends AbstractCrdIT {
    private static final Logger LOGGER = LogManager.getLogger(ApiEvolutionCrdIT.class);

    public static final String NAMESPACE = "api-evolution-it";

    @Test
    public void kafkaApiEvolution() throws IOException {
        assumeKube1_16Plus();
        // Create CRD with v1beta1 having map-or-list listeners (and no v1beta2)
        LOGGER.info("Create CRD");
        long crdGeneration = createOrReplaceCrd("src/test/resources/io/strimzi/api/kafka/model/040-Crd-kafka-v1alpha1-v1beta1-store-v1beta1.yaml");
        waitForCrd("crd", "kafkas.kafka.strimzi.io");

        // Create one CR instance with a list listener and one with a map listeners
        LOGGER.info("Create instances");
        v1beta1Create("v1beta1.map.v1beta1.stored.via.v1beta1.endpoint", mapListener(), null);
        v1beta1Create("v1beta1.list.v1beta1.stored.via.v1beta1.endpoint", null, listListener());

        // Check we can consume these via v1beta1 endpoint
        LOGGER.info("Assert instances via v1beta1");
        assertIsMapListener(v1beta1Get("v1beta1.map.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta1Get("v1beta1.list.v1beta1.stored.via.v1beta1.endpoint"));

        // Replace CRD with one having v1beta2 which is served but not stored (v1beta1 is stored)
        LOGGER.info("Replace CRD");
        long crdGeneration2 = createOrReplaceCrd("src/test/resources/io/strimzi/api/kafka/model/040-Crd-kafka-v1beta1-v1beta2-store-v1beta1.yaml");
        waitForCrdUpdate(crdGeneration2);

        // Check we can't create a v1beta2 with a map
        assertV1beta2CreateFailure("v1beta2.map.v1beta1.stored.via.v1beta2.endpoint");

        // Create a v1beta2 with list
        LOGGER.info("Create 3rd instance via v1beta2 endpoint");
        v1beta2Create("v1beta2.list.v1beta1.stored.via.v1beta1.endpoint", null, listListener());

        // Check we can still consume all CRs via both endpoints
        LOGGER.info("Assert instances via both endpoints");
        assertIsMapListener(v1beta1Get("v1beta1.map.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta1Get("v1beta1.list.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta1Get("v1beta2.list.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsMapListener(v1beta2Get("v1beta1.map.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta2Get("v1beta1.list.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta2Get("v1beta2.list.v1beta1.stored.via.v1beta1.endpoint"));

        // Upgrade CRD so v1beta2 is stored
        LOGGER.info("Update CRD so v1beta2 is stored");
        long crdGeneration3 = createOrReplaceCrd("src/test/resources/io/strimzi/api/kafka/model/040-Crd-kafka-v1beta1-v1beta2-store-v1beta2.yaml");
        waitForCrdUpdate(crdGeneration3);

        // Check we can still consume all CRs via both endpoints
        LOGGER.info("Assert instances via both endpoints");
        assertIsMapListener(v1beta1Get("v1beta1.map.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta1Get("v1beta1.list.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta1Get("v1beta1.list.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsMapListener(v1beta2Get("v1beta1.map.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta2Get("v1beta1.list.v1beta1.stored.via.v1beta1.endpoint"));
        assertIsListListener(v1beta2Get("v1beta1.list.v1beta1.stored.via.v1beta1.endpoint"));

        // Check we can still create/update v1beta1 endpoint with a map listeners
        v1beta1Create("v1beta1.map.v1beta2.stored.via.v1beta1.endpoint", mapListener(), null);
        // But we can't via the v1beta2 endpoint
        assertV1beta2CreateFailure("v1beta2.map.v1beta2.stored.via.v1beta2.endpoint");
        // But lists are still OK
        v1beta1Create("v1beta1.list.v1beta2.stored.via.v1beta1.endpoint", null, listListener());
        v1beta2Create("v1beta2.list.v1beta2.stored.via.v1beta2.endpoint", null, listListener());
    }

    private void assertV1beta2CreateFailure(String name) {
        LOGGER.info("Check can't create map-listener via v1beta2");
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> v1beta2Create(name, mapListener(), null));
        LOGGER.info("Exception, good", e);
        Assertions.assertTrue(e.getMessage().contains(
                "Kafka.kafka.strimzi.io \"" + name + "\" is invalid: " +
                "spec.kafka.listeners: Invalid value: \"object\": " +
                "spec.kafka.listeners in body must be of type array:"));
    }

    private void v1beta1Create(String name, KafkaListeners kafkaListeners, GenericKafkaListener o) {
        v1beta1Op().create(buildKafkaCr(Kafka.V1BETA1, name, kafkaListeners, o));
    }

    private void v1beta2Create(String name, KafkaListeners kafkaListeners, GenericKafkaListener o) {
        v1beta2Op().create(buildKafkaCr(Kafka.V1BETA2, name, kafkaListeners, o));
    }

    private Kafka v1beta1Get(String s) {
        return v1beta1Op().withName(s).get();
    }

    private Kafka v1beta2Get(String s) {
        return v1beta2Op().withName(s).get();
    }

    private void waitForCrdUpdate(long crdGeneration2) {
        TestUtils.waitFor("CRD update", 1000, 30000, () ->
                crdGeneration2 == cluster.client().getClient().customResourceDefinitions()
                        .withName("kafkas.kafka.strimzi.io").get()
                        .getMetadata().getGeneration());
    }

    private Long createOrReplaceCrd(String s) throws IOException {
        File src = new File(s);
        if (!src.exists()) {
            throw new RuntimeException(src.getAbsolutePath() + " does not exist");
        }
        CustomResourceDefinition crd = new YAMLMapper().readValue(src, CustomResourceDefinition.class);
        LOGGER.info("Create or replacing {} with versions {}", crd.getMetadata().getName(),
                crd.getSpec().getVersions().stream()
                        .map(v -> v.getName() + "{stored=" + v.getStorage() + "}")
                        .collect(Collectors.joining(", ")));
        return cluster.client().getClient().customResourceDefinitions().createOrReplace(crd).getMetadata().getGeneration();
    }

    private void deleteCrd(String s) throws IOException {
        File src = new File(s);
        if (!src.exists()) {
            throw new RuntimeException(src.getAbsolutePath() + " does not exist");
        }
        CustomResourceDefinition crd = new YAMLMapper().readValue(src, CustomResourceDefinition.class);
        LOGGER.info("Create or replacing {} with versions {}", crd.getMetadata().getName(),
                crd.getSpec().getVersions().stream()
                        .map(v -> v.getName() + "{stored=" + v.getStorage() + "}")
                        .collect(Collectors.joining(", ")));
        cluster.client().getClient().customResourceDefinitions().delete(crd);
    }

    private void assertIsMapListener(Kafka kafka) {
        Assertions.assertNotNull(kafka);
        Assertions.assertNotNull(kafka.getSpec());
        Assertions.assertNotNull(kafka.getSpec().getKafka());
        Assertions.assertNotNull(kafka.getSpec().getKafka().getListeners());
        Assertions.assertNotNull(kafka.getSpec().getKafka().getListeners().getKafkaListeners());
        Assertions.assertNull(kafka.getSpec().getKafka().getListeners().getGenericKafkaListeners());
    }

    private void assertIsListListener(Kafka kafka) {
        Assertions.assertNotNull(kafka);
        Assertions.assertNotNull(kafka.getSpec());
        Assertions.assertNotNull(kafka.getSpec().getKafka());
        Assertions.assertNotNull(kafka.getSpec().getKafka().getListeners());
        Assertions.assertNull(kafka.getSpec().getKafka().getListeners().getKafkaListeners());
        Assertions.assertNotNull(kafka.getSpec().getKafka().getListeners().getGenericKafkaListeners());
    }

    private NonNamespaceOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> v1beta1Op() {
        return Crds.kafkaV1Beta1Operation(cluster.client().getClient()).inNamespace(NAMESPACE);
    }

    private NonNamespaceOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> v1beta2Op() {
        return Crds.kafkaV1Beta2Operation(cluster.client().getClient()).inNamespace(NAMESPACE);
    }

    private GenericKafkaListener listListener() {
        return new GenericKafkaListenerBuilder().withType(KafkaListenerType.INTERNAL).withName("plain").withPort(9092).withTls(false).build();
    }

    private KafkaListeners mapListener() {
        return new KafkaListenersBuilder().withNewPlain().endPlain().build();
    }

    private Kafka buildKafkaCr(String apiVersion, String name, KafkaListeners mapListeners, GenericKafkaListener listListeners) {
        if ((mapListeners == null) == (listListeners == null)) {
            throw new IllegalArgumentException("Exactly one of mapListeners or listListeners must be non-null");
        }
        return new KafkaBuilder().withApiVersion(apiVersion)
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withNewEphemeralStorage().endEphemeralStorage()
                        .withListeners(new ArrayOrObjectKafkaListeners(listListeners == null ? null : singletonList(listListeners), mapListeners))
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                        .build();
    }


    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        //waitForCrd("crd", "kafkas.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() throws IOException {
        deleteCrd("src/test/resources/io/strimzi/api/kafka/model//040-Crd-kafka-v1alpha1-v1beta1-store-v1beta1.yaml");
        cluster.deleteNamespaces();
    }
}
