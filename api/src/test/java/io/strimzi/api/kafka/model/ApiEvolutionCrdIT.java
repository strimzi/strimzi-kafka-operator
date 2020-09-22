/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.V1ApiextensionsAPIGroupClient;
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
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.crdgenerator.CrdGenerator;
import io.strimzi.api.annotations.KubeVersion;
import io.strimzi.api.annotations.VersionRange;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.strimzi.api.annotations.ApiVersion.V1;
import static io.strimzi.api.annotations.ApiVersion.V1ALPHA1;
import static io.strimzi.api.annotations.ApiVersion.V1BETA1;
import static io.strimzi.api.annotations.ApiVersion.V1BETA1_PLUS;
import static io.strimzi.api.annotations.ApiVersion.V1BETA2;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ApiEvolutionCrdIT extends AbstractCrdIT {
    private static final Logger LOGGER = LogManager.getLogger(ApiEvolutionCrdIT.class);

    public static final String NAMESPACE = "api-evolution-it";
    //public static final YAMLMapper YAML_MAPPER = new YAMLMapper();

    @Test
    public void kafkaApiEvolution() throws IOException, InterruptedException {
        assumeKube1_16Plus();
        try {
            // Strimzi 0.20: v1alpha1 exists but is no longer served, there is no v1beta2

            // Create CRD with v1beta1 having map-or-list listeners (and no v1beta2)
            LOGGER.info("Create CRD");
            long crdGeneration = createOrReplaceCrd(V1BETA1)
                    .withVersions(V1ALPHA1, V1BETA1)
                    .withServedVersions(V1BETA1_PLUS)
                    .withStorageVersion(V1BETA1)
                    .createOrReplace();
            Thread.sleep(5_000);
            waitForCrdUpdate(crdGeneration);

            // Create one CR instance with a list listener and one with a map listeners
            LOGGER.info("Create instances");
            v1beta1Create(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1), mapListener(), null);
            v1beta1Create(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1), null, listListener());

            // Check we can consume these via v1beta1 endpoint
            LOGGER.info("Assert instances via v1beta1");
            assertIsMapListener(v1beta1Get(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta1Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));

            // Strimzi 0.21: v1beta2 gets added, but v1beta1 is stored
            // If we're assuming the None strategy what would the upgrade process be?
            // Stop the operator
            // Install a CRD with beta1(served,stored) and beta2(served)
            // Convert CR instances to v1beta2 form
            // Update the CRD so beta1(served) and beta2(served,stored)
            // Convert CR instances to v1beta2 apiVersion
            // Update the CRD so beta1(not served) and beta2(served,stored)
            // Start the operator

            // this is safe for clients of v1beta1 API because every v1beta2 instance is a valid v1beta1
            // it's not safe for clients of v1beta2 API because v1beta1 instances might not be valid v1beta2

            // Replace CRD with one having v1beta2 which is served but not stored (v1beta1 is stored)
            LOGGER.info("Replace CRD, removing v1alpha1, adding v1beta2");
            long crdGeneration2 = createOrReplaceCrd(V1BETA1)
                    .withVersions(V1BETA1, V1BETA2)
                    .withServedVersions(V1BETA1_PLUS)
                    .withStorageVersion(V1BETA1)
                    .createOrReplace();
            waitForCrdUpdate(crdGeneration2);

            // Check we can't create a v1beta2 with a map via the v1beta2 endpoint
            assertV1beta2CreateFailure(name(V1BETA1, V1BETA2, false, V1BETA1, V1BETA2));


            // Check we can still create a v1beta1 with map via the v1beta1 endpoint
            v1beta2Create(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1) + ".v2", null, listListener());

            // Check we can create a v1beta2 with list via the v1beta2 endpoint
            LOGGER.info("Create 3rd instance via v1beta2 endpoint");
            v1beta2Create(name(V1BETA1, V1BETA2, true, V1BETA1, V1BETA2), null, listListener());

            // Check we can still consume all CRs via both endpoints
            LOGGER.info("Assert instances via both endpoints");
            assertIsMapListener(v1beta1Get(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta1Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta1Get(name(V1BETA1, V1BETA2, true, V1BETA1, V1BETA2)));
            assertIsMapListener(v1beta2Get(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta2Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta2Get(name(V1BETA1, V1BETA2, true, V1BETA1, V1BETA2)));

            // Strimzi 0.22

            // Upgrade CRD so v1beta2 is stored, and v1beta1 is not served
            LOGGER.info("Update CRD so v1beta2 is stored");
            long crdGeneration3 = createOrReplaceCrd(V1BETA1)
                    .withVersions(V1BETA1, V1BETA2)
                    .withServedVersions(ApiVersion.V1BETA2_PLUS)
                    .withStorageVersion(V1BETA2)
                    .createOrReplace();
            waitForCrdUpdate(crdGeneration3);

            // Check we can still consume all CRs via both endpoints
            LOGGER.info("Assert instances via both endpoints");
            assertIsMapListener(v1beta1Get(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta1Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta1Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            assertIsMapListener(v1beta2Get(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta2Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta2Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));

            // Check we can still create/update v1beta1 endpoint with a map listeners
            v1beta1Create("v1beta1.map.v1beta2.stored.via.v1beta1.endpoint", mapListener(), null);
            // But we can't via the v1beta2 endpoint
            assertV1beta2CreateFailure("v1beta2.map.v1beta2.stored.via.v1beta2.endpoint");
            // But lists are still OK
            v1beta1Create("v1beta1.list.v1beta2.stored.via.v1beta1.endpoint", null, listListener());
            v1beta2Create("v1beta2.list.v1beta2.stored.via.v1beta2.endpoint", null, listListener());

            // Strimzi 0.23
            LOGGER.info("Update CRD to use CRDv1");
            long crdGeneration4 = createOrReplaceCrd(V1)
                    .withVersions(V1BETA2)
                    .withServedVersions(ApiVersion.V1BETA2_PLUS)
                    .withStorageVersion(V1BETA2)
                    .createOrReplace();
            waitForCrdUpdate(crdGeneration4);
            // Can't get via endpoint which doesn't exist
            assertThrows(IllegalArgumentException.class, () -> v1beta1Get(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1)));
            assertThrows(IllegalArgumentException.class, () -> v1beta1Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            assertThrows(IllegalArgumentException.class, () -> v1beta1Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            // Can get via v1beta2 endpoint, even though the stored version for the map one is not schema valid
            assertIsMapListener(v1beta2Get(name(V1BETA1, V1BETA1, false, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta2Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            assertIsListListener(v1beta2Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
            // Can't create via endpoint which doesn't exist
            assertThrows(IllegalArgumentException.class, () -> v1beta1Create("v1beta1.list.v1beta2.stored.via.v1beta1.endpoint", null, listListener()));
            assertThrows(IllegalArgumentException.class, () -> v1beta2Create("v1beta1.list.v1beta2.stored.via.v1beta1.endpoint", null, listListener()));
            v1Create("v1beta1.list.v1beta2.stored.via.v1beta1.endpoint", null, listListener());
            assertIsListListener(v1Get(name(V1BETA1, V1BETA1, true, V1BETA1, V1BETA1)));
        } finally {
            deleteCrd();
        }

    }

    private String name(ApiVersion crdApi, ApiVersion crVersion, boolean list, ApiVersion stored, ApiVersion endpoint) {
        return String.format("crd.%s.cr.%s.%s.stored.%s.via.%s",
                crdApi, crVersion, list ? "list" : "map", stored, endpoint);
    }

    private void assertV1beta2CreateFailure(String name) {
        LOGGER.info("Check can't create map-listener via v1beta2");
        KubernetesClientException e = Assertions.assertThrows(KubernetesClientException.class, () -> v1beta2Create(name, mapListener(), null));
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

    private void v1Create(String name, KafkaListeners kafkaListeners, GenericKafkaListener o) {
        v1Op().create(buildKafkaCr(Kafka.V1BETA2, name, kafkaListeners, o));
    }

    private Kafka v1beta1Get(String s) {
        return v1beta1Op().withName(s).get();
    }

    private Kafka v1beta2Get(String s) {
        return v1beta2Op().withName(s).get();
    }

    private Kafka v1Get(String s) {
        return v1Op().withName(s).get();
    }

    private void waitForCrdUpdate(long crdGeneration2) {
        TestUtils.waitFor("CRD update", 1000, 30000, () ->
                crdGeneration2 == cluster.client().getClient().customResourceDefinitions()
                        .withName("kafkas.kafka.strimzi.io").get()
                        .getMetadata().getGeneration());
    }

    private Builder createOrReplaceCrd(ApiVersion expectedCrdApiVersion) throws IOException {
        if (expectedCrdApiVersion.equals(V1)) {
            return new CrdV1Builder();
        } else {
            return new CrdV1Beta1Builder();
        }
    }

    abstract class Builder {
        protected VersionRange<ApiVersion> servedVersions;
        protected ApiVersion storageVersion;
        protected ApiVersion[] versions;

        public Builder withServedVersions(VersionRange<ApiVersion> apiVersions) {
            this.servedVersions = apiVersions;
            return this;
        }

        public Builder withStorageVersion(ApiVersion apiVersion) {
            this.storageVersion = apiVersion;
            return this;
        }

        public Builder withVersions(ApiVersion... apiVersions) {
            this.versions = apiVersions;
            return this;
        }

        abstract Long createOrReplace() throws IOException;
    }

    class CrdV1Beta1Builder extends Builder {

        private CustomResourceDefinition build() throws IOException {
            StringWriter sw = new StringWriter();
            new CrdGenerator(KubeVersion.V1_16_PLUS, V1BETA1, CrdGenerator.YAML_MAPPER, emptyMap(),
                    new CrdGenerator.DefaultReporter(), Arrays.asList(versions), storageVersion,
                    servedVersions,
                    new CrdGenerator.NoneConversionStrategy()).generate(Kafka.class, sw);
            return CrdGenerator.YAML_MAPPER.readValue(sw.toString(), CustomResourceDefinition.class);
        }

        public Long createOrReplace() throws IOException {
            CustomResourceDefinition build = build();
            return cluster.client().getClient().customResourceDefinitions()
                    .createOrReplace(build).getMetadata().getGeneration();
        }

    }

    class CrdV1Builder extends Builder {

        private io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition build() throws IOException {
            StringWriter sw = new StringWriter();
            new CrdGenerator(KubeVersion.V1_16_PLUS, V1, CrdGenerator.YAML_MAPPER, emptyMap(),
                    new CrdGenerator.DefaultReporter(), Arrays.asList(versions), storageVersion, servedVersions,
                    new CrdGenerator.NoneConversionStrategy()).generate(Kafka.class, sw);
            return CrdGenerator.YAML_MAPPER.readValue(sw.toString(), io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition.class);
        }

        public Long createOrReplace() throws IOException {
            return cluster.client().getClient().adapt(V1ApiextensionsAPIGroupClient.class).customResourceDefinitions()
                    .createOrReplace(build()).getMetadata().getGeneration();
        }
    }

    private void deleteCrd() {
        KubernetesClientException ex = null;
        try {
            cluster.client().getClient().customResourceDefinitions()
                    .withName("kafkas.kafka.strimzi.io")
                    .delete();
        } catch (KubernetesClientException e) {
            ex = e;
            try {
                cluster.client().getClient().adapt(V1ApiextensionsAPIGroupClient.class).customResourceDefinitions()
                        .withName("kafkas.kafka.strimzi.io")
                        .delete();
            } catch (KubernetesClientException e2) {
                if (ex == null) {
                    ex = e2;
                }
            }
        }
        if (ex != null) {
            throw ex;
        }
    }

    private void assertIsMapListener(Kafka kafka) {
        assertNotNull(kafka);
        assertNotNull(kafka.getSpec());
        KafkaClusterSpec kafkaSpec = kafka.getSpec().getKafka();
        assertNotNull(kafkaSpec);
        assertNotNull(kafkaSpec.getListeners());
        assertNotNull(kafkaSpec.getListeners().getKafkaListeners());
        assertNull(kafkaSpec.getListeners().getGenericKafkaListeners());
        assertNotNull(kafkaSpec.getConfig());
        assertEquals("someValue", kafkaSpec.getConfig().get("some.kafka.config"));
    }

    private void assertIsListListener(Kafka kafka) {
        assertNotNull(kafka);
        assertNotNull(kafka.getSpec());
        KafkaClusterSpec kafkaSpec = kafka.getSpec().getKafka();
        assertNotNull(kafkaSpec);
        assertNotNull(kafkaSpec.getListeners());
        assertNull(kafkaSpec.getListeners().getKafkaListeners());
        assertNotNull(kafkaSpec.getListeners().getGenericKafkaListeners());
        assertNotNull(kafkaSpec.getConfig());
        assertEquals("someValue", kafkaSpec.getConfig().get("some.kafka.config"));
    }

    private NonNamespaceOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> v1beta1Op() {
        return Crds.kafkaV1Beta1Operation(cluster.client().getClient()).inNamespace(NAMESPACE);
    }

    private NonNamespaceOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> v1beta2Op() {
        return Crds.kafkaV1Beta2Operation(cluster.client().getClient()).inNamespace(NAMESPACE);
    }

    private NonNamespaceOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> v1Op() {
        return Crds.kafkaV1Operation(cluster.client().getClient()).inNamespace(NAMESPACE);
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
                        .withListeners(listListeners == null ? new ArrayOrObjectKafkaListeners(singletonList(listListeners)) : new ArrayOrObjectKafkaListeners(mapListeners))
                        .addToConfig("some.kafka.config", "someValue")
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
    void teardownEnvironment() {
        cluster.deleteNamespaces();
    }
}
