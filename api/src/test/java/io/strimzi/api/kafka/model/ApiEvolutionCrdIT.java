/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.io.IOException;
import java.io.StringWriter;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionBuilder;
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
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ApiEvolutionCrdIT extends AbstractCrdIT {
    private static final Logger LOGGER = LogManager.getLogger(ApiEvolutionCrdIT.class);

    public static final String NAMESPACE = "api-evolution-it";
    //public static final YAMLMapper YAML_MAPPER = new YAMLMapper();

    @Test
    public void kafkaApiEvolution() throws IOException, InterruptedException {
        assumeKube1_16Plus();
        try {
            /* Strimzi 0.20
             * v1alpha1 exists but is no longer served, there is no v1beta2
             * users upgrade their instances
             */

            // Create CRD with v1beta1 having map-or-list listeners (and no v1beta2)
            LOGGER.info("Phase 1 : Create CRD");
            CustomResourceDefinition crdPhase1 = new CrdV1Beta1Builder()
                    .withVersions(V1ALPHA1, V1BETA1)
                    .withServedVersions(V1BETA1_PLUS)
                    .withStorageVersion(V1BETA1)
                    .createOrReplace();
            Thread.sleep(5_000);
            waitForCrdUpdate(crdPhase1.getMetadata().getGeneration());

            // Create one CR instance with a list listener and one with a map listeners
            LOGGER.info("Phase 1 : Create instances");
            final String nameA = "instance.a";
            final String nameB = "instance.b";
            Kafka instanceA = v1beta1Create(nameA, mapListener(), null);
            Kafka instanceB = v1beta1Create(nameB, null, listListener());

            // Check we can consume these via v1beta1 endpoint
            LOGGER.info("Phase 1 : Assert instances via v1beta1");
            assertIsMapListener(v1beta1Get(nameA));
            assertIsListListener(v1beta1Get(nameB));

            // Upgrade instance A to use a list listener
            v1beta1Op().withName(nameA).replace(new KafkaBuilder(instanceA).editSpec().editKafka().withListeners(
                    new ArrayOrObjectKafkaListeners(instanceA.getSpec().getKafka().getListeners().newOrConverted()))
                    .endKafka().endSpec().build());

            /* Strimzi 0.21
             * v1beta2 gets added, and v1beta2 is stored
             * users touch all instances, so there's nothing stored at v1beta1
             */

            // Replace CRD with one having v1beta2 which is served but not stored (v1beta1 is stored)
            LOGGER.info("Phase 2 : Replace CRD, removing v1alpha1, adding v1beta2");
            CustomResourceDefinition crdPhase2 = new CrdV1Beta1Builder()
                    .withVersions(V1BETA1, V1BETA2)
                    .withServedVersions(V1BETA1_PLUS)
                    .withStorageVersion(V1BETA2)
                    .createOrReplace();
            waitForCrdUpdate(crdPhase2.getMetadata().getGeneration());
            assertEquals("v1beta2", crdPhase2.getSpec().getVersions().stream()
                    .filter(v -> v.getStorage()).map(v -> v.getName()).findFirst().get());

            // Check we can't create a v1beta2 with a map via the v1beta2 endpoint
            assertV1beta2CreateFailure("not.valid");

            // Check we can still create a v1beta1 with list via the v1beta1 endpoint
            final String nameC = "instance.c";
            Kafka instanceC = v1beta2Create(nameC, null, listListener());

            LOGGER.info("Phase 2 : Upgrading all instances to new stored version");
            instanceA = touchV1Beta2(nameA);
            instanceB = touchV1Beta2(nameB);
            instanceC = touchV1Beta2(nameC);

            LOGGER.info("Phase 2 : Assert instances via both endpoints");
            assertIsListListener(v1beta1Get(nameA));
            assertIsListListener(v1beta1Get(nameB));
            assertIsListListener(v1beta1Get(nameC));
            assertIsListListener(v1beta2Get(nameA));
            assertIsListListener(v1beta2Get(nameB));
            assertIsListListener(v1beta2Get(nameC));

            LOGGER.info("Phase 2 : Updating CRD so v1beta1 has served=false");

            CustomResourceDefinition crdPhase2Part2 = cluster.client().getClient().apiextensions().v1().customResourceDefinitions()
                    .createOrReplace(new CustomResourceDefinitionBuilder(crdPhase2).editSpec()
                            .editLastVersion().withServed(false).endVersion().endSpec().build());

            CustomResourceDefinition crdPhase2Part3 = waitForCrdUpdate(crdPhase2Part2.getMetadata().getGeneration());

            LOGGER.info("Phase 2 : Updating CRD status.stored versions = v1beta2");
            CustomResourceDefinition crdPhase2Part4 = cluster.client().getClient().apiextensions().v1().customResourceDefinitions()
                    .updateStatus(new CustomResourceDefinitionBuilder(crdPhase2Part3).editStatus().withStoredVersions(asList("v1beta2")).endStatus().build());

            assertEquals(asList("v1beta2"), crdPhase2Part4.getStatus().getStoredVersions());
            assertIsListListener(v1beta2Get(nameA));
            assertIsListListener(v1beta2Get(nameB));
            assertIsListListener(v1beta2Get(nameC));

            // Strimzi 0.22

            // Upgrade CRD so v1beta2 is stored, and v1beta1 is not served
            LOGGER.info("Phase 3 : Update CRD so v1beta2 is stored");
            io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition crdPhase3 = new CrdV1Builder()
                    .withVersions(V1BETA2)
                    .withServedVersions(ApiVersion.V1BETA2_PLUS)
                    .withStorageVersion(V1BETA2)
                    .createOrReplace();
            waitForCrdUpdate(crdPhase3.getMetadata().getGeneration());
            assertEquals("v1beta2", crdPhase3.getSpec().getVersions().stream()
                    .filter(v -> v.getStorage()).map(v -> v.getName()).findFirst().get());
            assertEquals(asList("v1beta2"), crdPhase3.getStatus().getStoredVersions());

            // Check we can still consume all CRs via v1beta2 endpoint
            LOGGER.info("Assert instances via v1beta2 endpoint");
            assertIsListListener(v1beta2Get(nameA));
            assertIsListListener(v1beta2Get(nameB));
            assertIsListListener(v1beta2Get(nameC));
        } finally {
            deleteCrd();
        }

    }

    public Kafka touchV1Beta2(String name) {
        Kafka build = new KafkaBuilder(v1beta2Get(name))
                .withApiVersion("kafka.strimzi.io/v1beta2")
                .build();
        build = v1beta2Op().withName(name).replace(build);
        return build;
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

    private Kafka v1beta1Create(String name, KafkaListeners kafkaListeners, GenericKafkaListener o) {
        return v1beta1Op().create(buildKafkaCr(Kafka.V1BETA1, name, kafkaListeners, o));
    }

    private Kafka v1beta2Create(String name, KafkaListeners kafkaListeners, GenericKafkaListener o) {
        return v1beta2Op().create(buildKafkaCr(Kafka.V1BETA2, name, kafkaListeners, o));
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

    private CustomResourceDefinition waitForCrdUpdate(long crdGeneration2) {
        TestUtils.waitFor("CRD update", 1000, 30000, () ->
                crdGeneration2 == cluster.client().getClient().apiextensions().v1().customResourceDefinitions()
                        .withName("kafkas.kafka.strimzi.io").get()
                        .getMetadata().getGeneration());
        return cluster.client().getClient().apiextensions().v1().customResourceDefinitions()
                .withName("kafkas.kafka.strimzi.io").get();
    }

    abstract class Builder<Crd extends HasMetadata, Self extends Builder<Crd, Self>> {
        protected VersionRange<ApiVersion> servedVersions;
        protected ApiVersion storageVersion;
        protected ApiVersion[] versions;

        public Self withServedVersions(VersionRange<ApiVersion> apiVersions) {
            this.servedVersions = apiVersions;
            return self();
        }

        public Self withStorageVersion(ApiVersion apiVersion) {
            this.storageVersion = apiVersion;
            return self();
        }

        public Self withVersions(ApiVersion... apiVersions) {
            this.versions = apiVersions;
            return self();
        }

        protected abstract Self self();

        abstract Crd createOrReplace() throws IOException;
    }

    class CrdV1Beta1Builder extends Builder<CustomResourceDefinition, CrdV1Beta1Builder> {

        private CustomResourceDefinition build() throws IOException {
            StringWriter sw = new StringWriter();
            new CrdGenerator(KubeVersion.V1_16_PLUS, V1BETA1, CrdGenerator.YAML_MAPPER, emptyMap(),
                    new CrdGenerator.DefaultReporter(), asList(versions), storageVersion,
                    servedVersions,
                    new CrdGenerator.NoneConversionStrategy()).generate(Kafka.class, sw);
            return CrdGenerator.YAML_MAPPER.readValue(sw.toString(), CustomResourceDefinition.class);
        }

        @Override
        protected CrdV1Beta1Builder self() {
            return this;
        }

        @Override
        public CustomResourceDefinition createOrReplace() throws IOException {
            CustomResourceDefinition build = build();
            return cluster.client().getClient().apiextensions().v1().customResourceDefinitions()
                    .createOrReplace(build);
        }

    }

    class CrdV1Builder extends Builder<io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition, CrdV1Builder> {

        private io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition build() throws IOException {
            StringWriter sw = new StringWriter();
            new CrdGenerator(KubeVersion.V1_16_PLUS, V1, CrdGenerator.YAML_MAPPER, emptyMap(),
                    new CrdGenerator.DefaultReporter(), asList(versions), storageVersion, servedVersions,
                    new CrdGenerator.NoneConversionStrategy()).generate(Kafka.class, sw);
            return CrdGenerator.YAML_MAPPER.readValue(sw.toString(), io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition.class);
        }

        @Override
        protected CrdV1Builder self() {
            return this;
        }

        @Override
        public io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition createOrReplace() throws IOException {
            return cluster.client().getClient().adapt(V1ApiextensionsAPIGroupClient.class).customResourceDefinitions()
                    .createOrReplace(build());
        }
    }

    private void deleteCrd() {
        KubernetesClientException ex = null;
        try {
            cluster.client().getClient().apiextensions().v1().customResourceDefinitions()
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
        ArrayOrObjectKafkaListeners listeners = kafkaSpec.getListeners();
        assertNotNull(listeners);
        assertNotNull(listeners.getKafkaListeners());
        assertNull(listeners.getGenericKafkaListeners());
        assertNotNull(kafkaSpec.getConfig());
        assertEquals("someValue", kafkaSpec.getConfig().get("some.kafka.config"));
    }

    private void assertIsListListener(Kafka kafka) {
        assertNotNull(kafka);
        assertNotNull(kafka.getSpec());
        KafkaClusterSpec kafkaSpec = kafka.getSpec().getKafka();
        assertNotNull(kafkaSpec);
        ArrayOrObjectKafkaListeners listeners = kafkaSpec.getListeners();
        assertNotNull(listeners);
        assertNull(listeners.getKafkaListeners());
        assertNotNull(listeners.getGenericKafkaListeners());
        assertNotNull(kafkaSpec.getConfig());
        assertEquals("someValue", kafkaSpec.getConfig().get("some.kafka.config"));
    }

    private NonNamespaceOperation<Kafka, KafkaList, Resource<Kafka>> v1beta1Op() {
        return Crds.kafkaV1Beta1Operation(cluster.client().getClient()).inNamespace(NAMESPACE);
    }

    private NonNamespaceOperation<Kafka, KafkaList, Resource<Kafka>> v1beta2Op() {
        return Crds.kafkaV1Beta2Operation(cluster.client().getClient()).inNamespace(NAMESPACE);
    }

    private NonNamespaceOperation<Kafka, KafkaList, Resource<Kafka>> v1Op() {
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
                        .withListeners(listListeners != null ? new ArrayOrObjectKafkaListeners(singletonList(listListeners)) : new ArrayOrObjectKafkaListeners(mapListeners))
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
