/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LabelsTest {
    @Test
    public void testParseValidLabels()   {
        String validLabels = "key1=value1,key2=value2";

        Map sourceMap = new HashMap<String, String>(2);
        sourceMap.put("key1", "value1");
        sourceMap.put("key2", "value2");
        Labels expected = Labels.fromMap(sourceMap);

        assertThat(Labels.fromString(validLabels), is(expected));
    }

    @Test
    public void testParseNullLabels()   {
        String validLabels = null;
        assertThat(Labels.fromString(validLabels), is(Labels.EMPTY));
    }

    @Test
    public void testParseNullLabelsInFromMap()   {
        assertThat(Labels.fromMap(null), is(Labels.EMPTY));
    }

    @Test
    public void testParseNullLabelsInUserLabels()   {
        assertThat(Labels.EMPTY.withAdditionalLabels(null), is(Labels.EMPTY));
    }

    @Test
    public void testParseEmptyLabels()   {
        String validLabels = "";
        assertThat(Labels.fromString(validLabels), is(Labels.EMPTY));
    }

    @Test
    public void testParseInvalidLabels1()   {
        assertThrows(IllegalArgumentException.class, () -> {
            String invalidLabels = ",key1=value1,key2=value2";

            Labels.fromString(invalidLabels);
        });
    }

    @Test
    public void testParseInvalidLabels2()   {
        assertThrows(IllegalArgumentException.class, () -> {
            String invalidLabels = "key1=value1,key2=";

            Labels.fromString(invalidLabels);
        });
    }

    @Test
    public void testParseInvalidLabels3()   {
        assertThrows(IllegalArgumentException.class, () -> {
            String invalidLabels = "key2";

            Labels.fromString(invalidLabels);
        });
    }

    @Test
    public void testStrimziSelectorLabels()   {
        Map sourceMap = new HashMap<String, String>(5);
        sourceMap.put(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster");
        sourceMap.put("key1", "value1");
        sourceMap.put(Labels.STRIMZI_KIND_LABEL, "Kafka");
        sourceMap.put("key2", "value2");
        sourceMap.put(Labels.STRIMZI_NAME_LABEL, "my-cluster-kafka");
        sourceMap.put(Labels.STRIMZI_DISCOVERY_LABEL, "true");
        Labels labels = Labels.fromMap(sourceMap);

        Map expected = new HashMap<String, String>(2);
        expected.put(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster");
        expected.put(Labels.STRIMZI_KIND_LABEL, "Kafka");
        expected.put(Labels.STRIMZI_NAME_LABEL, "my-cluster-kafka");

        assertThat(labels.strimziSelectorLabels().toMap(), is(expected));
    }

    @Test
    public void testToLabelSelectorString()   {
        Map sourceMap = new HashMap<String, String>(4);
        sourceMap.put(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster");
        sourceMap.put(Labels.STRIMZI_KIND_LABEL, "Kafka");
        sourceMap.put(Labels.STRIMZI_NAME_LABEL, "my-cluster-kafka");
        sourceMap.put("key1", "value1");
        Labels labels = Labels.fromMap(sourceMap);

        String expected = "key1=value1," + Labels.STRIMZI_CLUSTER_LABEL + "=my-cluster," + Labels.STRIMZI_KIND_LABEL + "=Kafka," + Labels.STRIMZI_NAME_LABEL + "=my-cluster-kafka";

        assertThat(labels.toSelectorString(), is(expected));
    }

    @Test
    public void testWithUserLabels()   {
        Labels start = Labels.forStrimziCluster("my-cluster");

        Map<String, String> userLabels = new HashMap<String, String>();
        Map<String, String> expected = new HashMap<String, String>();
        expected.putAll(start.toMap());

        // null user labels
        Labels nullLabels = start.withAdditionalLabels(null, null);
        assertThat(nullLabels.toMap(), is(start.toMap()));

        // Non-null values with default exclusion
        userLabels.put("key1", "value");
        expected.putAll(userLabels);
        userLabels.put("app.kubernetes.io/instance", "value");
        Labels nonNullLabels = start.withAdditionalLabels(userLabels, null);
        assertThat(nonNullLabels.toMap(), is(expected));

        // Exclude all user Labels
        expected.remove("key1");
        Labels excludeAll = start.withAdditionalLabels(userLabels, ".*");
        assertThat(excludeAll.toMap(), is(expected));

        // Exclude user Labels that starts with test
        userLabels.put("key2", "value");
        expected.putAll(userLabels);
        userLabels.put("testkey1", "value");
        Labels excludeTestStart = start.withAdditionalLabels(userLabels, "^test.*");
        assertThat(excludeTestStart.toMap(), is(expected));
    }

    @Test
    public void testWithUserLabelsFiltersKubernetesDomainLabelsWithExceptionPartOfLabel()   {
        Labels start = Labels.forStrimziCluster("my-cluster");

        Map userLabels = new HashMap<String, String>(5);
        userLabels.put(Labels.KUBERNETES_NAME_LABEL, "kafka");
        userLabels.put("key1", "value1");
        userLabels.put(Labels.KUBERNETES_INSTANCE_LABEL, "my-cluster");
        userLabels.put(Labels.KUBERNETES_PART_OF_LABEL, "strimzi-my-cluster");
        userLabels.put("key2", "value2");
        userLabels.put(Labels.KUBERNETES_MANAGED_BY_LABEL, "my-operator");
        String validLabelContainingKubernetesDomainSubstring = "foo/" + Labels.KUBERNETES_DOMAIN;
        userLabels.put(validLabelContainingKubernetesDomainSubstring, "bar");


        // user labels should appear as if Kubernetes Domain labels are not present
        Map expectedUserLabels = new HashMap<String, String>(2);
        expectedUserLabels.put("key1", "value1");
        expectedUserLabels.put("key2", "value2");
        expectedUserLabels.put(validLabelContainingKubernetesDomainSubstring, "bar");
        expectedUserLabels.put(Labels.KUBERNETES_PART_OF_LABEL, "strimzi-my-cluster");

        Map<String, String> expected = new HashMap<String, String>();
        expected.putAll(start.toMap());
        expected.putAll(expectedUserLabels);

        Labels labels = start.withAdditionalLabels(userLabels, null);
        assertThat(labels.toMap(), is(expected));
    }

    @Test
    public void testWithInvalidUserSuppliedLabels()   {
        Map userLabelsWithStrimzi = new HashMap<String, String>(3);
        userLabelsWithStrimzi.put("key1", "value1");
        userLabelsWithStrimzi.put("key2", "value2");
        userLabelsWithStrimzi.put(Labels.STRIMZI_DOMAIN + "something", "value3");

        assertThrows(IllegalArgumentException.class, () -> Labels.EMPTY.withAdditionalLabels(userLabelsWithStrimzi));
    }

    @Test
    public void testFromResourceWithoutLabels()   {
        Kafka kafka = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("my-kafka")
                    .endMetadata()
                    .withNewSpec()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                        .withNewKafka()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                    .endSpec()
                .build();

        Labels l = Labels.fromResource(kafka);
        assertThat(l, is(Labels.EMPTY));
    }

    @Test
    public void testFromResourceWithLabels()   {
        Map<String, String> userProvidedLabels = new HashMap<String, String>(6);
        userProvidedLabels.put(Labels.KUBERNETES_NAME_LABEL, "some-app");
        userProvidedLabels.put(Labels.KUBERNETES_INSTANCE_LABEL, "my-cluster");
        userProvidedLabels.put(Labels.KUBERNETES_PART_OF_LABEL, "some-other-application-name");
        userProvidedLabels.put(Labels.KUBERNETES_MANAGED_BY_LABEL, "my-operator");
        userProvidedLabels.put("key1", "value1");
        userProvidedLabels.put("key2", "value2");

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-kafka")
                    .withLabels(userProvidedLabels)
                .endMetadata()
                .withNewSpec()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();

        Map<String, String> expectedLabels = new HashMap<String, String>(6);
        expectedLabels.putAll(userProvidedLabels);

        Labels l = Labels.fromResource(kafka);
        assertThat(l.toMap(), is(expectedLabels));
    }

    @Test
    public void testWithKubernetesLabelsCorrect() {
        String instance = "my-cluster";
        String operatorName  = "my-operator";
        String appName = "an-app";
        String appArchitecture = "app-architecture";

        Map<String, String> expectedLabels = new HashMap<>();
        expectedLabels.put(Labels.KUBERNETES_NAME_LABEL, appName);
        expectedLabels.put(Labels.KUBERNETES_INSTANCE_LABEL, instance);
        expectedLabels.put(Labels.KUBERNETES_MANAGED_BY_LABEL, operatorName);
        expectedLabels.put(Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + instance);

        Labels l = Labels.EMPTY
            .withKubernetesName(appName)
            .withKubernetesInstance(instance)
            .withKubernetesManagedBy(operatorName)
            .withKubernetesPartOf(instance);

        assertThat(l.toMap(), is(expectedLabels));
    }

    @Test
    public void testGenerateDefaultLabels() {
        Map<String, String> userProvidedLabels = new HashMap<String, String>(2);
        userProvidedLabels.put("key1", "value1");
        userProvidedLabels.put("key2", "value2");

        Map<String, String> userProvidedAnotations = new HashMap<String, String>(1);
        userProvidedAnotations.put("strimzi.io/labels-exclusion-pattern", "^key1.*");

        String instance = "my-cluster";
        String operatorName  = "my-operator";
        String appName = "an-app";
        String appArchitecture = "app-architecture";

        class ResourceWithMetadata implements HasMetadata {

            String kind;
            String apiVersion;
            ObjectMeta metadata;

            public ResourceWithMetadata(String kind, String apiVersion, ObjectMeta metadata) {
                this.kind = kind;
                this.apiVersion = apiVersion;
                this.metadata = metadata;
            }

            public ObjectMeta getMetadata() {
                return metadata;
            }
            public void setMetadata(ObjectMeta metadata) {
                this.metadata = metadata;
            }

            public String getKind() {
                return kind;
            }

            public String getApiVersion() {
                return apiVersion;
            }
            public void setApiVersion(String apiVersion) {
                this.apiVersion = apiVersion;
            }

        }

        Map<String, String> expectedLabels = new HashMap<>();
        expectedLabels.put(Labels.STRIMZI_KIND_LABEL, "MyResource");
        expectedLabels.put(Labels.STRIMZI_NAME_LABEL, Labels.APPLICATION_NAME);
        expectedLabels.put(Labels.STRIMZI_CLUSTER_LABEL, instance);
        expectedLabels.put(Labels.KUBERNETES_NAME_LABEL, appName);
        expectedLabels.put(Labels.KUBERNETES_INSTANCE_LABEL, instance);
        expectedLabels.put(Labels.KUBERNETES_MANAGED_BY_LABEL, operatorName);
        expectedLabels.put(Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + instance);
        expectedLabels.put("key2", "value2");

        Labels l = Labels.generateDefaultLabels(new ResourceWithMetadata("MyResource", "strimzi.io/v0", new ObjectMetaBuilder()
            .withNewName(instance)
            .withLabels(userProvidedLabels)
            .withAnnotations(userProvidedAnotations)
            .build()), appName, operatorName);
        assertThat(l.toMap(), is(expectedLabels));

        // Without strimzi.io/labels-exclusion-pattern annotation
        expectedLabels.put("key1", "value1");
        l = Labels.generateDefaultLabels(new ResourceWithMetadata("MyResource", "strimzi.io/v0", new ObjectMetaBuilder()
            .withNewName(instance)
            .withLabels(userProvidedLabels)
            .build()), appName, operatorName);
        assertThat(l.toMap(), is(expectedLabels));

    }

    @Test
    public void testValidInstanceNamesAsLabelValues()   {
        assertThat(Labels.getOrValidInstanceLabelValue(null), is(""));
        assertThat(Labels.getOrValidInstanceLabelValue(""), is(""));
        assertThat(Labels.getOrValidInstanceLabelValue("valid-label-value"), is("valid-label-value"));
        assertThat(Labels.getOrValidInstanceLabelValue("too-long-012345678901234567890123456789012345678901234567890123456789"), is("too-long-012345678901234567890123456789012345678901234567890123"));
        assertThat(Labels.getOrValidInstanceLabelValue("too-long-01234567890123456789012345678901234567890123456789012-456789"), is("too-long-01234567890123456789012345678901234567890123456789012"));
        assertThat(Labels.getOrValidInstanceLabelValue("too-long-01234567890123456789012345678901234567890123456789.---456789"), is("too-long-01234567890123456789012345678901234567890123456789"));
    }
}
