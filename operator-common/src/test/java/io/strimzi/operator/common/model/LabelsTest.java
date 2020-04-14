/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;

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
        assertThat(Labels.userLabels(null), is(Labels.EMPTY));
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
    public void testWithUserLabels()   {
        Labels start = Labels.forCluster("my-cluster");

        // null user labels
        Labels nullLabels = start.withUserLabels(null);
        assertThat(nullLabels.toMap(), is(start.toMap()));

        // Non-null values
        Map userLabels = new HashMap<String, String>(2);
        userLabels.put("key1", "value1");
        userLabels.put("key2", "value2");

        Map<String, String> expected = new HashMap<String, String>();
        expected.putAll(start.toMap());
        expected.putAll(userLabels);

        Labels nonNullLabels = start.withUserLabels(userLabels);
        assertThat(nonNullLabels.toMap(), is(expected));
    }

    @Test
    public void testWithUserLabelsFiltersKubernetesDomainLabels()   {
        Labels start = Labels.forCluster("my-cluster");

        Map userLabels = new HashMap<String, String>(5);
        userLabels.put(Labels.KUBERNETES_NAME_LABEL, Labels.KUBERNETES_NAME);
        userLabels.put("key1", "value1");
        userLabels.put(Labels.KUBERNETES_INSTANCE_LABEL, "my-cluster");
        userLabels.put("key2", "value2");
        userLabels.put(Labels.KUBERNETES_MANAGED_BY_LABEL, "my-operator");
        userLabels.put(Labels.KUBERNETES_PART_OF_LABEL, "strimzi-my-cluster");
        String validLabelContainingKubernetesDomainSubstring = "foo/" + Labels.KUBERNETES_DOMAIN;
        userLabels.put(validLabelContainingKubernetesDomainSubstring, "bar");


        // user labels should appear as if Kubernetes Domain labels are not present
        Map expectedUserLabels = new HashMap<String, String>(2);
        expectedUserLabels.put("key1", "value1");
        expectedUserLabels.put("key2", "value2");
        // Kubernetes part-of is not filtered and allowed to be set by user
        expectedUserLabels.put(Labels.KUBERNETES_PART_OF_LABEL, "strimzi-my-cluster");
        expectedUserLabels.put(validLabelContainingKubernetesDomainSubstring, "bar");


        Map<String, String> expected = new HashMap<String, String>();
        expected.putAll(start.toMap());
        expected.putAll(expectedUserLabels);

        Labels labels = start.withUserLabels(userLabels);
        assertThat(labels.toMap(), is(expected));
    }

    @Test
    public void testWithInvalidUserLabels()   {
        assertThrows(IllegalArgumentException.class, () -> {
            Map userLabelsWithStrimzi = new HashMap<String, String>(2);
            userLabelsWithStrimzi.put("key1", "value1");
            userLabelsWithStrimzi.put("key2", "value2");
            userLabelsWithStrimzi.put(Labels.STRIMZI_DOMAIN + "something", "value3");

            Labels nonNullLabels = Labels.EMPTY.withUserLabels(userLabelsWithStrimzi);
        });
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
        Map<String, String> userLabels = new HashMap<String, String>(5);
        userLabels.put(Labels.KUBERNETES_NAME_LABEL, Labels.KUBERNETES_NAME);
        userLabels.put(Labels.KUBERNETES_INSTANCE_LABEL, "my-cluster");
        userLabels.put(Labels.KUBERNETES_PART_OF_LABEL, "strimzi-my-cluster");
        userLabels.put(Labels.KUBERNETES_MANAGED_BY_LABEL, "my-operator");
        userLabels.put("key1", "value1");
        userLabels.put("key2", "value2");

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-kafka")
                    .withLabels(userLabels)
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

        Map<String, String> expectedLabels = new HashMap<String, String>(2);
        expectedLabels.put("key1", "value1");
        expectedLabels.put("key2", "value2");
        expectedLabels.put(Labels.KUBERNETES_PART_OF_LABEL, "strimzi-my-cluster");

        Labels l = Labels.fromResource(kafka);
        assertThat(l.toMap(), is(expectedLabels));
    }

    @Test
    public void testWithKubernetesLabelsCorrect() {
        String instance = "my-cluster";
        String operatorName  = "my-operator";
        String appName = "strimzi";

        Map<String, String> expectedLabels = new HashMap<>();
        expectedLabels.put(Labels.KUBERNETES_NAME_LABEL, appName);
        expectedLabels.put(Labels.KUBERNETES_INSTANCE_LABEL, instance);
        expectedLabels.put(Labels.KUBERNETES_MANAGED_BY_LABEL, operatorName);
        expectedLabels.put(Labels.KUBERNETES_PART_OF_LABEL, Labels.KUBERNETES_NAME + "-" + instance);

        Labels l = Labels.EMPTY
                .withKubernetesName()
                .withKubernetesInstance(instance)
                .withKubernetesManagedBy(operatorName)
                .withKubernetesPartOf(instance);

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
