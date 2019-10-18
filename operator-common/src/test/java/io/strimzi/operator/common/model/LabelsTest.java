/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LabelsTest {
    @Test
    public void testParseValidLabels()   {
        String validLabels = "key1=value1,key2=value2";

        Map sourceMap = new HashMap<String, String>(2);
        sourceMap.put("key1", "value1");
        sourceMap.put("key2", "value2");
        Labels expected = Labels.fromMap(sourceMap);

        Assert.assertEquals(expected, Labels.fromString(validLabels));
    }

    @Test
    public void testParseNullLabels()   {
        String validLabels = null;
        assertEquals(Labels.EMPTY, Labels.fromString(validLabels));
    }

    @Test
    public void testParseNullLabelsInFromMap()   {
        assertEquals(Labels.EMPTY, Labels.fromMap(null));
    }

    @Test
    public void testParseNullLabelsInUserLabels()   {
        assertEquals(Labels.EMPTY, Labels.userLabels(null));
    }

    @Test
    public void testParseEmptyLabels()   {
        String validLabels = "";
        assertEquals(Labels.EMPTY, Labels.fromString(validLabels));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidLabels1()   {
        String invalidLabels = ",key1=value1,key2=value2";

        Labels.fromString(invalidLabels);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidLabels2()   {
        String invalidLabels = "key1=value1,key2=";

        Labels.fromString(invalidLabels);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidLabels3()   {
        String invalidLabels = "key2";

        Labels.fromString(invalidLabels);
    }

    @Test
    public void testStrimziLabels()   {
        Map sourceMap = new HashMap<String, String>(5);
        sourceMap.put(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster");
        sourceMap.put("key1", "value1");
        sourceMap.put(Labels.STRIMZI_KIND_LABEL, "Kafka");
        sourceMap.put("key2", "value2");
        sourceMap.put(Labels.STRIMZI_NAME_LABEL, "my-cluster-kafka");
        Labels labels = Labels.fromMap(sourceMap);

        Map expected = new HashMap<String, String>(2);
        expected.put(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster");
        expected.put(Labels.STRIMZI_KIND_LABEL, "Kafka");
        expected.put(Labels.STRIMZI_NAME_LABEL, "my-cluster-kafka");

        assertEquals(expected, labels.strimziLabels().toMap());
    }

    @Test
    public void testWithUserLabels()   {
        Labels start = Labels.forCluster("my-cluster");

        // null user labels
        Labels nullLabels = start.withUserLabels(null);
        assertEquals(start.toMap(), nullLabels.toMap());

        // Non-null values
        Map userLabels = new HashMap<String, String>(2);
        userLabels.put("key1", "value1");
        userLabels.put("key2", "value2");

        Map<String, String> expected = new HashMap<String, String>();
        expected.putAll(start.toMap());
        expected.putAll(userLabels);

        Labels nonNullLabels = start.withUserLabels(userLabels);
        assertEquals(expected, nonNullLabels.toMap());
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
        
        // user labels should appear as if Kubernetes Domain labels are not present
        Map expectedUserLabels = new HashMap<String, String>(2);
        expectedUserLabels.put("key1", "value1");
        expectedUserLabels.put("key2", "value2");

        Map<String, String> expected = new HashMap<String, String>();
        expected.putAll(start.toMap());
        expected.putAll(expectedUserLabels);

        Labels labels = start.withUserLabels(userLabels);
        assertEquals(expected, labels.toMap());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithInvalidUserLabels()   {
        Map userLabelsWithStrimzi = new HashMap<String, String>(2);
        userLabelsWithStrimzi.put("key1", "value1");
        userLabelsWithStrimzi.put("key2", "value2");
        userLabelsWithStrimzi.put("strimzi.io/something", "value3");

        Labels nonNullLabels = Labels.EMPTY.withUserLabels(userLabelsWithStrimzi);
    }
}
