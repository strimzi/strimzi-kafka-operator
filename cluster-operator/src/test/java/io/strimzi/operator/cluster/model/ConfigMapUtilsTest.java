/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class ConfigMapUtilsTest {
    private final static String NAME = "my-cm";
    private final static String NAMESPACE = "my-namespace";
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("my-kind")
            .withName("my-name")
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-name")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type")
            .withAdditionalLabels(Map.of("label-1", "value-1", "label-2", "value-2"));

    @ParallelTest
    public void testConfigMapCreation() {
        ConfigMap cm = ConfigMapUtils.createConfigMap(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, Map.of("key1", "value1", "key2", "value2"));

        assertThat(cm.getMetadata().getName(), is(NAME));
        assertThat(cm.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(cm.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(cm.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(cm.getMetadata().getAnnotations(), is(Map.of()));

        assertThat(cm.getData(), is(Map.of("key1", "value1", "key2", "value2")));
    }
}
