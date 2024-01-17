/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplateBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class ServiceAccountUtilsTest {
    private final static String NAME = "my-sa";
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
    public void testServiceAccountCreationWithNullTemplate() {
        ServiceAccount sa = ServiceAccountUtils.createServiceAccount(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null);

        assertThat(sa.getMetadata().getName(), is(NAME));
        assertThat(sa.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(sa.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(sa.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(sa.getMetadata().getAnnotations(), is(nullValue()));
    }

    @ParallelTest
    public void testServiceAccountCreationWithEmptyTemplate() {
        ServiceAccount sa = ServiceAccountUtils.createServiceAccount(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, new ResourceTemplate());

        assertThat(sa.getMetadata().getName(), is(NAME));
        assertThat(sa.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(sa.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(sa.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(sa.getMetadata().getAnnotations(), is(nullValue()));
    }

    @ParallelTest
    public void testServiceAccountCreationWithTemplate() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                    .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                    .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                .endMetadata()
                .build();

        ServiceAccount sa = ServiceAccountUtils.createServiceAccount(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, template);

        assertThat(sa.getMetadata().getName(), is(NAME));
        assertThat(sa.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(sa.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(sa.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(sa.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));
    }
}
