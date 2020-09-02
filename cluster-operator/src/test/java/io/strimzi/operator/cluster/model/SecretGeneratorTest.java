/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

class SecretGeneratorTest {

    final static String NAME = "secret-name";
    final static String NAMESPACE = "my-namespace";
    final static Labels LABELS = Labels.EMPTY.withStrimziName("component");
    final static Map<String, String> LABELS_AS_MAP = LABELS.toMap();
    final static OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withName("my-cr")
            .withKind("my-kind")
            .build();
    final static HashMap<String, String> DATA = new HashMap<>(2);
    static {
        DATA.put("key1", "val1");
    }

    @Test
    void testCreate() {
        Secret secret = SecretGenerator.create(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, DATA);
        assertThat(secret, is(notNullValue()));
        assertThat(secret.getMetadata().getName(), is(NAME));
        assertThat(secret.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(secret.getMetadata().getLabels(), is(LABELS_AS_MAP));
        assertThat(secret.getMetadata().getOwnerReferences(), is(Arrays.asList(OWNER_REFERENCE)));
        assertThat(secret.getData(), is(DATA));
    }

    @Test
    void testCreateWithoutOwnerReference() {
        Secret secret = SecretGenerator.create(NAME, NAMESPACE, LABELS, null, DATA);
        assertThat(secret, is(notNullValue()));
        assertThat(secret.getMetadata().getName(), is(NAME));
        assertThat(secret.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(secret.getMetadata().getLabels(), is(LABELS_AS_MAP));
        assertThat(secret.getMetadata().getOwnerReferences(), hasSize(0));
        assertThat(secret.getData(), is(DATA));
    }
}