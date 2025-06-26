/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KubernetesResourceOperatorUtilsTest {
    private static final List<Predicate<String>> IGNORELIST = List.of(annotation -> annotation.startsWith("foo"));

    @Test
    public void testCurrentAnnotationsAreNull() {
        Pod current = new PodBuilder()
                .withNewMetadata()
                    .withName("my-pod")
                .endMetadata()
                .build();
        Pod desired = new PodBuilder()
                .withNewMetadata()
                    .withName("my-pod")
                    .withAnnotations(Map.of("avfc", "1874"))
                .endMetadata()
                .build();

        KubernetesResourceOperatorUtils.patchAnnotations(current, desired, IGNORELIST);
        assertThat(desired.getMetadata().getAnnotations(), is(Map.of("avfc", "1874")));
    }

    @Test
    public void testNonMatchingAnnotationsAreRemoved() {
        Pod current = new PodBuilder()
                .withNewMetadata()
                    .withName("my-pod")
                    .withAnnotations(Map.of("baz", "value", "bar", "value2"))
                .endMetadata()
                .build();
        Pod desired = new PodBuilder()
                .withNewMetadata()
                    .withName("my-pod")
                    .withAnnotations(Map.of("avfc", "1874"))
                .endMetadata()
                .build();

        KubernetesResourceOperatorUtils.patchAnnotations(current, desired, IGNORELIST);
        assertThat(desired.getMetadata().getAnnotations(), is(Map.of("avfc", "1874")));
    }

    @Test
    public void testMatchingAnnotationsAreIgnored() {
        Pod current = new PodBuilder()
                .withNewMetadata()
                    .withName("my-pod")
                    .withAnnotations(Map.of("foo", "value", "bar", "value2"))
                .endMetadata()
                .build();
        Pod desired = new PodBuilder()
                .withNewMetadata()
                    .withName("my-pod")
                    .withAnnotations(Map.of("avfc", "1874"))
                .endMetadata()
                .build();

        KubernetesResourceOperatorUtils.patchAnnotations(current, desired, IGNORELIST);
        assertThat(desired.getMetadata().getAnnotations(), is(Map.of("avfc", "1874", "foo", "value")));
    }
}
