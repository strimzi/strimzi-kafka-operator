/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.platform.KubernetesVersion;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class KafkaConnectAssemblyOperatorConnectorRestartAnnoValueTest {

    /**
     * Helper class to represent a request to test an annotation value. Important to note that method to check if we have a
     * restart request is different from method to check if the annotation value is valid.
     * @param annotationValue The value of the annotation
     * @param shouldRestart Whether the operator should interpret the annotation as a restart request
     * @param shouldBeValid Whether the operator should interpret the annotation as a valid value
     * */
    record AnnotationValueRequest(String annotationValue, boolean shouldRestart, boolean shouldBeValid) { }

    @Test
    public void testRestartAnnotationValuesIsValid() {
        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(null, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                ResourceUtils.supplierWithMocks(false), ResourceUtils.dummyClusterOperatorConfig());
        for (AnnotationValueRequest annoValue : generateAnnotationValues()) {
            HasMetadata resource = new KafkaConnectBuilder()
                    .withNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_RESTART, annoValue.annotationValue))
                    .endMetadata()
                    .build();
            boolean isValid = op.restartAnnotationIsValid(resource, null);
            boolean shouldBeValid = annoValue.shouldBeValid;
            assertThat(String.format("value '%s' is expected to %s", annoValue, shouldBeValid ? "be valid" : "NOT be valid"), isValid, equalTo(shouldBeValid));
        }
    }

    @Test
    public void testRestartAnnotationValueHasRestart() {
        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(null, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                ResourceUtils.supplierWithMocks(false), ResourceUtils.dummyClusterOperatorConfig());
        for (AnnotationValueRequest annoValue : generateAnnotationValues()) {
            HasMetadata resource = new KafkaConnectBuilder()
                    .withNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_RESTART, annoValue.annotationValue))
                    .endMetadata()
                    .build();
            boolean hasRestart = op.hasRestartAnnotation(resource, null);
            boolean shouldRestart = annoValue.shouldRestart;
            assertThat(String.format("value '%s' is expected to %s", annoValue, shouldRestart ? "restart" : "NOT restart"), hasRestart, equalTo(shouldRestart));
        }
    }

    private List<AnnotationValueRequest> generateAnnotationValues() {
        return List.of(
                new AnnotationValueRequest("true", true, true),
                new AnnotationValueRequest("includeTasks,onlyFailed", true, true),
                new AnnotationValueRequest("onlyFailed,includeTasks", true, true),
                new AnnotationValueRequest("    onlyFailed   ,   includeTasks   ", true, true),
                new AnnotationValueRequest("includeTasks", true, true),
                new AnnotationValueRequest("onlyFailed", true, true),
                new AnnotationValueRequest("   onlyFailed   ", true, true),
                new AnnotationValueRequest("    includeTasks   ", true, true),
                new AnnotationValueRequest("false", false, false),
                new AnnotationValueRequest("true,includeTasks", true, false),
                new AnnotationValueRequest("true,includeTasks,onlyFailed", true, false),
                new AnnotationValueRequest("true,onlyFailed", true, false),
                new AnnotationValueRequest("true,invalidArg,onlyFailed", true, false),
                new AnnotationValueRequest("invalidArg,onlyFailed", true, false),
                new AnnotationValueRequest("onlyFailed,invalidArg", true, false),
                new AnnotationValueRequest("includeTasks,invalidArg", true, false),
                new AnnotationValueRequest("invalidArg,includeTasks", true, false),
                new AnnotationValueRequest("", false, false),
                new AnnotationValueRequest("     ", false, false),
                new AnnotationValueRequest("   b  ", true, false)
        );
    }

}
