/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.platform.KubernetesVersion;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class KafkaMirrorMaker2AssemblyOperatorConnectorRestartAnnoValueTest {

    /**
     * Helper class to represent a request to test an annotation value. Important to note that method to check if we have a
     * restart request is different from method to check if the annotation value is valid.
     *
     * @param annotationValue The value of the annotation
     * @param connectorName The name of the connector to restart
     * @param shouldRestart Whether the operator should interpret the annotation as a restart request
     * @param shouldBeValid Whether the operator should interpret the annotation as a valid value
     * */
    record AnnotationValueRequest(String annotationValue, String connectorName,  boolean shouldRestart, boolean shouldBeValid) { }

    @Test
    public void testRestartAnnotationValuesIsValid() {
        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(null, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                ResourceUtils.supplierWithMocks(false), ResourceUtils.dummyClusterOperatorConfig());
        for (AnnotationValueRequest annoValue : generateAnnotationValues()) {
            HasMetadata resource = new KafkaMirrorMaker2Builder()
                    .withNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR, annoValue.annotationValue))
                    .endMetadata()
                    .build();
            boolean isValid = op.restartAnnotationIsValid(resource, annoValue.connectorName);
            boolean shouldBeValid = annoValue.shouldBeValid;
            assertThat(String.format("value '%s' is expected to %s", annoValue, shouldBeValid ? "be valid" : "NOT be valid"), isValid, equalTo(shouldBeValid));
        }
    }

    @Test
    public void testRestartAnnotationValueHasRestart() {
        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(null, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                ResourceUtils.supplierWithMocks(false), ResourceUtils.dummyClusterOperatorConfig());
        for (AnnotationValueRequest annoValue : generateAnnotationValues()) {
            HasMetadata resource = new KafkaMirrorMaker2Builder()
                    .withNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR, annoValue.annotationValue))
                    .endMetadata()
                    .build();
            boolean hasRestart = op.hasRestartAnnotation(resource, annoValue.connectorName);
            boolean shouldRestart = annoValue.shouldRestart;
            assertThat(String.format("value '%s' is expected to %s", annoValue, shouldRestart ? "restart" : "NOT restart"), hasRestart, equalTo(shouldRestart));
        }
    }

    private List<AnnotationValueRequest> generateAnnotationValues() {
        String connectorName = "my-connector";
        return List.of(
                new AnnotationValueRequest(connectorName, connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":includeTasks,onlyFailed", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":onlyFailed,includeTasks", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":    onlyFailed   ,   includeTasks   ", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":includeTasks", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":onlyFailed", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":   onlyFailed   ", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":    includeTasks   ", connectorName, true, true),
                new AnnotationValueRequest("    " + connectorName + ":    includeTasks   ", connectorName, true, false),
                new AnnotationValueRequest("", connectorName, false, false),
                new AnnotationValueRequest("     ", connectorName, false, false),
                new AnnotationValueRequest("   b  ", connectorName, false, false)
        );
    }

}
