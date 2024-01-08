/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplateBuilder;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplateBuilder;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class TemplateUtilsTest {
    @ParallelTest
    public void testMetadataWithNullTemplate() {
        assertThat(TemplateUtils.annotations(null), is(nullValue()));
        assertThat(TemplateUtils.labels(null), is(nullValue()));
    }

    @ParallelTest
    public void testMetadataWithEmptyMetadataTemplate() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        assertThat(TemplateUtils.annotations(template), is(Map.of()));
        assertThat(TemplateUtils.labels(template), is(Map.of()));
    }

    @ParallelTest
    public void testAnnotations() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                    .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                .endMetadata()
                .build();

        assertThat(TemplateUtils.annotations(template), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));
    }

    @ParallelTest
    public void testLabels() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                    .withLabels(Map.of("label-1", "value-1", "label-2", "value-2"))
                .endMetadata()
                .build();

        assertThat(TemplateUtils.labels(template), is(Map.of("label-1", "value-1", "label-2", "value-2")));
    }

    @ParallelTest
    public void testDeploymentStrategy() {
        assertThat(TemplateUtils.deploymentStrategy(null, DeploymentStrategy.RECREATE), is(DeploymentStrategy.RECREATE));
        assertThat(TemplateUtils.deploymentStrategy(new DeploymentTemplate(), DeploymentStrategy.ROLLING_UPDATE), is(DeploymentStrategy.ROLLING_UPDATE));
        assertThat(TemplateUtils.deploymentStrategy(new DeploymentTemplateBuilder().withDeploymentStrategy(DeploymentStrategy.RECREATE).build(), DeploymentStrategy.ROLLING_UPDATE), is(DeploymentStrategy.RECREATE));
    }
}
