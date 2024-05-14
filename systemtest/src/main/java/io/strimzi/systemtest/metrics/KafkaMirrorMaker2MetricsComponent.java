/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;

public class KafkaMirrorMaker2MetricsComponent implements MetricsComponent {

    private String componentName;

    public static KafkaMirrorMaker2MetricsComponent create(final String componentName) {
        return new KafkaMirrorMaker2MetricsComponent(componentName);
    }

    private KafkaMirrorMaker2MetricsComponent(String componentName) {
        this.componentName = componentName;
    }

    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.COMPONENTS_METRICS_PORT;
    }

    @Override
    public String getDefaultMetricsPath() {
        return "/metrics";
    }

    @Override
    public LabelSelector getLabelSelector() {
        return KafkaMirrorMaker2Resource.getLabelSelector(componentName, KafkaMirrorMaker2Resources.componentName(componentName));
    }
}
