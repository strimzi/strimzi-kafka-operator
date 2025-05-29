/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;

/**
 *  Provides auxiliary methods for Scraper Pod, which reaches KafkaConnect API in the Kubernetes cluster.
 */
public class ScraperUtils {

    private ScraperUtils() { }

    public static Pod getScraperPod(final String namespaceName) {
        return KubeResourceManager.get().kubeClient().listPods(namespaceName, getDefaultLabelSelector()).stream().findFirst().orElseThrow();
    }

    private static LabelSelector getDefaultLabelSelector() {
        return new LabelSelectorBuilder()
            .addToMatchLabels(TestConstants.SCRAPER_LABEL_KEY, TestConstants.SCRAPER_LABEL_VALUE)
            .build();
    }
}
