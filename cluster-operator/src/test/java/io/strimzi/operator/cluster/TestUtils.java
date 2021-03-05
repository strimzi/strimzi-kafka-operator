/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.cluster.model.AbstractModel;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.fail;

public class TestUtils {
    public static JmxPrometheusExporterMetrics getJmxPrometheusExporterMetrics(String key, String name) {
        JmxPrometheusExporterMetrics metricsConfig = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                            .withName(name)
                            .withKey(key)
                            .withOptional(true)
                            .build())
                .endValueFrom()
                .build();
        return metricsConfig;
    }

    public static ConfigMap getJmxMetricsCm(String data, String metricsCMName) {
        ConfigMap metricsCM = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(metricsCMName)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, data))
                .build();
        return metricsCM;
    }

    public static <T extends CustomResource<?, ? extends Status>> void waitForStatus(Resource<T> resource, String resourceName, Predicate<T> predicate) {
        try {
            resource.waitUntilCondition(predicate, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (!(e instanceof TimeoutException)) {
                throw new RuntimeException(e);
            }
            String conditions =
                    resource.get().getStatus() == null ? "no status" :
                            String.valueOf(resource.get().getStatus().getConditions());
            fail(resourceName + " never matched required predicate: " + conditions);
        }
    }
}
