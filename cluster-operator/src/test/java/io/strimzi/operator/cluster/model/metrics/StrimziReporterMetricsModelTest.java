/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertThrows;

public class StrimziReporterMetricsModelTest {

    @Test
    public void testGetAllowlistWithValidPatterns() throws Exception {
        StrimziMetricsReporterValues values = new StrimziMetricsReporterValues();
        values.setAllowList(Arrays.asList("metric1.*", "metric2.*"));

        Optional<String> allowlist = values.getAllowlist();
        Assertions.assertTrue(allowlist.isPresent());
        Assertions.assertEquals("metric1.*|metric2.*", allowlist.get());
    }

    @Test
    public void testGetAllowlistWithEmptyList() throws Exception {
        StrimziMetricsReporterValues values = new StrimziMetricsReporterValues();
        values.setAllowList(Collections.emptyList());

        Optional<String> allowlist = values.getAllowlist();
        Assertions.assertFalse(allowlist.isPresent());
    }

    @Test
    public void testGetAllowlistWithNullList() throws Exception {
        StrimziMetricsReporterValues values = new StrimziMetricsReporterValues();
        values.setAllowList(null);

        Optional<String> allowlist = values.getAllowlist();
        Assertions.assertFalse(allowlist.isPresent());
    }

    @Test
    public void testGetAllowlistWithInvalidPattern() {
        StrimziMetricsReporterValues values = new StrimziMetricsReporterValues();
        values.setAllowList(Arrays.asList("metric1.*", "metric["));

        Exception exception = assertThrows(Exception.class, values::getAllowlist);
        Assertions.assertTrue(exception.getMessage().contains("Invalid regular expression in prometheus.metrics.reporter.allowlist"));
    }
}
