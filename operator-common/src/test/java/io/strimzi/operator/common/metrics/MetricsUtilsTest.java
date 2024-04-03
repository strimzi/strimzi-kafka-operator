/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class MetricsUtilsTest {
    private Meter meter;
    private Set<Tag> meterTags;

    @BeforeEach
    public void setup() {
        meter = Mockito.mock(Meter.class);
        meterTags = new HashSet<>();
        when(meter.getId()).thenReturn(new Meter.Id("testMetric", Tags.of(meterTags), null, null, Meter.Type.COUNTER));
    }

    @Test
    void matchingMetricNameReturnsTrue() {
        assertTrue(MetricsUtils.isMatchingMetricName(meter, "testMetric"));
    }

    @Test
    void nonMatchingMetricNameReturnsFalse() {
        assertFalse(MetricsUtils.isMatchingMetricName(meter, "nonMatchingMetric"));
    }

    @Test
    void matchingMetricTagsReturnsTrue() {
        meterTags.add(Tag.of("key", "value"));
        Set<Tag> expectedTags = new HashSet<>();
        expectedTags.add(Tag.of("key", "value"));

        assertTrue(MetricsUtils.isMatchingMetricTags(meterTags, expectedTags));
    }

    @Test
    void nonMatchingMetricTagsReturnsFalse() {
        meterTags.add(Tag.of("key", "value"));
        Set<Tag> expectedTags = new HashSet<>();
        expectedTags.add(Tag.of("key", "differentValue"));

        assertFalse(MetricsUtils.isMatchingMetricTags(meterTags, expectedTags));
    }
}