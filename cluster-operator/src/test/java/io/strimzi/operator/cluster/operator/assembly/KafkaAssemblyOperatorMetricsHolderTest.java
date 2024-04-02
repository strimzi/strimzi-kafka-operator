/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.metrics.CertificateMetricKey;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaAssemblyOperatorMetricsHolderTest {

    private KafkaAssemblyOperatorMetricsHolder metricsHolder;

    @BeforeEach
    void setup() {
        metricsHolder = new KafkaAssemblyOperatorMetricsHolder("TestKind", Labels.EMPTY, new MicrometerMetricsProvider(new SimpleMeterRegistry()));
        metricsHolder.certificateExpirationMap.put(new CertificateMetricKey("TestKind", "TestNamespace", "TestCluster", CertificateMetricKey.Type.CLIENT_CA), new AtomicLong(1000L));
        metricsHolder.certificateExpirationMap.put(new CertificateMetricKey("TestKind", "TestNamespace", "TestCluster", CertificateMetricKey.Type.CLUSTER_CA), new AtomicLong(2000L));
    }

    @Test
    @DisplayName("Should remove certificate metrics for client CA type")
    void shouldRemoveCertificateMetricsForClientCaType() {
        Predicate<CertificateMetricKey> predicate = key -> matchCaTypes(key.getCaType(), CertificateMetricKey.Type.CLIENT_CA);
        metricsHolder.removeMetricsForCertificates(predicate);

        boolean hasClientCaMetric = metricsHolder.certificateExpirationMap.keySet().stream()
                .anyMatch(key -> matchCaTypes(((CertificateMetricKey) key).getCaType(), CertificateMetricKey.Type.CLIENT_CA));

        assertTrue(!hasClientCaMetric, "Client CA metric should be removed");
    }

    @Test
    @DisplayName("Should not remove certificate metrics for cluster CA type")
    void shouldNotRemoveCertificateMetricsForClusterCaType() {
        Predicate<CertificateMetricKey> predicate = key -> matchCaTypes(key.getCaType(), CertificateMetricKey.Type.CLIENT_CA);
        metricsHolder.removeMetricsForCertificates(predicate);

        boolean hasClusterCaMetric = metricsHolder.certificateExpirationMap.keySet().stream()
                .anyMatch(key -> matchCaTypes(((CertificateMetricKey) key).getCaType(), CertificateMetricKey.Type.CLUSTER_CA));

        assertTrue(hasClusterCaMetric, "Cluster CA metric should not be removed");
    }

    @Test
    @DisplayName("Should return correct expiration time for client CA certificate")
    void shouldReturnCorrectExpirationTimeForClientCaCertificate() {
        AtomicLong expirationTime = metricsHolder.clientCaCertificateExpiration("TestCluster", "TestNamespace");

        assertEquals(1000L, expirationTime.get(), "Expiration time should match the initial value");
    }

    @Test
    @DisplayName("Should return correct expiration time for cluster CA certificate")
    void shouldReturnCorrectExpirationTimeForClusterCaCertificate() {
        AtomicLong expirationTime = metricsHolder.clusterCaCertificateExpiration("TestCluster", "TestNamespace");

        assertEquals(2000L, expirationTime.get(), "Expiration time should match the initial value");
    }

    private boolean matchCaTypes(String actual, CertificateMetricKey.Type expected) {
        return actual.equals(expected.getDisplayName().toLowerCase(Locale.ROOT));
    }
}
