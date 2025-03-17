/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.model.PasswordGenerator;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CaProviderTest {
    private static final String NAMESPACE = Reconciliation.DUMMY_RECONCILIATION.namespace();
    private static final String NAME = Reconciliation.DUMMY_RECONCILIATION.name();
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build())
                .endKafka()
            .endSpec()
            .build();

    private static final OpenSslCertIssuer CERT_ISSUER = new OpenSslCertIssuer();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    @Test
    public void testCreateProviderReturnsInternalCaProviderWhenGenerateCaIsTrue() {
        CaConfig caConfig = new CaConfig(new CertificateAuthorityBuilder()
                .withGenerateCertificateAuthority(true)
                .build(), true);

        CaProvider provider = CaProvider.create(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                caConfig,
                KAFKA,
                null,
                null,
                CERT_ISSUER,
                PASSWORD_GENERATOR,
                Clock.systemUTC(),
                null,
                null,
                null,
                false);

        assertThat(provider, instanceOf(InternalCaProvider.class));
    }

    @Test
    public void testCreateProviderReturnsCustomCaProviderWhenGenerateCaIsFalse() {
        CaConfig caConfig = new CaConfig(new CertificateAuthorityBuilder()
                .withGenerateCertificateAuthority(false)
                .build(), true);

        CaProvider provider = CaProvider.create(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                caConfig,
                KAFKA,
                null,
                null,
                CERT_ISSUER,
                PASSWORD_GENERATOR,
                Clock.systemUTC(),
                null,
                null,
                null,
                false);

        assertThat(provider, instanceOf(CustomCaProvider.class));
    }

    @Test
    public void testCreateProviderReturnsCertManagerCaProviderWhenTypeIsCertManager() {
        CaConfig caConfig = new CaConfig(new CertificateAuthorityBuilder()
                .withGenerateCertificateAuthority(false)
                .withType(CertificateManagerType.CERT_MANAGER_IO)
                .build(), true);

        CaProvider provider = CaProvider.create(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                caConfig,
                KAFKA,
                null,
                null,
                CERT_ISSUER,
                PASSWORD_GENERATOR,
                Clock.systemUTC(),
                null,
                null,
                null,
                true);

        assertThat(provider, instanceOf(CertManagerCaProvider.class));
    }

    @Test
    public void testCreateProviderReturnsCustomCaProviderWhenTypeIsCertManagerAndFeatureGateIsNotEnabled() {
        CaConfig caConfig = new CaConfig(new CertificateAuthorityBuilder()
                .withGenerateCertificateAuthority(false)
                .withType(CertificateManagerType.CERT_MANAGER_IO)
                .build(), true);

        assertThrows(RuntimeException.class, () -> CaProvider.create(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                caConfig,
                KAFKA,
                null,
                null,
                CERT_ISSUER,
                PASSWORD_GENERATOR,
                Clock.systemUTC(),
                null,
                null,
                null,
                false));
    }

    @Test
    public void testCreateProviderThrowsWhenTypeIsCertManagerAndGenerateCaIsTrue() {
        CaConfig caConfig = new CaConfig(new CertificateAuthorityBuilder()
                .withGenerateCertificateAuthority(true)
                .withType(CertificateManagerType.CERT_MANAGER_IO)
                .build(), true);

        assertThrows(IllegalArgumentException.class, () -> CaProvider.create(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                caConfig,
                KAFKA,
                null,
                null,
                CERT_ISSUER,
                PASSWORD_GENERATOR,
                Clock.systemUTC(),
                null,
                null,
                null,
                true));
    }
}
