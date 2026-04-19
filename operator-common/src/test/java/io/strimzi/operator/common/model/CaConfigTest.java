/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the CaConfig class
 */
public class CaConfigTest {

    @Test
    void testDefaultConstructor() {
        CaConfig caConfig = CaConfig.createDefault();

        assertThat(caConfig.getValidityDays(), is(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS));
        assertThat(caConfig.getRenewalDays(), is(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS));
        assertTrue(caConfig.isGenerateCa());
        assertTrue(caConfig.isGenerateSecretOwnerRef());
        assertThat(caConfig.getCertificateExpirationPolicy(), is(CertificateExpirationPolicy.RENEW_CERTIFICATE));
    }

    @Test
    void testConstructorPassingNullCertificateAuthority() {
        CaConfig caConfig = new CaConfig(null, true);

        assertThat(caConfig.getValidityDays(), is(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS));
        assertThat(caConfig.getRenewalDays(), is(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS));
        assertTrue(caConfig.isGenerateCa());
        assertTrue(caConfig.isGenerateSecretOwnerRef());
        assertThat(caConfig.getCertificateExpirationPolicy(), is(CertificateExpirationPolicy.RENEW_CERTIFICATE));
    }

    @Test
    void testConstructor() {
        CertificateAuthority ca = new CertificateAuthorityBuilder()
                .withValidityDays(6)
                .withRenewalDays(4)
                .withGenerateCertificateAuthority(false)
                .withGenerateSecretOwnerReference(false)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();
        CaConfig caConfig = new CaConfig(ca, true);

        assertThat(caConfig.getValidityDays(), is(6));
        assertThat(caConfig.getRenewalDays(), is(4));
        assertFalse(caConfig.isGenerateCa());
        assertFalse(caConfig.isGenerateSecretOwnerRef());
        assertThat(caConfig.getCertificateExpirationPolicy(), is(CertificateExpirationPolicy.REPLACE_KEY));
    }

    @Test
    void testConstructorPassingSomeVariables() {
        CaConfig caConfig = new CaConfig(6, 4, false, true);

        assertThat(caConfig.getValidityDays(), is(6));
        assertThat(caConfig.getRenewalDays(), is(4));
        assertFalse(caConfig.isGenerateCa());
        assertTrue(caConfig.isGenerateSecretOwnerRef());
        assertThat(caConfig.getCertificateExpirationPolicy(), is(CertificateExpirationPolicy.RENEW_CERTIFICATE));
    }
}
