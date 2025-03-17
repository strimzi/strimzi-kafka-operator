/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.common.certmanager.IssuerKind;
import io.strimzi.api.kafka.model.common.certmanager.IssuerRefBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertIssuer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CertManagerCaTest {
    private final static String NAMESPACE = Reconciliation.DUMMY_RECONCILIATION.namespace();

    @Test
    public void matchesCertManagerSecretNaming() {
        assertThat(CertManagerCa.matchesCertManagerSecretNaming("test-cm"), is(true));
        assertThat(CertManagerCa.matchesCertManagerSecretNaming("test-foo"), is(false));
        assertThat(CertManagerCa.matchesCertManagerSecretNaming("cm-test"), is(false));
    }

    @Test
    public void mapToStrimziSecretName() {
        assertThat(CertManagerCa.mapToStrimziSecretName("test-cm"), is("test"));
        assertThat(CertManagerCa.mapToStrimziSecretName("test-cm-cm"), is("test-cm"));
        assertThrows(RuntimeException.class, () -> CertManagerCa.mapToStrimziSecretName("test"));
    }

    @Test
    public void removeOldCertificate() {
        Map<String, String> data = new HashMap<>();
        data.put("ca.crt", MockCertIssuer.clusterCaCert());
        data.put("ca-2023-03-23T09-00-00Z.crt", MockCertIssuer.clusterCaCert());
        Secret existingCertSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-cluster-ca-secret")
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(data)
                .build();

        CertManagerCa certManagerCa = new CertManagerCa(
                Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                existingCertSecret,
                new CaConfig(100, 10, false, false, CertificateManagerType.CERT_MANAGER_IO),
                null,
                null,
                null,
                new IssuerRefBuilder()
                        .withName("cm-issuer")
                        .withKind(IssuerKind.CLUSTER_ISSUER)
                        .build()
        );

        assertThat(certManagerCa.caCertData().size(), is(2));
        assertThat(certManagerCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(true));

        certManagerCa.maybeDeleteOldCerts();
        assertThat(certManagerCa.caCertData().size(), is(1));
        assertThat(certManagerCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(false));
        assertThat(certManagerCa.certsRemoved(), is(true));
    }
}
