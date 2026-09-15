/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.ca;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.kafka.certmanager.IssuerRef;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.CertManagerCa;
import io.strimzi.operator.common.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.user.model.InvalidCertificateException;

import java.util.concurrent.CompletionStage;

/**
 * Issuer that uses cert-manager to issue user certificates.
 */
public class CertManagerCaUserCertIssuer implements UserCertIssuer {
    private final CertManagerCertificateOperator certManagerCertificateOperator;
    private final SecretOperator secretOperator;
    private final IssuerRef issuerRef;

    /**
     * Constructor
     *
     * @param certManagerCertificateOperator    Operator for managing cert-manager Certificate resources
     * @param secretOperator                    Operator for managing Secrets
     * @param issuerRef                         Reference to the cert-manager issuer
     */
    public CertManagerCaUserCertIssuer(CertManagerCertificateOperator certManagerCertificateOperator,
                                       SecretOperator secretOperator,
                                       IssuerRef issuerRef) {
        this.certManagerCertificateOperator = certManagerCertificateOperator;
        this.secretOperator = secretOperator;
        this.issuerRef = issuerRef;
    }

    @Override
    public CompletionStage<UserCertResult> maybeCopyOrGenerateCert(
            Reconciliation reconciliation,
            Secret caCertSecret,
            Secret caKeySecret,
            Secret userSecret,
            String userName,
            int validityDays,
            int renewalDays,
            boolean generatePkcs12Stores,
            OwnerReference ownerReference) {
        validateCaSecrets(caCertSecret);

        CertManagerCa clientsCa = new CertManagerCa(
                reconciliation,
                Ca.CaRole.CLIENTS_CA,
                caCertSecret,
                new CaConfig(validityDays, renewalDays, false, generatePkcs12Stores, CertificateManagerType.CERT_MANAGER),
                certManagerCertificateOperator,
                secretOperator,
                ownerReference,
                issuerRef
        );

        CertAndKey existingCertAndKey = null;
        if (userSecret != null) {
            String userCrt = userSecret.getData().get("user.crt");
            String userKey = userSecret.getData().get("user.key");
            if (userCrt != null && !userCrt.isEmpty() && userKey != null && !userKey.isEmpty()) {
                existingCertAndKey = new CertAndKey(Util.decodeBytesFromBase64(userKey), Util.decodeBytesFromBase64(userCrt), clientsCa.caCertGeneration());
            }
        }

        return clientsCa.maybeCopyOrGenerateClientCert(reconciliation, userName, existingCertAndKey, false)
                .thenApply(certAndKey -> new UserCertResult(clientsCa.currentCaCertBase64(), certAndKey));
    }

    private void validateCaSecrets(Secret caCertSecret) {
        if (caCertSecret == null) {
            throw new InvalidCertificateException("The Clients CA Cert Secret is missing");
        } else if (caCertSecret.getData() == null || caCertSecret.getData().get("ca.crt") == null) {
            throw new InvalidCertificateException("The Clients CA Cert Secret is missing the ca.crt file");
        }
    }
}
