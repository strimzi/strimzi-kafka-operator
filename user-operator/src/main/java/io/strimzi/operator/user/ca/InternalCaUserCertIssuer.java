/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.ca;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertIssuer;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.CertificateUtils;
import io.strimzi.operator.common.ca.InternalCa;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.user.model.InvalidCertificateException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Issuer that uses the internal CA to issue user certificates.
 */
public class InternalCaUserCertIssuer implements UserCertIssuer {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(InternalCaUserCertIssuer.class);

    private final CertIssuer certIssuer;
    private final PasswordGenerator passwordGenerator;
    private final List<String> maintenanceWindows;
    private final Clock clock;

    /**
     * Constructor
     *
     * @param certIssuer            CertIssuer instance for handling certificates creation
     * @param passwordGenerator     PasswordGenerator instance for generating passwords
     * @param maintenanceWindows    List of configured maintenance windows
     * @param clock                 Clock for checking maintenance windows
     */
    public InternalCaUserCertIssuer(CertIssuer certIssuer, PasswordGenerator passwordGenerator, List<String> maintenanceWindows, Clock clock) {
        this.certIssuer = certIssuer;
        this.passwordGenerator = passwordGenerator;
        this.maintenanceWindows = maintenanceWindows;
        this.clock = clock;
    }

    @Override
    public CompletionStage<UserCertResult> maybeCopyOrGenerateCert(
            Reconciliation reconciliation,
            Secret clientsCaCertSecret,
            Secret clientsCaKeySecret,
            Secret userSecret,
            String userName,
            int caValidityDays,
            int caRenewalDays,
            boolean generatePkcs12Stores,
            OwnerReference ownerReference) {
        validateCaSecrets(reconciliation, clientsCaCertSecret, clientsCaKeySecret);

        InternalCa clientsCa = new InternalCa(
                reconciliation,
                Ca.CaRole.CLIENTS_CA,
                certIssuer,
                passwordGenerator,
                clientsCaCertSecret,
                clientsCaKeySecret,
                new CaConfig(caValidityDays, caRenewalDays, false, generatePkcs12Stores, CertificateManagerType.STRIMZI)
        );

        CertAndKey existingUserCertAndKey = null;
        if (userSecret != null) {
            if (userSecret.getMetadata() != null
                    && Annotations.booleanAnnotation(userSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, false)) {
                // The user secret has the annotation which forces replacement => we have to generate a new user certificate
                LOGGER.infoCr(reconciliation, "Certificate for user {} in namespace {} will be renewed due to force-renew annotation", userName, reconciliation.namespace());
            } else {
                // Secret already exists -> lets verify if it has keys from the same CA
                String originalCaCrt = clientsCaCertSecret.getData().get("ca.crt");
                String caCrt = userSecret.getData().get("ca.crt");

                if (originalCaCrt != null && originalCaCrt.equals(caCrt)) {
                    existingUserCertAndKey = getExistingCertificateAndKey(reconciliation, clientsCa, userName, userSecret, generatePkcs12Stores);
                }
            }
        }

        return clientsCa.maybeCopyOrGenerateClientCert(reconciliation, userName, existingUserCertAndKey, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()))
                .thenApply(certAndKey -> new UserCertResult(clientsCa.currentCaCertBase64(), certAndKey));
    }

    private CertAndKey getExistingCertificateAndKey(Reconciliation reconciliation, InternalCa clientsCa, String userName, Secret userSecret, boolean generatePkcs12Stores) {
        String userCrt = userSecret.getData().get("user.crt");
        String userKey = userSecret.getData().get("user.key");
        if (userCrt == null || userCrt.isEmpty() || userKey == null || userKey.isEmpty()) {
            return null;
        }

        byte[] key = Util.decodeBytesFromBase64(userKey);
        byte[] cert = Util.decodeBytesFromBase64(userCrt);
        int caCertGeneration = clientsCa.caCertGeneration();

        if (!generatePkcs12Stores) {
            return new CertAndKey(key, cert, null, null, null, caCertGeneration);
        }

        String userKeyStore = userSecret.getData().get("user.p12");
        String userKeyStorePassword = userSecret.getData().get("user.password");

        if (userKeyStore != null && !userKeyStore.isEmpty()
                && userKeyStorePassword != null && !userKeyStorePassword.isEmpty()) {
            return new CertAndKey(
                    key,
                    cert,
                    null,
                    Util.decodeBytesFromBase64(userKeyStore),
                    new String(Util.decodeBytesFromBase64(userKeyStorePassword), StandardCharsets.US_ASCII),
                    caCertGeneration);
        }

        // PKCS12 stores should be generated because they are missing from the secret
        try {
            return clientsCa.generatePkcs12Store(userName, key, cert, caCertGeneration);
        } catch (IOException e) {
            LOGGER.errorCr(reconciliation, "Error generating the keystore for user {}", userName, e);
            return null;
        }
    }

    private void validateCaSecrets(Reconciliation reconciliation, Secret clientsCaCertSecret, Secret clientsCaKeySecret) {
        if (clientsCaCertSecret == null) {
            throw new InvalidCertificateException("The Clients CA Cert Secret is missing");
        } else if (clientsCaCertSecret.getData() == null || clientsCaCertSecret.getData().get("ca.crt") == null) {
            throw new InvalidCertificateException("The Clients CA Cert Secret is missing the ca.crt file");
        } else if (clientsCaKeySecret == null) {
            throw new InvalidCertificateException("The Clients CA Key Secret is missing");
        } else if (clientsCaKeySecret.getData() == null || clientsCaKeySecret.getData().get("ca.key") == null) {
            throw new InvalidCertificateException("The Clients CA Key Secret is missing the ca.key file");
        }
        CertificateUtils.validateUserCaCertChain(reconciliation, Ca.CaRole.CLIENTS_CA, clientsCaCertSecret.getData());
    }
}
