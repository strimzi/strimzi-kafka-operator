/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility class for handling certificate objects
 */
public class CertificateUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CertificateUtils.class);

    private CertificateUtils() {}

    /**
     * Converts the Java X509Certificate into the proper PEM format.
     *
     * @param cert  X509 certificate to convert
     *
     * @return  String with PEM encoded certificate
     *
     * @throws CertificateEncodingException An exception might be thrown if the certificate encoding fails
     */
    public static String x509CertificateToPem(X509Certificate cert) throws CertificateEncodingException {
        Base64.Encoder encoder = Base64.getMimeEncoder(64, System.lineSeparator().getBytes(StandardCharsets.US_ASCII));

        return "-----BEGIN CERTIFICATE-----\n"
                + new String(encoder.encode(cert.getEncoded()), StandardCharsets.US_ASCII)
                + "\n-----END CERTIFICATE-----";
    }

    /**
     * Creates X509Certificate instance from a byte array containing a certificate.
     *
     * @param bytes     Bytes with the X509 certificate
     *
     * @throws CertificateException     Thrown when the creation of the X509Certificate instance fails. Typically, this
     *                                  would happen because the bytes do not contain a valid X509 certificate.
     *
     * @return  X509Certificate instance created based on the Certificate bytes
     */
    public static X509Certificate x509Certificate(byte[] bytes) throws CertificateException {
        CertificateFactory factory = certificateFactory();
        return x509Certificate(factory, bytes);
    }

    static X509Certificate x509Certificate(CertificateFactory factory, byte[] bytes) throws CertificateException {
        // When bytes contain a certificate chain, read only the first certificate
        // to get thumbprints of the leaf certificate
        Certificate certificate = factory.generateCertificates(new ByteArrayInputStream(bytes)).stream().findFirst().orElse(null);
        if (certificate instanceof X509Certificate) {
            return (X509Certificate) certificate;
        } else {
            throw new CertificateException("Not an X509Certificate: " + certificate);
        }
    }

    static CertificateFactory certificateFactory() {
        CertificateFactory factory;
        try {
            factory = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new RuntimeException("No security provider with support for X.509 certificates", e);
        }
        return factory;
    }

    /**
     * Extracts X509 certificate from a Kubernetes Secret
     *
     * @param secret    Kubernetes Secret with the certificate
     * @param key       Key under which the certificate is stored in the Secret
     *
     * @return  An X509Certificate instance with the certificate
     */
    public static X509Certificate cert(Secret secret, String key)  {
        if (secret == null || secret.getData() == null || secret.getData().get(key) == null) {
            return null;
        }
        byte[] bytes = Util.decodeBytesFromBase64(secret.getData().get(key));
        try {
            return x509Certificate(bytes);
        } catch (CertificateException e) {
            throw new RuntimeException("Failed to decode certificate in data." + key.replace(".", "\\.") + " of Secret " + secret.getMetadata().getName(), e);
        }
    }

    /**
     * Validates whether each of the user provided CA certs has a valid chain. Any intermediate CAs should be provided first,
     * in order, with the root CA being the last certificate.
     *
     * @param reconciliation Reconciliation marker.
     * @param caRole The role of the CA.
     * @param userCaCertData The CA cert data provided by the user.
     */
    public static void validateUserCaCertChain(Reconciliation reconciliation, Ca.CaRole caRole, Map<String, String> userCaCertData) {
        userCaCertData.entrySet()
                .stream()
                .filter(entry -> Ca.SecretEntry.CRT.matchesType(entry.getKey()))
                .forEach(entry -> {
                    List<X509Certificate> certChain = extractCertChain(entry.getKey(), Util.decodeBytesFromBase64(entry.getValue()));
                    if (certChain.isEmpty()) {
                        LOGGER.errorCr(reconciliation, "{} certificate chain in {} is empty", caRole.caName(), entry.getKey());
                        throw new RuntimeException("Failed to validate User supplied " + caRole.caName() + " cert chain in " + entry.getKey());
                    } else if (certChain.size() == 1) {
                        LOGGER.debugCr(reconciliation, "{} certificate {} contains a single certificate", caRole.caName(), entry.getKey());
                        return;
                    }
                    if (!certIsTrusted(reconciliation, certChain.subList(0, certChain.size() - 1), certChain.getLast())) {
                        String errorMessage = "User supplied " + caRole.caName() + " cert chain " + entry.getKey() + " is not valid. Certificates must be provided in the correct order.";
                        LOGGER.errorCr(reconciliation, errorMessage);
                        throw new RuntimeException(errorMessage);
                    }
                });
    }

    /**
     * Validates whether the provided cert chain is trusted and valid using the provided CA certificate.
     * <p>
     * Uses the <code>CertPathValidator.validate</code> method to check if a cert chain
     * can be validated with the provided caCert. If the cert chain contains more than one
     * certificate the first certificate should be the end entity (or leaf) certificate, while
     * the final certificate should be the one issued by the root Ca. The root Ca should not be
     * included in the cert chain list.
     *
     * @param reconciliation Reconciliation marker
     * @param certChainToValidate Certificates to validate. Can be a single certificate or a chain of ordered certificates.
     * @param caCert The root Ca certificate to use for validation.
     *
     * @return True if the CA certificate can be used to validate the provided certificate or certificate chain. False otherwise.
     */
    public static boolean certIsTrusted(Reconciliation reconciliation, List<X509Certificate> certChainToValidate, X509Certificate caCert) {
        CertPathValidator certPathValidator;
        CertPath eeCertPath;
        PKIXParameters pkixParams;
        try {
            certPathValidator = CertPathValidator.getInstance("PKIX");
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            TrustAnchor trustAnchor = new TrustAnchor(caCert, null);
            pkixParams = new PKIXParameters(Collections.singleton(trustAnchor));
            pkixParams.setRevocationEnabled(false);
            eeCertPath = factory.generateCertPath(certChainToValidate);
        } catch (NoSuchAlgorithmException | CertificateException | InvalidAlgorithmParameterException e) {
            LOGGER.errorCr(reconciliation, "Error constructing objects to validate certificate chain.", e);
            throw new RuntimeException(e);
        }
        try {
            certPathValidator.validate(eeCertPath, pkixParams);
            LOGGER.debugCr(reconciliation, "Certificate chain validated using supplied CA cert.");
            return true;
        } catch (CertPathValidatorException e) {
            LOGGER.errorCr(reconciliation, "Certificate chain cannot be validated with supplied CA cert.", e);
            return false;
        } catch (InvalidAlgorithmParameterException e) {
            LOGGER.errorCr(reconciliation, "Error validating the certificate chain.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Extract List of X509Certificates from certificate bytes
     * @param key Key to use during logging
     * @param certBytes Certificate bytes to extract X509Certificates from
     * @return List of X509Certificates
     */
    public static List<X509Certificate> extractCertChain(String key, byte[] certBytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(certBytes)) {
            Collection<? extends Certificate> certificates = certificateFactory().generateCertificates(bis);
            List<X509Certificate> x509Certificates = new ArrayList<>(certificates.size());
            for (Certificate certificate : certificates) {
                if (certificate instanceof X509Certificate) {
                    x509Certificates.add((X509Certificate) certificate);
                } else {
                    throw new CertificateException("Not an X509Certificate: " + certificate);
                }
            }
            return x509Certificates;
        } catch (CertificateException | IOException e) {
            throw new RuntimeException("Failed to decode "  + key, e);
        }
    }

    /**
     * Create a subject
     *
     * @param commonName The CN of the certificate to be created.
     * @param organization The O of the certificate to be created. May be null.
     * @return The subject created with the given CN and O
     */
    public static Subject getSubject(String commonName, String organization) {
        Subject.Builder subject = new Subject.Builder();
        if (organization != null) {
            subject.withOrganizationName(organization);
        }
        subject.withCommonName(commonName);
        return subject.build();
    }
}
