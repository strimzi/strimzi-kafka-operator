/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.strimzi.systemtest.storage.TestStorage;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1Encoding;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import static io.strimzi.systemtest.security.SystemTestCertAndKeyBuilder.endEntityCertBuilder;
import static io.strimzi.systemtest.security.SystemTestCertAndKeyBuilder.intermediateCaCertBuilder;
import static io.strimzi.systemtest.security.SystemTestCertAndKeyBuilder.rootCaCertBuilder;
import static io.strimzi.systemtest.security.SystemTestCertAndKeyBuilder.strimziCaCertBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SystemTestCertManager {

    static final String STRIMZI_ROOT_CA = "C=CZ, L=Prague, O=Strimzi, CN=StrimziRootCA";
    static final String STRIMZI_INTERMEDIATE_CA = "C=CZ, L=Prague, O=Strimzi, CN=StrimziIntermediateCA";
    static final String STRIMZI_END_SUBJECT = "C=CZ, L=Prague, O=Strimzi, CN=kafka.strimzi.io";

    public static SystemTestCertAndKey generateRootCaCertAndKey() {
        return rootCaCertBuilder()
                .withIssuerDn(STRIMZI_ROOT_CA)
                .withSubjectDn(STRIMZI_ROOT_CA)
                .build();
    }

    public static SystemTestCertAndKey generateRootCaCertAndKey(final String rootCaDn, final ASN1Encodable[] sanDnsNames) {
        return rootCaCertBuilder()
            .withIssuerDn(rootCaDn)
            .withSubjectDn(rootCaDn)
            .withSanDnsNames(sanDnsNames)
            .build();
    }

    public static SystemTestCertAndKey generateIntermediateCaCertAndKey(SystemTestCertAndKey rootCert) {
        return intermediateCaCertBuilder(rootCert)
                .withSubjectDn(STRIMZI_INTERMEDIATE_CA)
                .build();
    }

    public static SystemTestCertAndKey generateStrimziCaCertAndKey(SystemTestCertAndKey rootCert, String subjectDn) {
        return strimziCaCertBuilder(rootCert)
                .withSubjectDn(subjectDn)
                .build();
    }

    public static SystemTestCertAndKey generateEndEntityCertAndKey(final SystemTestCertAndKey intermediateCert,
                                                                   final ASN1Encodable[] sansNames) {
        return endEntityCertBuilder(intermediateCert)
                .withSubjectDn(STRIMZI_END_SUBJECT)
                .withSanDnsNames(sansNames)
                .build();
    }

    public static SystemTestCertAndKey generateEndEntityCertAndKey(SystemTestCertAndKey intermediateCert) {
        return endEntityCertBuilder(intermediateCert)
                .withSubjectDn(STRIMZI_END_SUBJECT)
                .withSanDnsName("*.127.0.0.1.nip.io")
                .build();
    }

    /**
     * Generates a broker certificate chain (root CA, intermediate CA, and broker end-entity cert)
     * using SANs derived from the provided {@link TestStorage} context. The resulting certificates
     * are exported to PEM files and returned in a {@link CertAndKeyFiles} bundle.
     *
     * @param testStorage Holds the test context (e.g. cluster name, namespace) used for building SANs.
     * @return A {@link CertAndKeyFiles} object with the broker certificate and key in PEM format.
     */
    public static CertAndKeyFiles createBrokerCertChain(final TestStorage testStorage) {
        final SystemTestCertAndKey root = generateRootCaCertAndKey();
        final SystemTestCertAndKey intermediate = generateIntermediateCaCertAndKey(root);
        final SystemTestCertAndKey brokerCertAndKey = generateEndEntityCertAndKey(intermediate, retrieveKafkaBrokerSANs(testStorage));

        return exportToPemFiles(brokerCertAndKey);
    }

    public static CertAndKeyFiles exportToPemFiles(SystemTestCertAndKey... certs) {
        if (certs.length == 0) {
            throw new IllegalArgumentException("List of certificates should has at least one element");
        }
        try {
            File keyFile = exportPrivateKeyToPemFile(certs[0].getPrivateKey());
            File certFile = exportCertsToPemFile(certs);
            return new CertAndKeyFiles(certFile, keyFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File convertPrivateKeyToPKCS8File(PrivateKey privatekey) throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        byte[] encoded = privatekey.getEncoded();
        final PrivateKeyInfo privateKeyInfo = PrivateKeyInfo.getInstance(encoded);

        final ASN1Encodable asn1Encodable = privateKeyInfo.parsePrivateKey();
        final byte[] privateKeyPKCS8Formatted = asn1Encodable.toASN1Primitive().getEncoded(ASN1Encoding.DER);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyPKCS8Formatted);

        KeyFactory kf = KeyFactory.getInstance(SystemTestCertAndKeyBuilder.KEY_PAIR_ALGORITHM);
        PrivateKey privateKey = kf.generatePrivate(keySpec);
        return  exportPrivateKeyToPemFile(privateKey);
    }

    private static File exportPrivateKeyToPemFile(PrivateKey privateKey) throws IOException {
        File keyFile = Files.createTempFile("key-", ".key").toFile();
        try (JcaPEMWriter pemWriter = new JcaPEMWriter(new FileWriter(keyFile, UTF_8))) {
            pemWriter.writeObject(privateKey);
            pemWriter.flush();
        }
        return keyFile;
    }

    private static File exportCertsToPemFile(SystemTestCertAndKey... certs) throws IOException {
        File certFile = Files.createTempFile("crt-", ".crt").toFile();
        try (JcaPEMWriter pemWriter = new JcaPEMWriter(new FileWriter(certFile, UTF_8))) {
            for (SystemTestCertAndKey certAndKey : certs) {
                pemWriter.writeObject(certAndKey.getCertificate());
            }
            pemWriter.flush();
        }
        return certFile;
    }

    /**
     * Constructs a list of Subject Alternative Name (SAN) DNS entries commonly used
     * for Kafka broker certificates in tests.
     *
     * @param testStorage   Holds test-related identifiers such as the cluster name and namespace.
     * @return An array of ASN1Encodable objects representing DNS Subject Alternative Names for Kafka brokers.
     */
    public static ASN1Encodable[] retrieveKafkaBrokerSANs(final TestStorage testStorage) {
        return new ASN1Encodable[] {
            new GeneralName(GeneralName.dNSName, "*.127.0.0.1.nip.io"),
            new GeneralName(GeneralName.dNSName, "*." + testStorage.getClusterName() + "-kafka-brokers"),
            new GeneralName(GeneralName.dNSName, "*." + testStorage.getClusterName() + "-kafka-brokers." + testStorage.getNamespaceName() + ".svc"),
            new GeneralName(GeneralName.dNSName, testStorage.getClusterName() + "-kafka-bootstrap"),
            new GeneralName(GeneralName.dNSName, testStorage.getClusterName() + "-kafka-bootstrap." + testStorage.getNamespaceName() + ".svc")
        };
    }

    /**
     * This method exports Certificate Authority (CA) data to a temporary file for cases in which mentioned data is
     * necessary in form of file - for use in applications like OpenSSL. The primary purpose is to save CA files,
     * such as certificates and private keys (e.g., ca.key and ca.cert), into temporary files.
     * These files are essential when you need to provide CA data to other applications, such as OpenSSL,
     * for signing user Certificate Signing Requests (CSRs).
     *
     * @param caData The Certificate Authority data to be saved to the temporary file.
     * @param prefix The prefix for the temporary file's name.
     * @param suffix The suffix for the temporary file's name.
     * @return A File object representing the temporary file containing the CA data.
     * @throws RuntimeException If an IOException occurs while creating a file or writing into the temporary file
     * given the critical role these operations play in ensuring proper functionality.
     */
    public static File exportCaDataToFile(String caData, String prefix, String suffix) {
        try {
            File tempFile = Files.createTempFile(prefix + "-", suffix).toFile();

            try (FileWriter fileWriter = new FileWriter(tempFile, StandardCharsets.UTF_8)) {
                fileWriter.write(caData);
                fileWriter.flush();
            }

            return tempFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean containsAllDN(String principal1, String principal2) {
        try {
            return new LdapName(principal1).getRdns().containsAll(new LdapName(principal2).getRdns());
        } catch (InvalidNameException e) {
            e.printStackTrace();
        }
        return false;
    }
}
