/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1Object;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.asList;
import static org.bouncycastle.asn1.x509.Extension.authorityKeyIdentifier;
import static org.bouncycastle.asn1.x509.Extension.basicConstraints;
import static org.bouncycastle.asn1.x509.Extension.extendedKeyUsage;
import static org.bouncycastle.asn1.x509.Extension.keyUsage;
import static org.bouncycastle.asn1.x509.Extension.subjectAlternativeName;
import static org.bouncycastle.asn1.x509.Extension.subjectKeyIdentifier;
import static org.bouncycastle.asn1.x509.GeneralName.dNSName;
import static org.bouncycastle.asn1.x509.KeyPurposeId.id_kp_clientAuth;
import static org.bouncycastle.asn1.x509.KeyPurposeId.id_kp_serverAuth;
import static org.bouncycastle.asn1.x509.KeyUsage.cRLSign;
import static org.bouncycastle.asn1.x509.KeyUsage.digitalSignature;
import static org.bouncycastle.asn1.x509.KeyUsage.keyCertSign;
import static org.bouncycastle.asn1.x509.KeyUsage.keyEncipherment;
import static org.bouncycastle.jce.provider.BouncyCastleProvider.PROVIDER_NAME;

public class SystemTestCertAndKeyBuilder {

    private static final int KEY_SIZE = 2048;
    protected static final String KEY_PAIR_ALGORITHM = "RSA";
    private static final String SIGNATURE_ALGORITHM = "SHA256WithRSA";
    private static final Duration CERTIFICATE_VALIDITY_PERIOD = Duration.ofDays(30);

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private final KeyPair keyPair;
    private final SystemTestCertAndKey caCert;
    private final List<Extension> extensions;
    private X500Name issuer;
    private X500Name subject;

    // Suppresses the deprecation warning about getSubjectDN()
    @SuppressWarnings("deprecation")
    private SystemTestCertAndKeyBuilder(KeyPair keyPair, SystemTestCertAndKey caCert, List<Extension> extensions) {
        this.keyPair = keyPair;
        this.caCert = caCert;
        if (caCert != null) {
            // getSubjectDN is deprecated, but BouncyCastle does not seem to work well with getSubjectX500Principal which replaces it
            // The fix for this issue is tracked in https://github.com/strimzi/strimzi-kafka-operator/issues/7698
            this.issuer = new X500Name(caCert.getCertificate().getSubjectDN().getName());
        }
        this.extensions = new ArrayList<>(extensions);
    }

    public static SystemTestCertAndKeyBuilder rootCaCertBuilder() {
        KeyPair keyPair = generateKeyPair();
        return new SystemTestCertAndKeyBuilder(
                keyPair,
                null,
                asList(
                        new Extension(keyUsage, true, keyUsage(keyCertSign | cRLSign)),
                        new Extension(basicConstraints, true, ca()),
                        new Extension(subjectKeyIdentifier, false, createSubjectKeyIdentifier(keyPair.getPublic()))
                )
        );
    }

    public static SystemTestCertAndKeyBuilder intermediateCaCertBuilder(SystemTestCertAndKey caCert) {
        KeyPair keyPair = generateKeyPair();
        return new SystemTestCertAndKeyBuilder(
                keyPair,
                caCert,
                asList(
                        new Extension(keyUsage, true, keyUsage(keyCertSign)),
                        new Extension(basicConstraints, true, ca()),
                        new Extension(subjectKeyIdentifier, false, createSubjectKeyIdentifier(keyPair.getPublic())),
                        new Extension(authorityKeyIdentifier, false, createAuthorityKeyIdentifier(caCert.getPublicKey()))
                )
        );
    }

    public static SystemTestCertAndKeyBuilder strimziCaCertBuilder(SystemTestCertAndKey caCert) {
        KeyPair keyPair = generateKeyPair();
        return new SystemTestCertAndKeyBuilder(
                keyPair,
                caCert,
                asList(
                        new Extension(basicConstraints, true, ca()),
                        new Extension(subjectKeyIdentifier, false, createSubjectKeyIdentifier(keyPair.getPublic())),
                        new Extension(authorityKeyIdentifier, false, createAuthorityKeyIdentifier(caCert.getPublicKey()))
                )
        );
    }

    public static SystemTestCertAndKeyBuilder endEntityCertBuilder(SystemTestCertAndKey caCert) {
        KeyPair keyPair = generateKeyPair();
        return new SystemTestCertAndKeyBuilder(
                keyPair,
                caCert,
                asList(
                        new Extension(keyUsage, true, keyUsage(digitalSignature | keyEncipherment)),
                        new Extension(extendedKeyUsage, false, extendedKeyUsage(id_kp_serverAuth, id_kp_clientAuth)),
                        new Extension(basicConstraints, true, notCa()),
                        new Extension(subjectKeyIdentifier, false, createSubjectKeyIdentifier(keyPair.getPublic())),
                        new Extension(authorityKeyIdentifier, false, createAuthorityKeyIdentifier(caCert.getPublicKey()))
                )
        );
    }

    public SystemTestCertAndKeyBuilder withIssuerDn(String issuerDn) {
        this.issuer = new X500Name(issuerDn);
        return this;
    }

    public SystemTestCertAndKeyBuilder withSubjectDn(String subjectDn) {
        this.subject = new X500Name(subjectDn);
        return this;
    }

    public SystemTestCertAndKeyBuilder withSanDnsName(final String hostName) {
        final GeneralName dnsName = new GeneralName(dNSName, hostName);
        final byte[] subjectAltName = encode(GeneralNames.getInstance(new DERSequence(dnsName)));
        extensions.add(new Extension(subjectAlternativeName, false, subjectAltName));
        return this;
    }

    public SystemTestCertAndKeyBuilder withSanDnsNames(final ASN1Encodable[] sanDnsNames) {
        final DERSequence subjectAlternativeNames = new DERSequence(sanDnsNames);
        final byte[] subjectAltName = encode(GeneralNames.getInstance(subjectAlternativeNames));
        extensions.add(new Extension(subjectAlternativeName, false, subjectAltName));

        return this;
    }

    public SystemTestCertAndKey build() {
        try {
            BigInteger certSerialNumber = BigInteger.valueOf(System.currentTimeMillis());
            ContentSigner contentSigner = createContentSigner();
            Instant startDate = Instant.now().minus(1, DAYS);
            Instant endDate = startDate.plus(CERTIFICATE_VALIDITY_PERIOD);
            JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                    issuer,
                    certSerialNumber,
                    Date.from(startDate),
                    Date.from(endDate),
                    subject,
                    keyPair.getPublic()
            );
            for (Extension extension : extensions) {
                certBuilder.addExtension(extension);
            }
            X509Certificate certificate = new JcaX509CertificateConverter()
                    .setProvider(PROVIDER_NAME)
                    .getCertificate(certBuilder.build(contentSigner));
            return new SystemTestCertAndKey(certificate, keyPair.getPrivate());
        } catch (CertIOException | CertificateException | OperatorCreationException e) {
            throw new RuntimeException(e);
        }
    }

    private ContentSigner createContentSigner() throws OperatorCreationException {
        JcaContentSignerBuilder builder = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM);
        if (caCert == null) {
            return builder.build(keyPair.getPrivate());
        } else {
            return builder.build(caCert.getPrivateKey());
        }
    }

    private static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_PAIR_ALGORITHM, PROVIDER_NAME);
            keyPairGenerator.initialize(KEY_SIZE);
            return keyPairGenerator.generateKeyPair();
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] keyUsage(int usage) {
        return encode(new KeyUsage(usage));
    }

    private static byte[] extendedKeyUsage(KeyPurposeId... usages) {
        return encode(new ExtendedKeyUsage(usages));
    }

    private static byte[] notCa() {
        return encode(new BasicConstraints(false));
    }

    private static byte[] ca() {
        return encode(new BasicConstraints(true));
    }

    private static byte[] createSubjectKeyIdentifier(PublicKey publicKey) {
        JcaX509ExtensionUtils extensionUtils = createExtensionUtils();
        return encode(extensionUtils.createSubjectKeyIdentifier(publicKey));
    }

    private static byte[] createAuthorityKeyIdentifier(PublicKey publicKey) {
        JcaX509ExtensionUtils extensionUtils = createExtensionUtils();
        return encode(extensionUtils.createAuthorityKeyIdentifier(publicKey));
    }

    private static byte[] encode(ASN1Object object) {
        try {
            return object.getEncoded();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static JcaX509ExtensionUtils createExtensionUtils() {
        try {
            return new JcaX509ExtensionUtils();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
