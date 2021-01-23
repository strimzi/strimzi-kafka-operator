/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.List;

import static io.strimzi.systemtest.security.SystemTestCertAndKeyBuilder.intermediateCaCertBuilder;
import static io.strimzi.systemtest.security.SystemTestCertAndKeyBuilder.endEntityCertBuilder;
import static io.strimzi.systemtest.security.SystemTestCertAndKeyBuilder.rootCaCertBuilder;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static java.util.Arrays.asList;

public class SystemTestCertManager {

    private static final String KAFKA_CERT_FILE_PATH = "/opt/kafka/broker-certs/";
    private static final String ZK_CERT_FILE_PATH = "/opt/kafka/zookeeper-node-certs/";
    private static final String CA_FILE_PATH = "/opt/kafka/cluster-ca-certs/ca.crt";

    public SystemTestCertManager() {}

    private static List<String> generateBaseSSLCommand(String server, String caFilePath, String hostname) {
        return new ArrayList<>(asList("echo -n | openssl",
                "s_client",
                "-connect", server,
                "-showcerts",
                "-CAfile", caFilePath,
                "-verify_hostname", hostname
        ));
    }

    public static String generateOpenSslCommandByComponent(String podName, String hostname, String port, String component, String namespace) {
        return generateOpenSslCommandByComponent(podName + "." + hostname + ":" + port,
                podName + "." + hostname + "." + namespace + ".svc.cluster.local", podName, component, true);
    }

    public static String generateOpenSslCommandByComponent(String server, String hostname, String podName, String component) {
        return generateOpenSslCommandByComponent(server, hostname, podName, component, true);
    }

    public static String generateOpenSslCommandByComponent(String server, String hostname, String podName, String component, boolean withCertAndKey) {
        String path = component.equals("kafka") ? KAFKA_CERT_FILE_PATH : ZK_CERT_FILE_PATH;

        List<String> cmd = generateBaseSSLCommand(server, CA_FILE_PATH, hostname);

        if (withCertAndKey) {
            cmd.add("-cert " + path + podName + ".crt");
            cmd.add("-key " + path + podName + ".key");
        }

        if (component.equals("kafka")) {
            return cmdKubeClient().execInPod(podName, "/bin/bash", "-c", String.join(" ", cmd)).out();
        } else {
            return cmdKubeClient().execInPod(podName,  "/bin/bash", "-c",
                    String.join(" ", cmd)).out();
        }
    }

    public static List<String> getCertificateChain(String certificateName) {
        return new ArrayList<>(asList(
                "s:/O=io.strimzi/CN=" + certificateName + "\n" +
                "   i:/O=io.strimzi/CN=cluster-ca",
                "Server certificate\n" +
                "subject=/O=io.strimzi/CN=" + certificateName + "\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca"
        ));
    }

    public static SystemTestCertAndKey generateRootCaCertAndKey() {
        return rootCaCertBuilder()
                .withIssuerDn("C=CZ, L=Prague, O=Strimzi, CN=StrimziRootCA")
                .withSubjectDn("C=CZ, L=Prague, O=Strimzi, CN=StrimziRootCA")
                .build();
    }

    public static SystemTestCertAndKey generateIntermediateCaCertAndKey(SystemTestCertAndKey rootCert) {
        return intermediateCaCertBuilder(rootCert)
                .withSubjectDn("C=CZ, L=Prague, O=Strimzi, CN=StrimziIntermediateCA")
                .build();
    }

    public static SystemTestCertAndKey generateEndEntityCertAndKey(SystemTestCertAndKey intermediateCert) {
        return endEntityCertBuilder(intermediateCert)
                .withSubjectDn("C=CZ, L=Prague, O=Strimzi, CN=kafka.strimzi.io")
                .withSanDnsName("*.127.0.0.1.nip.io")
                .build();
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

    private static File exportPrivateKeyToPemFile(PrivateKey privateKey) throws IOException {
        File keyFile = File.createTempFile("key-", ".key");
        try (JcaPEMWriter pemWriter = new JcaPEMWriter(new FileWriter(keyFile))) {
            pemWriter.writeObject(privateKey);
            pemWriter.flush();
        }
        return keyFile;
    }

    private static File exportCertsToPemFile(SystemTestCertAndKey... certs) throws IOException {
        File certFile = File.createTempFile("crt-", ".crt");
        try (JcaPEMWriter pemWriter = new JcaPEMWriter(new FileWriter(certFile))) {
            for (SystemTestCertAndKey certAndKey : certs) {
                pemWriter.writeObject(certAndKey.getCertificate());
            }
            pemWriter.flush();
        }
        return certFile;
    }
}
