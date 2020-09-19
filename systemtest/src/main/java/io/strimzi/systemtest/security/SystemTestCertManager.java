/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import java.util.ArrayList;
import java.util.List;

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
}
