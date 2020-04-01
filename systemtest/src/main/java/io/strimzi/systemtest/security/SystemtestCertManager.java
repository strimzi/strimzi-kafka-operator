/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import java.util.ArrayList;
import java.util.List;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static java.util.Arrays.asList;

public class SystemtestCertManager {

    private static final String KAFKA_CERT_FILE_PATH = "/opt/kafka/broker-certs/";
    private static final String ZK_CERT_FILE_PATH = "/etc/tls-sidecar/zookeeper-nodes/";
    private static final String KAFKA_CA_FILE_PATH = "/opt/kafka/cluster-ca-certs/ca.crt";
    private static final String ZK_CA_FILE_PATH = "/etc/tls-sidecar/cluster-ca-certs/ca.crt";

    public SystemtestCertManager() {}

    private List<String> generateOpenSSLCommand(String server, String caFilePath, String hostname) {
        return new ArrayList<>(asList("openssl",
                "s_client",
                "-connect", server,
                "-showcerts",
                "-CAfile", caFilePath,
                "-verify_hostname", hostname
        ));
    }

    public String openSSLCommandByResource(String podName, String hostname, String port, String customResource, String namespace) {
        return openSSLCommandByResource(podName + "." + hostname + ":" + port,
                podName + "." + hostname + "." + namespace + ".svc.cluster.local", podName, customResource, true);
    }

    public String openSSLCommandByResource(String server, String hostname, String podName, String customResource) {
        return openSSLCommandByResource(server, hostname, podName, customResource, true);
    }

    public String openSSLCommandByResource(String server, String hostname, String podName, String customResource, boolean withCertAndKey) {
        String path = customResource.equals("kafka") ? KAFKA_CERT_FILE_PATH : ZK_CERT_FILE_PATH;
        String caFilePath = customResource.equals("kafka") ? KAFKA_CA_FILE_PATH : ZK_CA_FILE_PATH;
        String output;

        List<String> cmd = generateOpenSSLCommand(server, caFilePath, hostname);

        if (withCertAndKey) {
            cmd.add("-cert " + path + podName + ".crt");
            cmd.add("-key " + path + podName + ".key");
        }

        if (customResource.equals("kafka")) {
            output = executeCommandInPod(cmd, podName);
        } else {
            output = executeCommandInContainer(cmd, podName, "tls-sidecar");
        }
        return output;
    }

    public static String executeCommandInPod(List<String> cmd, String podName) {
        return cmdKubeClient().execInPod(podName, "/bin/bash", "-c", String.join(" ", cmd)).out();
    }

    public static String executeCommandInContainer(List<String> cmd, String podName, String container) {
        return cmdKubeClient().execInPodContainer(podName, container,  "/bin/bash", "-c",
                String.join(" ", cmd)).out();
    }

}
