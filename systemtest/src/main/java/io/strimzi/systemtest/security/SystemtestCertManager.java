/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;


import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.OpenSslCertManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static java.util.Arrays.asList;

public class SystemtestCertManager {
    private static final Logger log = LogManager.getLogger(SystemtestCertManager.class);

    private static final String kafkaCertFilePath = "/opt/kafka/broker-certs/";
    private static final String zkCertFilePath = "/etc/tls-sidecar/zookeeper-nodes/";
    private static final String kafkaCAFilePath = "/opt/kafka/cluster-ca-certs/";
    private static final String zkCAFilePath = "/etc/tls-sidecar/cluster-ca-certs/";

    public SystemtestCertManager() {}

    private List<String> generateOpenSSLCommand(String server, String caFilePath, String hostname) {
        List<String> cmd = new ArrayList<>(asList("openssl",
                "s_client",
                "-connect", server,
                "-showcerts",
                "-CAfile", caFilePath,
                "-verify_hostname", hostname
        ));

        return cmd;
    }

    public void openSSLCommandByResource(String podName, String hostname, String port, String customResource, String namespace){
        openSSLCommandByResource(podName + "." + hostname + ":" + port,
                podName + "." + hostname + "." + namespace + ".svc.cluster.local", podName, customResource);
    }

    public void openSSLCommandByResource(String server, String hostname, String podName, String customResource) {
        String path = customResource.equals("kafka") ? kafkaCertFilePath : zkCertFilePath;
        String caFilePath = customResource.equals("kafka") ? kafkaCAFilePath : zkCAFilePath;

        List<String> cmd = generateOpenSSLCommand(server, caFilePath, hostname);

        cmd.add("-cert " + path + podName + ".crt");
        cmd.add("-key " + path + podName + ".key");

        executeCommand(cmd, podName);
    }

    public static void executeCommand(List<String> cmd, String podName){
        cmdKubeClient().execInPod(podName, "/bin/bash", "-c", cmd.toString()).out();
    }

}
