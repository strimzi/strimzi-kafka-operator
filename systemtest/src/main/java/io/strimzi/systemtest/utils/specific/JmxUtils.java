/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.Secret;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class JmxUtils {

    private JmxUtils() {}

    private static final Logger LOGGER = LogManager.getLogger(JmxUtils.class);

    private static void createScriptForJMXTermInPod(String namespaceName, String podName, String serviceName, String userName, String password, String commands) {
        String scriptBody = "open service:jmx:rmi:///jndi/rmi://" + serviceName + ":" + TestConstants.JMX_PORT + "/jmxrmi";

        if (!userName.equals("") && !password.equals("")) {
            scriptBody += " -u " + userName + " -p " + password + "\n";
        } else {
            scriptBody += "\n";
        }

        scriptBody += String.join("\n", commands);

        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/bin/bash", "-c", "echo '" + scriptBody + "' > /tmp/" + serviceName + ".sh");
    }

    private static String getResultOfJMXTermExec(String namespaceName, String podName, String serviceName) {
        String[] cmd = new String[] {
            "java",
            "-jar",
            "/tmp/jmxterm.jar",
            "-i",
            "/tmp/" + serviceName + ".sh"
        };

        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, cmd).out().trim();
    }

    public static void downloadJmxTermToPod(String namespaceName, String podName) {
        String[] cmd = new String[] {
            "curl",
            "-L",
            "https://github.com/jiaqi/jmxterm/releases/download/v1.0.2/jmxterm-1.0.2-uber.jar",
            "-o",
            "/tmp/jmxterm.jar"
        };

        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, cmd);
    }

    public static String collectJmxMetricsWithWait(String namespaceName, String serviceName, String secretName, String podName, String commands) {
        Secret jmxSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(secretName).get();

        LOGGER.info("Getting username and password for Service: {}/{} and Secret: {}/{}", namespaceName, serviceName, namespaceName, secretName);
        String userName = Util.decodeFromBase64(jmxSecret.getData().get("jmx-username"), StandardCharsets.UTF_8);
        String password = Util.decodeFromBase64(jmxSecret.getData().get("jmx-password"), StandardCharsets.UTF_8);

        LOGGER.info("Creating script file for Service: {}/{}", namespaceName, serviceName);
        createScriptForJMXTermInPod(namespaceName, podName, serviceName, userName, password, commands);

        String[] result = {""};

        LOGGER.info("Waiting for JMX metrics to be present for Service: {}/{}", namespaceName, serviceName);
        TestUtils.waitFor("JMX metrics to be present for Service: " + serviceName, TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                result[0] = getResultOfJMXTermExec(namespaceName, podName, serviceName);
                return !result[0].isEmpty();
            }
        );

        return result[0];
    }
}
