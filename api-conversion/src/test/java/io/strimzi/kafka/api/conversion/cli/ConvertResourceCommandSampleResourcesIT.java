/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.strimzi.kafka.api.conversion.converter.MultipartConversions;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConvertResourceCommandSampleResourcesIT {
    private static final String NAMESPACE = "api-conversion";
    private static final KubeClusterResource CLUSTER = KubeClusterResource.getInstance();

    @BeforeEach
    public void beforeEach()    {
        MultipartConversions.remove();
        CLUSTER.cluster();
    }

    @BeforeAll
    static void setupEnvironment() {
        CLUSTER.createNamespace(NAMESPACE);
        CliTestUtils.setupAllCrds(CLUSTER);
    }

    @AfterAll
    static void teardownEnvironment() {
        CliTestUtils.deleteAllCrds(CLUSTER);
        CLUSTER.deleteNamespaces();
    }

    @Test
    public void convertSampleResources()    {
        convertSampleResource("bridge-logging.yaml", 0);
        convertSampleResource("bridge-v1alpha1.yaml", 0);
        convertSampleResource("connect-affinity-tolerations.yaml", 0);
        convertSampleResource("connect-logging.yaml", 0);
        convertSampleResource("connect-metrics.yaml", 0);
        convertSampleResource("connect-s2i-affinity-tolerations.yaml", 0);
        convertSampleResource("connect-s2i-logging.yaml", 0);
        convertSampleResource("connect-s2i-metrics.yaml", 0);
        convertSampleResource("connect-s2i-v1alpha1.yaml", 0);
        convertSampleResource("connect-s2i-v1beta1.yaml", 0);
        convertSampleResource("connect-v1alpha1.yaml", 0);
        convertSampleResource("connect-v1beta1.yaml", 0);
        convertSampleResource("connector-v1alpha1.yaml", 0);
        convertSampleResource("kafka-affinity-tolerations.yaml", 0);
        convertSampleResource("kafka-complex.yaml", 0);
        convertSampleResource("kafka-listeners.yaml", 0);
        convertSampleResource("kafka-logging.yaml", 0);
        convertSampleResource("kafka-metrics.yaml", 0);
        convertSampleResource("kafka-tls-sidecar.yaml", 0);
        convertSampleResource("kafka-tofull.yaml", 0);
        convertSampleResource("kafka-tosimple.yaml", 0);
        convertSampleResource("kafka-v1alpha1.yaml", 0);
        convertSampleResource("kafka-v1beta1.yaml", 0);
        convertSampleResource("kafka-v1beta2.yaml", 0);
        convertSampleResource("mm-affinity-tolerations.yaml", 0);
        convertSampleResource("mm-logging.yaml", 0);
        convertSampleResource("mm-metrics.yaml", 0);
        convertSampleResource("mm-v1alpha1.yaml", 0);
        convertSampleResource("mm-v1beta1.yaml", 0);
        convertSampleResource("mm2-affinity-tolerations.yaml", 0);
        convertSampleResource("mm2-logging.yaml", 0);
        convertSampleResource("mm2-metrics.yaml", 0);
        convertSampleResource("mm2-v1alpha1.yaml", 0);
        convertSampleResource("rebalance-v1alpha1.yaml", 0);
        convertSampleResource("topic-v1alpha1.yaml", 0);
        convertSampleResource("topic-v1beta1.yaml", 0);
        convertSampleResource("user-v1alpha1.yaml", 0);
        convertSampleResource("user-v1beta1.yaml", 0);
    }

    @Test
    public void convertInvalidSampleResources()    {
        convertSampleResource("kafka-different-policies.yaml", 1);
        convertSampleResource("kafka-different-ranges.yaml", 1);
        convertSampleResource("kafka-to-twice.yaml", 1);
        convertSampleResource("kafka-to-two-tls-sidecars.yaml", 1);
        convertSampleResource("kafka-toleration-conflict.yaml", 1);
    }

    private void convertSampleResource(String testFile, int expectedExitCode)    {
        String testResource = getClass().getResource(testFile).getPath();

        try {
            CLUSTER.createCustomResources(testResource);

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--namespace", NAMESPACE);
            assertThat("Checking exit code for conversion of " + testFile + " is " + expectedExitCode, exitCode, is(expectedExitCode));
        } finally {
            CLUSTER.deleteCustomResources(testResource);
        }
    }
}
