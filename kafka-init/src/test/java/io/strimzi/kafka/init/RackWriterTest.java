/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public class RackWriterTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static Map<String, String> envVars = new HashMap<>(2);
    private static Map<String, String> labels = new HashMap<>(4);

    static {

        envVars.put(RackWriterConfig.NODE_NAME, "localhost");
        envVars.put(RackWriterConfig.RACK_TOPOLOGY_KEY, "failure-domain.beta.kubernetes.io/zone");

        // metadata labels related to the Kubernetes/OpenShift cluster node
        labels.put("beta.kubernetes.io/arch", "amd64");
        labels.put("beta.kubernetes.io", "linux");
        labels.put("kubernetes.io/hostname", "localhost");
        labels.put("failure-domain.beta.kubernetes.io/zone", "eu-zone1");
    }

    @Test
    public void testWriteRackId() throws IOException {

        // create and configure (env vars) the path to the rack-id file
        File kafkaFolder = tmpFolder.newFolder("opt", "kafka");
        String rackFolder = kafkaFolder.getAbsolutePath() + "/rack";
        new File(rackFolder).mkdir();

        Map<String, String> envVars = new HashMap<>(RackWriterTest.envVars);
        envVars.put(RackWriterConfig.RACK_FOLDER, rackFolder);

        RackWriterConfig config = RackWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), labels);

        RackWriter writer = new RackWriter(client);
        assertTrue(writer.write(config));
    }

    @Test
    public void testNoLabel() {

        // the cluster node will not have the requested label
        Map<String, String> labels = new HashMap<>(RackWriterTest.labels);
        labels.remove("failure-domain.beta.kubernetes.io/zone");

        RackWriterConfig config = RackWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), labels);

        RackWriter writer = new RackWriter(client);
        assertFalse(writer.write(config));
    }

    @Test
    public void testNoFolder() throws IOException {

        // specify a not existing folder for emulating IOException in the rack writer
        Map<String, String> envVars = new HashMap<>(RackWriterTest.envVars);
        envVars.put(RackWriterConfig.RACK_FOLDER, "/no-folder");

        RackWriterConfig config = RackWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), labels);

        RackWriter writer = new RackWriter(client);
        assertFalse(writer.write(config));
    }

    /**
     * Mock a Kubernetes client for getting cluster node information
     *
     * @param nodeName cluster node name
     * @param labels metadata labels to be returned for the provided cluster node name
     * @return mocked Kubernetes client
     */
    private KubernetesClient mockKubernetesClient(String nodeName, Map<String, String> labels) {

        KubernetesClient client = mock(KubernetesClient.class);
        NonNamespaceOperation mockNodes = mock(NonNamespaceOperation.class);
        Resource mockResource = mock(Resource.class);
        Node mockNode = mock(Node.class);
        ObjectMeta mockNodeMetadata = mock(ObjectMeta.class);

        when(client.nodes()).thenReturn(mockNodes);
        when(mockNodes.withName(nodeName)).thenReturn(mockResource);
        when(mockResource.get()).thenReturn(mockNode);
        when(mockNode.getMetadata()).thenReturn(mockNodeMetadata);
        when(mockNodeMetadata.getLabels()).thenReturn(labels);

        return client;
    }

}
