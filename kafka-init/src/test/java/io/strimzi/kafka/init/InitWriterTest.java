/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeAddressBuilder;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InitWriterTest {

    @TempDir
    public File tempDir;

    private static Map<String, String> envVars = new HashMap<>(3);
    private static Map<String, String> labels = new HashMap<>(4);
    private static List<NodeAddress> addresses = new ArrayList<>(3);

    static {
        envVars.put(InitWriterConfig.NODE_NAME, "localhost");
        envVars.put(InitWriterConfig.RACK_TOPOLOGY_KEY, "failure-domain.beta.kubernetes.io/zone");
        envVars.put(InitWriterConfig.EXTERNAL_ADDRESS, "true");

        // metadata labels related to the Kubernetes cluster node
        labels.put("beta.kubernetes.io/arch", "amd64");
        labels.put("beta.kubernetes.io", "linux");
        labels.put("kubernetes.io/hostname", "localhost");
        labels.put("failure-domain.beta.kubernetes.io/zone", "eu-zone1");

        addresses.add(new NodeAddressBuilder().withType("ExternalDNS").withAddress("my.external.address").build());
        addresses.add(new NodeAddressBuilder().withType("InternalDNS").withAddress("my.internal.address").build());
        addresses.add(new NodeAddressBuilder().withType("InternalIP").withAddress("192.168.2.94").build());
    }

    @Test
    public void testWriteRackId() throws IOException {

        // create and configure (env vars) the path to the rack-id file
        File kafkaFolder = new File(tempDir.getPath() + "opt/kafka");
        String rackFolder = kafkaFolder.getAbsolutePath() + "/rack";
        new File(rackFolder).mkdirs();

        Map<String, String> envVars = new HashMap<>(InitWriterTest.envVars);
        envVars.put(InitWriterConfig.INIT_FOLDER, rackFolder);

        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), labels, Collections.EMPTY_LIST);

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeRack(), is(true));
        assertThat(readFile(rackFolder + "/rack.id"), is("eu-zone1"));
    }

    @Test
    public void testWriteExternalAddress() throws IOException {

        // create and configure (env vars) the path to the rack-id file
        File kafkaFolder = new File(tempDir.getPath(), "/opt/kafka");
        String addressFolder = kafkaFolder.getAbsolutePath() + "/external.address";
        new File(addressFolder).mkdirs();

        Map<String, String> envVars = new HashMap<>(InitWriterTest.envVars);
        envVars.put(InitWriterConfig.INIT_FOLDER, addressFolder);

        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), Collections.EMPTY_MAP, addresses);

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeExternalAddress(), is(true));
        assertThat(readFile(addressFolder + "/external.address"), is("my.external.address"));
    }

    @Test
    public void testWriteRackFailWithMissingKubernetesZoneLabel() {

        // the cluster node will not have the requested label
        Map<String, String> labels = new HashMap<>(InitWriterTest.labels);
        labels.remove("failure-domain.beta.kubernetes.io/zone");

        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), labels, Collections.EMPTY_LIST);

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeRack(), is(false));
    }

    @Test
    public void testWriteRackFailsWhenInitFolderDoesNotExist() {

        // specify a not existing folder for emulating IOException in the rack writer
        Map<String, String> envVars = new HashMap<>(InitWriterTest.envVars);
        envVars.put(InitWriterConfig.INIT_FOLDER, "/no-folder");

        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), labels, addresses);

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeRack(), is(false));
    }

    private String readFile(String file) throws IOException {
        return new String(Files.readAllBytes(Paths.get(file)));
    }

    /**
     * Mock a Kubernetes client for getting cluster node information
     *
     * @param nodeName cluster node name
     * @param labels metadata labels to be returned for the provided cluster node name
     * @return mocked Kubernetes client
     */
    private KubernetesClient mockKubernetesClient(String nodeName, Map<String, String> labels, List<NodeAddress> addresses) {

        KubernetesClient client = mock(KubernetesClient.class);
        NonNamespaceOperation mockNodes = mock(NonNamespaceOperation.class);
        Resource mockResource = mock(Resource.class);
        Node mockNode = mock(Node.class);
        ObjectMeta mockNodeMetadata = mock(ObjectMeta.class);
        NodeStatus mockNodeStatus = mock(NodeStatus.class);

        when(client.nodes()).thenReturn(mockNodes);
        when(mockNodes.withName(nodeName)).thenReturn(mockResource);
        when(mockResource.get()).thenReturn(mockNode);
        when(mockNode.getMetadata()).thenReturn(mockNodeMetadata);
        when(mockNodeMetadata.getLabels()).thenReturn(labels);
        when(mockNode.getStatus()).thenReturn(mockNodeStatus);
        when(mockNodeStatus.getAddresses()).thenReturn(addresses);

        return client;
    }
}
