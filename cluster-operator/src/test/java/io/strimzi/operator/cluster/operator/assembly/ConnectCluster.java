/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.test.TestUtils;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.Connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;

public class ConnectCluster {
    private int numNodes;
    private String brokerList;
    private final List<Connect> connectInstances = new ArrayList<>();
    private final List<Integer> connectPorts = new ArrayList<>();
    private final List<String> pluginPath = new ArrayList<>();

    ConnectCluster addConnectNodes(int numNodes) {
        this.numNodes = numNodes;
        return this;
    }

    ConnectCluster usingBrokers(String bootstrapServers) {
        this.brokerList = bootstrapServers;
        return this;
    }

    public void startup() throws InterruptedException {
        for (int i = 0; i < numNodes; i++) {
            int port = TestUtils.getFreePort();

            Map<String, String> workerProps = new HashMap<>();
            workerProps.put("listeners", "http://localhost:" + port);
            workerProps.put("plugin.path", String.join(",", pluginPath));
            workerProps.put("group.id", toString());
            workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
            workerProps.put("key.converter.schemas.enable", "false");
            workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
            workerProps.put("value.converter.schemas.enable", "false");
            workerProps.put("offset.storage.topic", getClass().getSimpleName() + "-offsets");
            workerProps.put("offset.storage.replication.factor", "3");
            workerProps.put("config.storage.topic", getClass().getSimpleName() + "-config");
            workerProps.put("config.storage.replication.factor", "3");
            workerProps.put("status.storage.topic", getClass().getSimpleName() + "-status");
            workerProps.put("status.storage.replication.factor", "3");
            workerProps.put("bootstrap.servers", brokerList);

            CountDownLatch l = new CountDownLatch(1); // Indicates that the Kafka Connect cluster startup is finished (successfully or not)
            AtomicReference<Exception> startupException = new AtomicReference<>(); // Indicates whether any exception was raised during the Kafka Connect node start

            Thread thread = new Thread(() -> {
                try {
                    ConnectDistributed connectDistributed = new ConnectDistributed();
                    Connect connect = connectDistributed.startConnect(workerProps);
                    waitForAllServicesToStart(connect, 120);
                    connectInstances.add(connect);
                    connectPorts.add(port);
                    l.countDown();
                    connect.awaitStop();
                } catch (ConnectException e)    {
                    startupException.set(e);
                    l.countDown();
                }
            });

            thread.setDaemon(false);
            thread.start();
            l.await();

            if (startupException.get() != null) {
                // If the Kafka Connect node failed to start (i.e. raised an exception during the start), we should throw an exception to fail the tests
                throw new RuntimeException("Failed to start node " + i + " of the Kafka Connect cluster", startupException.get());
            }
        }
    }

    private static void waitForAllServicesToStart(Connect connect, int seconds) {
        while (!connect.isRunning() && seconds-- > 0) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new ConnectException(e);
            }
        }
        if (!connect.isRunning()) {
            throw new ConnectException(format("Connect failed to start."));
        }
    }

    public void shutdown() {
        for (Connect t : connectInstances) {
            t.stop();
        }
        for (Connect t : connectInstances) {
            t.awaitStop();
        }
    }

    /**
     * Gets the port used for given Connect node. The nodes start from 0.
     *
     * @param node  ID of the node for which we want to get the port number (node numbers start with 0)
     *
     * @return      Port which is used by given Connect node
     */
    public int getPort(int node) {
        return connectPorts.get(node);
    }
}
