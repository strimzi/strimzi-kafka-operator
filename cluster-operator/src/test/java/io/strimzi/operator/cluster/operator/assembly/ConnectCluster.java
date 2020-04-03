/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.debezium.kafka.KafkaCluster;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ConnectCluster {

    private static final int STARTING_PORT = 8083;

    private int numNodes;
    private String brokerList;
    private List<Connect> connectInstances = new ArrayList<>();
    private List<String> pluginPath = new ArrayList<>();

    ConnectCluster addConnectNodes(int numNodes) {
        this.numNodes = numNodes;
        return this;
    }

    ConnectCluster usingBrokers(KafkaCluster kafkaCluster) {
        this.brokerList = kafkaCluster.brokerList();
        return this;
    }

    public void startup() throws InterruptedException {
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> workerProps = new HashMap<>();
            workerProps.put("listeners", "http://localhost:" + (STARTING_PORT + i));
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
            //DistributedConfig config = new DistributedConfig(workerProps);
            //RestServer rest = new RestServer(config);
            //rest.initializeServer();
            CountDownLatch l = new CountDownLatch(1);
            Thread thread = new Thread(() -> {
                ConnectDistributed connectDistributed = new ConnectDistributed();
                Connect connect = connectDistributed.startConnect(workerProps);
                l.countDown();
                connectInstances.add(connect);
                connect.awaitStop();
            });
            thread.setDaemon(false);
            thread.start();
            l.await();
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

    public int getPort() {
        return STARTING_PORT;
    }
}
