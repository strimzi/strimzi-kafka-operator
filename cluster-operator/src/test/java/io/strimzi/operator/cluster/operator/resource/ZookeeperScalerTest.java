/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ZookeeperScalerTest {
    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;

    // Shared values used in tests
    String dummyBase64Value = Base64.getEncoder().encodeToString("dummy".getBytes(StandardCharsets.US_ASCII));
    Secret dummyCaSecret = new SecretBuilder()
            .addToData(Ca.CA_CRT, MockCertManager.clusterCaCert())
            .build();
    Secret dummyCoSecret = new SecretBuilder()
            .addToData("cluster-operator.password", dummyBase64Value)
            .addToData("cluster-operator.p12", dummyBase64Value)
            .build();

    Function<Integer, String> zkNodeAddress = (Integer i) -> String.format("%s.%s.%s.svc",
            "my-cluster-zookeeper-" + i,
            "my-cluster-zookeeper-nodes",
            "myproject");

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
    }

    @Test
    public void testIsNotDifferent()   {
        Map<String, String> current = new HashMap<>(3);
        current.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        current.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        current.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        Map<String, String> desired = new HashMap<>(3);
        desired.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.isDifferent(current, desired), is(false));

        Map<String, String> desired2 = new HashMap<>(3);
        desired2.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired2.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired2.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.isDifferent(current, desired2), is(false));
    }

    @Test
    public void testIsDifferent()   {
        Map<String, String> current = new HashMap<>(3);
        current.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        current.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        current.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        Map<String, String> desired = new HashMap<>(3);
        desired.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired.put("server.4", "my-cluster-zookeeper-3.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.isDifferent(current, desired), is(true));

        Map<String, String> desired2 = new HashMap<>(3);
        desired2.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired2.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.isDifferent(current, desired2), is(true));

        Map<String, String> desired3 = new HashMap<>(3);
        desired3.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        desired3.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.isDifferent(current, desired3), is(true));
    }

    @Test
    public void testGenerateConfigOneNode() {
        Map<String, String> expected = new HashMap<>(3);
        expected.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.generateConfig(1, zkNodeAddress), is(expected));
    }

    @Test
    public void testGenerateConfigThreeNodes() {
        Map<String, String> expected = new HashMap<>(3);
        expected.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        expected.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        expected.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.generateConfig(3, zkNodeAddress), is(expected));
    }

    @Test
    public void testParseConfig() {
        String config = "server.1=my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                        "server.2=my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                        "server.3=my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                        "version=100000000b";

        Map<String, String> expected = new HashMap<>(3);
        expected.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        expected.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        expected.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.parseConfig(config.getBytes(StandardCharsets.US_ASCII)), is(expected));
    }

    @Test
    public void testMapToList() {
        Map<String, String> servers = new HashMap<>(3);
        servers.put("server.1", "my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        servers.put("server.2", "my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        servers.put("server.3", "my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        List<String> expected = new ArrayList<>(3);
        expected.add("server.1=my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        expected.add("server.2=my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");
        expected.add("server.3=my-cluster-zookeeper-2.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181");

        assertThat(ZookeeperScaler.serversMapToList(servers), containsInAnyOrder(expected.toArray()));
    }

    @Test
    public void testConnectionTimeout(VertxTestContext context)  {
        ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
        when(mockZooAdmin.getState()).thenReturn(ZooKeeper.States.NOT_CONNECTED);

        ZooKeeperAdminProvider zooKeeperAdminProvider = (connectString, sessionTimeout, watcher, conf) -> mockZooAdmin;

        ZookeeperScaler scaler = new ZookeeperScaler(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"),
                vertx, zooKeeperAdminProvider, "zookeeper:2181", null, dummyCaSecret, dummyCoSecret, 1_000, 10_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(5).onComplete(context.failing(cause -> context.verify(() -> {
            assertThat(cause.getMessage(), is("Failed to connect to Zookeeper zookeeper:2181. Connection was not ready in 1000 ms."));
            verify(mockZooAdmin, times(1)).close(anyInt());
            check.flag();
        })));
    }

    @Test
    public void testNoChange(VertxTestContext context) throws KeeperException, InterruptedException {
        String config = "server.1=my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                "version=100000000b";

        ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
        when(mockZooAdmin.getConfig(false, null)).thenReturn(config.getBytes(StandardCharsets.US_ASCII));
        when(mockZooAdmin.getState()).thenReturn(ZooKeeper.States.CONNECTED);

        ZooKeeperAdminProvider zooKeeperAdminProvider = (connectString, sessionTimeout, watcher, conf) -> {
            watcher.process(new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null));
            return mockZooAdmin;
        };

        ZookeeperScaler scaler = new ZookeeperScaler(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"),
                vertx, zooKeeperAdminProvider, "zookeeper:2181", zkNodeAddress, dummyCaSecret, dummyCoSecret, 1_000, 10_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(1).onComplete(context.succeeding(res -> context.verify(() -> {
            verify(mockZooAdmin, never()).reconfigure(isNull(), isNull(), anyList(), anyLong(), isNull());
            verify(mockZooAdmin, times(1)).close(anyInt());
            check.flag();
        })));
    }

    @Test
    public void testWithChange(VertxTestContext context) throws KeeperException, InterruptedException {
        String config = "server.1=my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                "server.2=my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                "version=100000000b";

        String updated = "server.1=my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                "version=100000000b";

        ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
        when(mockZooAdmin.getConfig(false, null)).thenReturn(config.getBytes(StandardCharsets.US_ASCII));
        when(mockZooAdmin.reconfigure(isNull(), isNull(), anyList(), anyLong(), isNull())).thenReturn(updated.getBytes(StandardCharsets.US_ASCII));
        when(mockZooAdmin.getState()).thenReturn(ZooKeeper.States.CONNECTED);

        ZooKeeperAdminProvider zooKeeperAdminProvider = (connectString, sessionTimeout, watcher, conf) -> {
            watcher.process(new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null));
            return mockZooAdmin;
        };

        ZookeeperScaler scaler = new ZookeeperScaler(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"),
                vertx, zooKeeperAdminProvider, "zookeeper:2181", zkNodeAddress, dummyCaSecret, dummyCoSecret, 1_000, 10_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(1).onComplete(context.succeeding(res -> context.verify(() -> {
            verify(mockZooAdmin, times(1)).reconfigure(isNull(), isNull(), anyList(), anyLong(), isNull());
            verify(mockZooAdmin, times(1)).close(anyInt());
            check.flag();
        })));
    }

    @Test
    public void testWhenThrows(VertxTestContext context) throws KeeperException, InterruptedException {
        String config = "server.1=my-cluster-zookeeper-0.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                "server.2=my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.myproject.svc:2888:3888:participant;127.0.0.1:12181\n" +
                "version=100000000b";

        ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
        when(mockZooAdmin.getConfig(false, null)).thenReturn(config.getBytes(StandardCharsets.US_ASCII));
        when(mockZooAdmin.reconfigure(isNull(), isNull(), anyList(), anyLong(), isNull())).thenThrow(new KeeperException.NewConfigNoQuorum());
        when(mockZooAdmin.getState()).thenReturn(ZooKeeper.States.CONNECTED);

        ZooKeeperAdminProvider zooKeeperAdminProvider = (connectString, sessionTimeout, watcher, conf) -> {
            watcher.process(new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null));
            return mockZooAdmin;
        };

        ZookeeperScaler scaler = new ZookeeperScaler(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"),
                vertx, zooKeeperAdminProvider, "zookeeper:2181", zkNodeAddress, dummyCaSecret, dummyCoSecret, 1_000, 10_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(1).onComplete(context.failing(cause -> context.verify(() -> {
            assertThat(cause.getCause(), instanceOf(KeeperException.class));
            verify(mockZooAdmin, times(1)).close(anyInt());
            check.flag();
        })));
    }

    @Test
    public void testConnectionToNonExistingHost(VertxTestContext context)  {
        ZookeeperScaler scaler = new ZookeeperScaler(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"),
                vertx, new DefaultZooKeeperAdminProvider(), "i-do-not-exist.com:2181", null, dummyCaSecret, dummyCoSecret, 2_000, 10_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(5).onComplete(context.failing(cause -> context.verify(() -> {
            assertThat(cause.getMessage(), is("Failed to connect to Zookeeper i-do-not-exist.com:2181. Connection was not ready in 2000 ms."));
            check.flag();
        })));
    }

    @Test
    public void testConnectionClosedOnGetConfigFailure(VertxTestContext context) throws KeeperException, InterruptedException  {
        ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
        when(mockZooAdmin.getState()).thenReturn(ZooKeeper.States.CONNECTED);
        when(mockZooAdmin.getConfig(false, null)).thenThrow(KeeperException.ConnectionLossException.class);
        when(mockZooAdmin.close(1_000)).thenThrow(InterruptedException.class);

        ZooKeeperAdminProvider zooKeeperAdminProvider = (connectString, sessionTimeout, watcher, conf) -> mockZooAdmin;

        ZookeeperScaler scaler = new ZookeeperScaler(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"),
                vertx, zooKeeperAdminProvider, "zookeeper:2181", null, dummyCaSecret, dummyCoSecret, 1_000, 10_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(5).onComplete(context.failing(cause -> context.verify(() -> {
            assertThat(cause.getMessage(), is("Failed to get current Zookeeper server configuration"));
            verify(mockZooAdmin, times(1)).close(anyInt());
            check.flag();
        })));
    }

}
