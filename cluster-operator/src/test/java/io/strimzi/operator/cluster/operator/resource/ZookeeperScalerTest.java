/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.cluster.model.Ca;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
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

    @BeforeAll
    public static void initVertx() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void closeVertx() {
        vertx.close();
    }

    @Test
    public void testIsNotDifferent()   {
        Map<String, String> current = new HashMap<>(3);
        current.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        current.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        current.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");

        Map<String, String> desired = new HashMap<>(3);
        desired.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        desired.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        desired.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");

        assertThat(ZookeeperScaler.isDifferent(current, desired), is(false));

        Map<String, String> desired2 = new HashMap<>(3);
        desired2.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        desired2.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");
        desired2.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");

        assertThat(ZookeeperScaler.isDifferent(current, desired2), is(false));
    }

    @Test
    public void testIsDifferent()   {
        Map<String, String> current = new HashMap<>(3);
        current.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        current.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        current.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");

        Map<String, String> desired = new HashMap<>(3);
        desired.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        desired.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        desired.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");
        desired.put("server.4", "127.0.0.1:28883:38883:participant;127.0.0.1:21813");

        assertThat(ZookeeperScaler.isDifferent(current, desired), is(true));

        Map<String, String> desired2 = new HashMap<>(3);
        desired2.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        desired2.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");

        assertThat(ZookeeperScaler.isDifferent(current, desired2), is(true));

        Map<String, String> desired3 = new HashMap<>(3);
        desired3.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        desired3.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");

        assertThat(ZookeeperScaler.isDifferent(current, desired3), is(true));
    }

    @Test
    public void testGenerateConfigOneNode() {
        Map<String, String> expected = new HashMap<>(3);
        expected.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");

        assertThat(ZookeeperScaler.generateConfig(1), is(expected));
    }

    @Test
    public void testGenerateConfigThreeNodes() {
        Map<String, String> expected = new HashMap<>(3);
        expected.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        expected.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        expected.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");

        assertThat(ZookeeperScaler.generateConfig(3), is(expected));
    }

    @Test
    public void testParseConfig() {
        String config = "server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810\n" +
                        "server.2=127.0.0.1:28881:38881:participant;127.0.0.1:21811\n" +
                        "server.3=127.0.0.1:28882:38882:participant;127.0.0.1:21812\n" +
                        "version=100000000b";

        Map<String, String> expected = new HashMap<>(3);
        expected.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        expected.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        expected.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");

        assertThat(ZookeeperScaler.parseConfig(config.getBytes(StandardCharsets.US_ASCII)), is(expected));
    }

    @Test
    public void testMapToList() {
        Map<String, String> servers = new HashMap<>(3);
        servers.put("server.1", "127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        servers.put("server.2", "127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        servers.put("server.3", "127.0.0.1:28882:38882:participant;127.0.0.1:21812");

        List<String> expected = new ArrayList<>(3);
        expected.add("server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810");
        expected.add("server.2=127.0.0.1:28881:38881:participant;127.0.0.1:21811");
        expected.add("server.3=127.0.0.1:28882:38882:participant;127.0.0.1:21812");

        assertThat(ZookeeperScaler.serversMapToList(servers), containsInAnyOrder(expected.toArray()));
    }

    @Test
    public void testTimeoutingConnection(VertxTestContext context)  {
        String dummyBase64Value = Base64.getEncoder().encodeToString("dummy".getBytes(StandardCharsets.US_ASCII));
        Secret dummyCaSecret = new SecretBuilder()
                .addToData(Ca.CA_STORE_PASSWORD, dummyBase64Value)
                .addToData(Ca.CA_STORE, dummyBase64Value)
                .build();
        Secret dummyCoSecret = new SecretBuilder()
                .addToData("cluster-operator.password", dummyBase64Value)
                .addToData("cluster-operator.p12", dummyBase64Value)
                .build();

        ZooKeeperAdminProvider zooKeeperAdminProvider = new ZooKeeperAdminProvider() {
            @Override
            public ZooKeeperAdmin createZookeeperAdmin(String connectString, int sessionTimeout, Watcher watcher, ZKClientConfig conf) throws IOException {
                ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
                return mockZooAdmin;
            }
        };

        ZookeeperScaler scaler = new ZookeeperScaler(vertx, zooKeeperAdminProvider, "zookeeper:2181", dummyCaSecret, dummyCoSecret, 1_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(5).setHandler(context.failing(cause -> context.verify(() -> {
            assertThat(cause.getMessage(), is("Failed to connect to Zookeeper zookeeper:2181 for 1000 ms"));
            check.flag();
        })));
    }

    @Test
    public void testNoChange(VertxTestContext context) throws KeeperException, InterruptedException {
        String dummyBase64Value = Base64.getEncoder().encodeToString("dummy".getBytes(StandardCharsets.US_ASCII));
        Secret dummyCaSecret = new SecretBuilder()
                .addToData(Ca.CA_STORE_PASSWORD, dummyBase64Value)
                .addToData(Ca.CA_STORE, dummyBase64Value)
                .build();
        Secret dummyCoSecret = new SecretBuilder()
                .addToData("cluster-operator.password", dummyBase64Value)
                .addToData("cluster-operator.p12", dummyBase64Value)
                .build();

        String config = "server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810\n" +
                "version=100000000b";

        ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
        when(mockZooAdmin.getConfig(false, null)).thenReturn(config.getBytes(StandardCharsets.US_ASCII));

        ZooKeeperAdminProvider zooKeeperAdminProvider = new ZooKeeperAdminProvider() {
            @Override
            public ZooKeeperAdmin createZookeeperAdmin(String connectString, int sessionTimeout, Watcher watcher, ZKClientConfig conf) throws IOException {
                watcher.process(new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null));
                return mockZooAdmin;
            }
        };

        ZookeeperScaler scaler = new ZookeeperScaler(vertx, zooKeeperAdminProvider, "zookeeper:2181", dummyCaSecret, dummyCoSecret, 1_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(1).setHandler(context.succeeding(res -> context.verify(() -> {
            verify(mockZooAdmin, never()).reconfigure(isNull(), isNull(), anyList(), anyLong(), isNull());
            check.flag();
        })));
    }

    @Test
    public void testWithChange(VertxTestContext context) throws KeeperException, InterruptedException {
        String dummyBase64Value = Base64.getEncoder().encodeToString("dummy".getBytes(StandardCharsets.US_ASCII));
        Secret dummyCaSecret = new SecretBuilder()
                .addToData(Ca.CA_STORE_PASSWORD, dummyBase64Value)
                .addToData(Ca.CA_STORE, dummyBase64Value)
                .build();
        Secret dummyCoSecret = new SecretBuilder()
                .addToData("cluster-operator.password", dummyBase64Value)
                .addToData("cluster-operator.p12", dummyBase64Value)
                .build();

        String config = "server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810\n" +
                "server.2=127.0.0.1:28881:38881:participant;127.0.0.1:21811\n" +
                "version=100000000b";

        String updated = "server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810\n" +
                "version=100000000b";

        ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
        when(mockZooAdmin.getConfig(false, null)).thenReturn(config.getBytes(StandardCharsets.US_ASCII));
        when(mockZooAdmin.reconfigure(isNull(), isNull(), anyList(), anyLong(), isNull())).thenReturn(updated.getBytes(StandardCharsets.US_ASCII));

        ZooKeeperAdminProvider zooKeeperAdminProvider = new ZooKeeperAdminProvider() {
            @Override
            public ZooKeeperAdmin createZookeeperAdmin(String connectString, int sessionTimeout, Watcher watcher, ZKClientConfig conf) throws IOException {
                watcher.process(new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null));
                return mockZooAdmin;
            }
        };

        ZookeeperScaler scaler = new ZookeeperScaler(vertx, zooKeeperAdminProvider, "zookeeper:2181", dummyCaSecret, dummyCoSecret, 1_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(1).setHandler(context.succeeding(res -> context.verify(() -> {
            verify(mockZooAdmin, times(1)).reconfigure(isNull(), isNull(), anyList(), anyLong(), isNull());
            check.flag();
        })));
    }

    @Test
    public void testWhenThrows(VertxTestContext context) throws KeeperException, InterruptedException {
        String dummyBase64Value = Base64.getEncoder().encodeToString("dummy".getBytes(StandardCharsets.US_ASCII));
        Secret dummyCaSecret = new SecretBuilder()
                .addToData(Ca.CA_STORE_PASSWORD, dummyBase64Value)
                .addToData(Ca.CA_STORE, dummyBase64Value)
                .build();
        Secret dummyCoSecret = new SecretBuilder()
                .addToData("cluster-operator.password", dummyBase64Value)
                .addToData("cluster-operator.p12", dummyBase64Value)
                .build();

        String config = "server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810\n" +
                "server.2=127.0.0.1:28881:38881:participant;127.0.0.1:21811\n" +
                "version=100000000b";

        String updated = "server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810\n" +
                "version=100000000b";

        ZooKeeperAdmin mockZooAdmin = mock(ZooKeeperAdmin.class);
        when(mockZooAdmin.getConfig(false, null)).thenReturn(config.getBytes(StandardCharsets.US_ASCII));
        when(mockZooAdmin.reconfigure(isNull(), isNull(), anyList(), anyLong(), isNull())).thenThrow(new KeeperException.NewConfigNoQuorum());

        ZooKeeperAdminProvider zooKeeperAdminProvider = new ZooKeeperAdminProvider() {
            @Override
            public ZooKeeperAdmin createZookeeperAdmin(String connectString, int sessionTimeout, Watcher watcher, ZKClientConfig conf) throws IOException {
                watcher.process(new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null));
                return mockZooAdmin;
            }
        };

        ZookeeperScaler scaler = new ZookeeperScaler(vertx, zooKeeperAdminProvider, "zookeeper:2181", dummyCaSecret, dummyCoSecret, 1_000);

        Checkpoint check = context.checkpoint();
        scaler.scale(1).setHandler(context.failing(cause -> context.verify(() -> {
            assertThat(cause.getCause(), instanceOf(KeeperException.class));
            check.flag();
        })));
    }
}
