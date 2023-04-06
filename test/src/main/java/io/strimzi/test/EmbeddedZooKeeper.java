/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;

public class EmbeddedZooKeeper {

    NIOServerCnxnFactory factory;
    ZooKeeperServer zk;
    File dir;

    public EmbeddedZooKeeper() throws IOException, InterruptedException {
        dir = Files.createTempDirectory("strimzi").toFile();
        zk = new ZooKeeperServer(dir, dir, 1000);
        start(new InetSocketAddress("localhost", TestUtils.getFreePort()));
    }

    private void start(InetSocketAddress addr) throws IOException, InterruptedException {
        factory = new NIOServerCnxnFactory();
        factory.configure(addr, 20);
        factory.startup(zk);
    }

    public void restart() throws IOException, InterruptedException {
        if (zk != null) {
            zk.shutdown(false);
        }
        // Reuse the existing port
        InetSocketAddress addr = factory.getLocalAddress();
        factory.shutdown();
        zk = new ZooKeeperServer(dir, dir, 1000);
        start(addr);
    }

    public void close() {
        if (zk != null) {
            zk.shutdown(true);
        }
        if (factory != null) {
            factory.shutdown();
        }
        delete(dir);
    }

    private static void delete(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                delete(child);
            }
        }
        if (!file.delete()) {
            noop();
        }
    }

    private static void noop() {
        // Here merely to please findbugs
    }

    public int getZkPort() {
        return factory.getLocalPort();
    }

    public String getZkConnectString() {
        InetSocketAddress addr = factory.getLocalAddress();
        return addr.getAddress().getHostAddress() + ":" + addr.getPort();
    }
}
