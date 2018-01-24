/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.topic;

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
        dir.mkdirs();
        zk = new ZooKeeperServer(dir, dir, 1000);
        start(new InetSocketAddress(0));
    }

    private void start(InetSocketAddress addr) throws IOException, InterruptedException {
        factory = new NIOServerCnxnFactory();
        factory.configure(addr, 10);
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
        //delete(dir);
    }

    private static void delete(File file) {
        if (file.isFile()) {
            file.delete();
        } else {
            for (File child : file.listFiles()) {
                delete(child);
            }
            file.delete();
        }
    }

    public int getZkPort() {
        return factory.getLocalPort();
    }

    public String getZkConnectString() {
        InetSocketAddress addr = factory.getLocalAddress();
        return addr.getAddress().getHostAddress()+":"+addr.getPort();
    }

}
