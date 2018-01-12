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

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * The entry-point to the topic controller.
 * Main responsibility is to deploy a {@link Session} with an appropriate Config and KubeClient,
 * redeploying if the config changes.
 */
public class Main {

    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Main main = new Main();
        main.run();
    }

    public void run() {
        Map<String, String> m = new HashMap<>(System.getenv());
        m.keySet().retainAll(Config.keyNames());
        Config config = new Config(m);
        deploy(config);
    }

    private void deploy(Config config) {
        DefaultKubernetesClient kubeClient = new DefaultKubernetesClient();
        Vertx vertx = Vertx.vertx();
        Session session = new Session(kubeClient, config);
        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                logger.info("Session deployed");
            } else {
                logger.error("Error deploying Session", ar.cause());
            }
        });
    }
}
