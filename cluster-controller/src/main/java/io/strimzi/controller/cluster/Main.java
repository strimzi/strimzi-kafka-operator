/*
 * Copyright 2018, Strimzi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.cluster;

import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String args[]) {
        try {
            Vertx vertx = Vertx.vertx();
            vertx.deployVerticle(new ClusterController(ClusterControllerConfig.fromEnv()), res -> {
                if (res.succeeded())    {
                    log.info("Cluster Controller verticle started");
                }
                else {
                    log.error("Cluster Controller verticle failed to start", res.cause());
                    System.exit(1);
                }
            });
        } catch (IllegalArgumentException e) {
            log.error("Unable to parse arguments", e);
            System.exit(1);
        } catch (Exception e) {
            log.error("Error starting cluster controller:", e);
            System.exit(1);
        }
    }
}
