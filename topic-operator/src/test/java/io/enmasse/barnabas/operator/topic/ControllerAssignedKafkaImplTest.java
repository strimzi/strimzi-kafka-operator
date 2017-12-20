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

package io.enmasse.barnabas.operator.topic;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

@RunWith(VertxUnitRunner.class)
public class ControllerAssignedKafkaImplTest {

    /**
     * We subclass the class under test so that we can run our own executable
     * rather than the real ReassignPartitionsCommand. This allows us to test
     * various scenarios without needing ZK+Kafka clusters deployed.
     */
    static class Subclass extends ControllerAssignedKafkaImpl {

        static List<String> generate(String current, String propose) {
            return asList("--generate-current", current, "--generate-propose", propose);
        }

        static List<String> executeStarted() {
            return asList("--execute-started");
        }

        static List<String> verifyInProgress(String... partitions) {
            List<String> result = new ArrayList(2*partitions.length);
            for (String partition: partitions) {
                result.add("--verify-in-progress");
                result.add(partition);
            }
            return result;
        }

        static List<String> verifySuccess(String... partitions) {
            List<String> result = new ArrayList(2*partitions.length);
            for (String partition: partitions) {
                result.add("--verify-success");
                result.add(partition);
            }
            return result;
        }

        public static final String SCRIPT = "src/test/scripts/reassign.sh";
        private final List<List<String>> args;
        private final String script;
        private int i = 0;

        public Subclass(AdminClient adminClient, Vertx vertx, Config config, List<List<String>> args) {
            this(adminClient, vertx, config, SCRIPT, args);
        }

        public Subclass(AdminClient adminClient, Vertx vertx, Config config, String script, List<List<String>> args) {
            super(adminClient, vertx, config);
            this.script = script;
            this.args = args;
        }

        @Override
        protected void addJavaArgs(List<String> args) {
            args.add(script);
            args.addAll(this.args.get(i++));
        }
    }

    // TODO Test executable not found, or not executable

    @Test
    public void changeReplicationFactor(TestContext context) {
        MockAdminClient adminClient = new MockAdminClient();
        Vertx vertx = Vertx.vertx();
        Config config = new Config(emptyMap());
        Topic topic = new Topic.Builder("changeReplicationFactor", 2, (short) 2, emptyMap()).build();
        String[] partitions = new String[]{"changeReplicationFactor-0", "changeReplicationFactor-1"};
        Subclass sub = new Subclass(adminClient, vertx, config, asList(
                Subclass.generate("{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}",
                        "{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}"),
                Subclass.executeStarted(),
                Subclass.verifyInProgress(partitions),
                Subclass.verifySuccess(partitions)));
        Async async = context.async();
        sub.changeReplicationFactor(topic, ar -> {
            context.assertTrue(ar.succeeded());
            async.complete();
        });
    }

}
