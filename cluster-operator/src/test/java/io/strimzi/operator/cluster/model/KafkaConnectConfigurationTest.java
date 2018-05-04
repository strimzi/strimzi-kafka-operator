/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KafkaConnectConfigurationTest {
    @Test
    public void testWithHostPort() {
        JsonObject configuration = new JsonObject().put("bootstrap.server", "my-server:9092");
        String expectedConfiguration = "bootstrap.server=my-server\\:9092\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }
}
