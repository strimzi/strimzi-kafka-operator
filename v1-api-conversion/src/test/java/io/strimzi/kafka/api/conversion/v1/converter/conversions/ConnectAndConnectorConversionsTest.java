/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
class ConnectAndConnectorConversionsTest extends AbstractConversionsTest {
    @Test
    public void testPauseToState() {
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewSpec()
                    .withPause(true)
                .endSpec()
                .build();

        Conversion<KafkaConnector> c = ConnectAndConnectorConversions.connectorPauseToState();

        c.convert(connector);
        assertThat(connector.getSpec().getPause(), is(nullValue()));
        assertThat(connector.getSpec().getState(), is(ConnectorState.PAUSED));
    }

    @Test
    public void testPauseToStateJson() {
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewSpec()
                    .withPause(true)
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(connector);

        Conversion<KafkaConnector> c = ConnectAndConnectorConversions.connectorPauseToState();
        c.convert(json);

        KafkaConnector converted = jsonNodeToTyped(json, KafkaConnector.class);
        assertThat(converted.getSpec().getPause(), is(nullValue()));
        assertThat(converted.getSpec().getState(), is(ConnectorState.PAUSED));
    }

    @Test
    public void testPauseToStateUnpaused() {
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewSpec()
                    .withPause(false)
                .endSpec()
                .build();

        Conversion<KafkaConnector> c = ConnectAndConnectorConversions.connectorPauseToState();

        c.convert(connector);
        assertThat(connector.getSpec().getPause(), is(nullValue()));
        assertThat(connector.getSpec().getState(), is(nullValue()));
    }

    @Test
    public void testPauseToStateUnpausedJson() {
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewSpec()
                    .withPause(false)
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(connector);

        Conversion<KafkaConnector> c = ConnectAndConnectorConversions.connectorPauseToState();
        c.convert(json);

        KafkaConnector converted = jsonNodeToTyped(json, KafkaConnector.class);
        assertThat(converted.getSpec().getPause(), is(nullValue()));
        assertThat(converted.getSpec().getState(), is(nullValue()));
    }

    @Test
    public void testPauseToStateWithStateSet() {
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewSpec()
                    .withPause(true)
                    .withState(ConnectorState.STOPPED)
                .endSpec()
                .build();

        Conversion<KafkaConnector> c = ConnectAndConnectorConversions.connectorPauseToState();

        c.convert(connector);
        assertThat(connector.getSpec().getPause(), is(nullValue()));
        assertThat(connector.getSpec().getState(), is(ConnectorState.STOPPED));
    }

    @Test
    public void testPauseToStateWithStateSetJson() {
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewSpec()
                    .withPause(true)
                    .withState(ConnectorState.STOPPED)
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(connector);

        Conversion<KafkaConnector> c = ConnectAndConnectorConversions.connectorPauseToState();
        c.convert(json);

        KafkaConnector converted = jsonNodeToTyped(json, KafkaConnector.class);
        assertThat(converted.getSpec().getPause(), is(nullValue()));
        assertThat(converted.getSpec().getState(), is(ConnectorState.STOPPED));
    }

    @Test
    public void testConnectRestructuring() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                    .withConfig(Map.of("group.id", "my-group",
                            "offset.storage.topic", "my-offsets",
                            "config.storage.topic", "my-configs",
                            "status.storage.topic", "my-statuses"))
                .endSpec()
                .build();

        Conversion<KafkaConnect> c = ConnectAndConnectorConversions.connectSpecRestructuring();

        c.convert(connect);

        assertThat(connect.getSpec().getGroupId(), is("my-group"));
        assertThat(connect.getSpec().getConfigStorageTopic(), is("my-configs"));
        assertThat(connect.getSpec().getOffsetStorageTopic(), is("my-offsets"));
        assertThat(connect.getSpec().getStatusStorageTopic(), is("my-statuses"));
        assertThat(connect.getSpec().getConfig().get("group.id"), is(nullValue()));
        assertThat(connect.getSpec().getConfig().get("config.storage.topic"), is(nullValue()));
        assertThat(connect.getSpec().getConfig().get("offset.storage.topic"), is(nullValue()));
        assertThat(connect.getSpec().getConfig().get("status.storage.topic"), is(nullValue()));
    }

    @Test
    public void testConnectRestructuringJson() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                    .withConfig(Map.of("group.id", "my-group",
                            "offset.storage.topic", "my-offsets",
                            "config.storage.topic", "my-configs",
                            "status.storage.topic", "my-statuses"))
                .endSpec()
                .build();
        JsonNode connectJson = typedToJsonNode(connect);

        Conversion<KafkaConnect> c = ConnectAndConnectorConversions.connectSpecRestructuring();
        c.convert(connectJson);

        KafkaConnect converted = jsonNodeToTyped(connectJson, KafkaConnect.class);
        assertThat(converted.getSpec().getGroupId(), is("my-group"));
        assertThat(converted.getSpec().getConfigStorageTopic(), is("my-configs"));
        assertThat(converted.getSpec().getOffsetStorageTopic(), is("my-offsets"));
        assertThat(converted.getSpec().getStatusStorageTopic(), is("my-statuses"));
        assertThat(converted.getSpec().getConfig().get("group.id"), is(nullValue()));
        assertThat(converted.getSpec().getConfig().get("config.storage.topic"), is(nullValue()));
        assertThat(converted.getSpec().getConfig().get("offset.storage.topic"), is(nullValue()));
        assertThat(converted.getSpec().getConfig().get("status.storage.topic"), is(nullValue()));
    }

    @Test
    public void testConnectRestructuringDefault() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                .endSpec()
                .build();

        Conversion<KafkaConnect> c = ConnectAndConnectorConversions.connectSpecRestructuring();

        c.convert(connect);

        assertThat(connect.getSpec().getGroupId(), is("connect-cluster"));
        assertThat(connect.getSpec().getConfigStorageTopic(), is("connect-cluster-configs"));
        assertThat(connect.getSpec().getOffsetStorageTopic(), is("connect-cluster-offsets"));
        assertThat(connect.getSpec().getStatusStorageTopic(), is("connect-cluster-status"));
        assertThat(connect.getSpec().getConfig().get("group.id"), is(nullValue()));
        assertThat(connect.getSpec().getConfig().get("config.storage.topic"), is(nullValue()));
        assertThat(connect.getSpec().getConfig().get("offset.storage.topic"), is(nullValue()));
        assertThat(connect.getSpec().getConfig().get("status.storage.topic"), is(nullValue()));
    }

    @Test
    public void testConnectRestructuringDefaultJson() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                .endSpec()
                .build();
        JsonNode connectJson = typedToJsonNode(connect);

        Conversion<KafkaConnect> c = ConnectAndConnectorConversions.connectSpecRestructuring();
        c.convert(connectJson);

        KafkaConnect converted = jsonNodeToTyped(connectJson, KafkaConnect.class);
        assertThat(converted.getSpec().getGroupId(), is("connect-cluster"));
        assertThat(converted.getSpec().getConfigStorageTopic(), is("connect-cluster-configs"));
        assertThat(converted.getSpec().getOffsetStorageTopic(), is("connect-cluster-offsets"));
        assertThat(converted.getSpec().getStatusStorageTopic(), is("connect-cluster-status"));
        assertThat(converted.getSpec().getConfig().get("group.id"), is(nullValue()));
        assertThat(converted.getSpec().getConfig().get("config.storage.topic"), is(nullValue()));
        assertThat(converted.getSpec().getConfig().get("offset.storage.topic"), is(nullValue()));
        assertThat(converted.getSpec().getConfig().get("status.storage.topic"), is(nullValue()));
    }
}