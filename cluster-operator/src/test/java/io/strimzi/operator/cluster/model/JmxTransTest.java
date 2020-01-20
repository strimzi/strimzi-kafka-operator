/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxTransSpec;
import io.strimzi.api.kafka.model.JmxTransSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplateBuilder;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplateBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.components.JmxTransOutputWriter;
import io.strimzi.operator.cluster.model.components.JmxTransQueries;
import io.strimzi.operator.cluster.model.components.JmxTransServer;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class JmxTransTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final Map<String, Object> configuration = singletonMap("foo", "bar");
    private final InlineLogging kafkaLog = new InlineLogging();
    private final InlineLogging zooLog = new InlineLogging();

    private final JmxTransSpec jmxTransSpec = new JmxTransSpecBuilder()
            .withOutputDefinitions(new JmxTransOutputDefinitionTemplateBuilder()
                    .withName("Name")
                    .withOutputType("output")
                    .build())
            .withKafkaQueries(new JmxTransQueryTemplateBuilder()
                    .withOutputs("name")
                    .withAttributes("attributes")
                    .withNewTargetMBean("mbean")
                    .build())
            .build();

    private final Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCm, configuration, kafkaLog, zooLog))
            .editSpec()
                .withJmxTrans(jmxTransSpec)
                .editKafka().withJmxOptions(new KafkaJmxOptionsBuilder()
                    .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                .build())
            .endKafka()
            .endSpec()
            .build();

    private final JmxTrans jmxTrans = JmxTrans.fromCrd(kafkaAssembly, VERSIONS);

    @Test
    public void testOutputDefinitionWriterDeserialization() {
        JmxTransOutputWriter outputWriter = new JmxTransOutputWriter();

        outputWriter.setAtClass("class");
        outputWriter.setHost("host");
        outputWriter.setPort(9999);
        outputWriter.setFlushDelayInSeconds(1);
        outputWriter.setTypeNames(Collections.singletonList("SingleType"));

        JsonObject targetJson = JsonObject.mapFrom(outputWriter);

        assertThat(targetJson.getString("host"), is("host"));
        assertThat(targetJson.getString("@class"), is("class"));
        assertThat(targetJson.getInteger("port"), is(9999));
        assertThat(targetJson.getInteger("flushDelayInSeconds"), is(1));
        assertThat(targetJson.getJsonArray("typeNames").size(), is(1));
        assertThat(targetJson.getJsonArray("typeNames").getList().get(0), is("SingleType"));

    }

    @Test
    public void testServersDeserialization() {
        JmxTransServer server = new JmxTransServer();

        server.setHost("host");
        server.setPort(9999);
        server.setUsername("username");
        server.setPassword("password");
        server.setQueries(Collections.emptyList());

        JsonObject targetJson = JsonObject.mapFrom(server);

        assertThat(targetJson.getString("host"), is("host"));
        assertThat(targetJson.getInteger("port"), is(9999));
        assertThat(targetJson.getString("username"), is("username"));
        assertThat(targetJson.getString("password"), is("password"));
        assertThat(targetJson.getJsonArray("queries").getList().size(), is(0));
    }

    @Test
    public void testQueriesDeserialization() {
        JmxTransOutputWriter outputWriter = new JmxTransOutputWriter();

        outputWriter.setAtClass("class");
        outputWriter.setHost("host");
        outputWriter.setPort(9999);
        outputWriter.setFlushDelayInSeconds(1);
        outputWriter.setTypeNames(Collections.singletonList("SingleType"));

        JmxTransQueries queries = new JmxTransQueries();

        queries.setObj("object");
        queries.setAttr(Collections.singletonList("attribute"));

        queries.setOutputWriters(Collections.singletonList(outputWriter));

        JsonObject targetJson = JsonObject.mapFrom(queries);
        JsonObject outputWriterJson = targetJson.getJsonArray("outputWriters").getJsonObject(0);

        assertThat(targetJson.getString("obj"), is("object"));
        assertThat(targetJson.getJsonArray("attr").size(), is(1));
        assertThat(targetJson.getJsonArray("attr").getString(0), is("attribute"));

        assertThat(outputWriterJson.getString("host"), is("host"));
        assertThat(outputWriterJson.getString("@class"), is("class"));
        assertThat(outputWriterJson.getInteger("port"), is(9999));
        assertThat(outputWriterJson.getInteger("flushDelayInSeconds"), is(1));
        assertThat(outputWriterJson.getJsonArray("typeNames").size(), is(1));
        assertThat(outputWriterJson.getJsonArray("typeNames").getList().get(0), is("SingleType"));
    }

    @Test
    public void testConfigMapOnScaleUp() throws JsonProcessingException  {
        ConfigMap originalCM = jmxTrans.generateJmxTransConfigMap(jmxTransSpec, 1);
        ConfigMap scaledCM = jmxTrans.generateJmxTransConfigMap(jmxTransSpec, 2);

        assertThat(originalCM.getData().get(JmxTrans.JMXTRANS_CONFIGMAP_KEY).length() <
                        scaledCM.getData().get(JmxTrans.JMXTRANS_CONFIGMAP_KEY).length(),
                is(true));
    }

    @Test
    public void testConfigMapOnScaleDown() throws JsonProcessingException  {
        ConfigMap originalCM = jmxTrans.generateJmxTransConfigMap(jmxTransSpec, 2);
        ConfigMap scaledCM = jmxTrans.generateJmxTransConfigMap(jmxTransSpec, 1);

        assertThat(originalCM.getData().get(JmxTrans.JMXTRANS_CONFIGMAP_KEY).length() >
                        scaledCM.getData().get(JmxTrans.JMXTRANS_CONFIGMAP_KEY).length(),
                is(true));
    }
}
