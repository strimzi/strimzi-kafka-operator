/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class JmxTransTest {
    @Test
    public void testQueries() {
        JmxTransSpec opts = TestUtils.fromJson("{" +
                "\"kafkaQueries\": [{" +
                        "\"targetMBean\": \"targetMBean\"," +
                        "\"attributes\": [\"attribute0\", \"attribute1\"]," +
                        "\"outputs\": [\"output0\", \"output1\"]" +
                    "}, {" +
                        "\"targetMBean\": \"targetMBean\"," +
                        "\"attributes\": [\"attribute1\", \"attribute2\"]," +
                        "\"outputs\": [\"output0\", \"output1\"]" +
                    "}]" +
                "}", JmxTransSpec.class);

        assertThat(opts, is(notNullValue()));
        assertThat(opts.getKafkaQueries().size(),  is(2));
        assertThat(opts.getKafkaQueries().get(0).getTargetMBean(),  is("targetMBean"));
        assertThat(opts.getKafkaQueries().get(0).getAttributes().size(),  is(2));
        assertThat(opts.getKafkaQueries().get(0).getAttributes().get(0),  is("attribute0"));
        assertThat(opts.getKafkaQueries().get(0).getAttributes().get(1),  is("attribute1"));
        assertThat(opts.getKafkaQueries().get(0).getOutputs().get(0),  is("output0"));
        assertThat(opts.getKafkaQueries().get(0).getOutputs().get(1),  is("output1"));
    }

    @Test
    public void testOutputDefinition() {
        JmxTransSpec opts = TestUtils.fromJson("{" +
                "\"outputDefinitions\": [{" +
                        "\"outputType\": \"targetOutputType\"," +
                        "\"host\": \"targetHost\"," +
                        "\"port\": 9999," +
                        "\"flushDelay\": 1," +
                        "\"typeNames\": [\"typeName0\", \"typeName1\"]," +
                        "\"name\": \"targetName\"" +
                    "}, {" +
                        "\"outputType\": \"targetOutputType\"," +
                        "\"name\": \"name1\"" +
                    "}]" +
                "}", JmxTransSpec.class);

        assertThat(opts, is(notNullValue()));
        assertThat(opts.getOutputDefinitionTemplates().size(),  is(2));
        assertThat(opts.getOutputDefinitionTemplates().get(0).getHost(),  is("targetHost"));
        assertThat(opts.getOutputDefinitionTemplates().get(0).getOutputType(),  is("targetOutputType"));
        assertThat(opts.getOutputDefinitionTemplates().get(0).getFlushDelay(),  is(1));
        assertThat(opts.getOutputDefinitionTemplates().get(0).getTypeNames().get(0),  is("typeName0"));
        assertThat(opts.getOutputDefinitionTemplates().get(0).getTypeNames().get(1),  is("typeName1"));
        assertThat(opts.getOutputDefinitionTemplates().get(0).getName(),  is("targetName"));
    }

    @Test
    public void getCustomImage() {
        JmxTransSpec opts = TestUtils.fromJson("{" +
                "\"image\": \"testImage\"" +
                "}", JmxTransSpec.class);

        assertThat(opts, is(notNullValue()));
        assertThat(opts.getImage(), is("testImage"));
    }
}
