/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class KafkaJmxOptionsTest {
    @Test
    public void testAuthentication() {
        KafkaJmxOptions opts = TestUtils.fromJson("{" +
                "\"authentication\": {" +
                    "\"type\": \"password\"" +
                    "}" +
                "}", KafkaJmxOptions.class);

        assertThat(opts.getAuthentication(),  is(notNullValue()));
        assertThat(opts.getAuthentication().getType(),  is(KafkaJmxAuthenticationPassword.TYPE_PASSWORD));
    }

    @Test
    public void testNoJmxOpts() {
        KafkaJmxOptions opts = TestUtils.fromJson("{}", KafkaJmxOptions.class);

        assertThat(opts.getAuthentication(),  is(nullValue()));
        assertThat(opts.getAdditionalProperties(),  is(Collections.emptyMap()));
    }

    @Test
    public void testQueries() {
        KafkaJmxOptions opts = TestUtils.fromJson("{" +
                "\"authentication\": {" +
                    "\"type\": \"password\"" +
                "}," +
                "\"jmxTrans\": {" +
                    "\"queries\": [{" +
                        "\"targetMBean\": \"targetMBean\"," +
                        "\"attributes\": [\"attribute0\", \"attribute1\"]," +
                        "\"outputs\": [\"output0\", \"output1\"]" +
                    "}, {" +
                        "\"targetMBean\": \"targetMBean\"," +
                        "\"attributes\": [\"attribute1\", \"attribute2\"]," +
                        "\"outputs\": [\"output0\", \"output1\"]" +
                    "}]" +
                "}" +
                "}", KafkaJmxOptions.class);

        assertThat(opts.getJmxTransSpec(),  is(notNullValue()));
        assertThat(opts.getJmxTransSpec().getQueries().size(),  is(2));
        assertThat(opts.getJmxTransSpec().getQueries().get(0).getTargetMBean(),  is("targetMBean"));
        assertThat(opts.getJmxTransSpec().getQueries().get(0).getAttributes().size(),  is(2));
        assertThat(opts.getJmxTransSpec().getQueries().get(0).getAttributes().get(0),  is("attribute0"));
        assertThat(opts.getJmxTransSpec().getQueries().get(0).getAttributes().get(1),  is("attribute1"));
        assertThat(opts.getJmxTransSpec().getQueries().get(0).getOutputs().get(0),  is("output0"));
        assertThat(opts.getJmxTransSpec().getQueries().get(0).getOutputs().get(1),  is("output1"));
    }

    @Test
    public void testOutputDefinition() {
        KafkaJmxOptions opts = TestUtils.fromJson("{" +
                "\"authentication\": {" +
                    "\"type\": \"password\"" +
                "}," +
                "\"jmxTrans\": {" +
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
                "}" +
                "}", KafkaJmxOptions.class);

        assertThat(opts.getJmxTransSpec(),  is(notNullValue()));
        assertThat(opts.getJmxTransSpec().getOutputDefinitionTemplates().size(),  is(2));
        assertThat(opts.getJmxTransSpec().getOutputDefinitionTemplates().get(0).getHost(),  is("targetHost"));
        assertThat(opts.getJmxTransSpec().getOutputDefinitionTemplates().get(0).getOutputType(),  is("targetOutputType"));
        assertThat(opts.getJmxTransSpec().getOutputDefinitionTemplates().get(0).getFlushDelay(),  is(1));
        assertThat(opts.getJmxTransSpec().getOutputDefinitionTemplates().get(0).getTypeNames().get(0),  is("typeName0"));
        assertThat(opts.getJmxTransSpec().getOutputDefinitionTemplates().get(0).getTypeNames().get(1),  is("typeName1"));
        assertThat(opts.getJmxTransSpec().getOutputDefinitionTemplates().get(0).getName(),  is("targetName"));
    }
}
