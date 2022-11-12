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
import static org.hamcrest.Matchers.hasSize;

public class JmxTransTest {
    @Test
    public void testQueries() {
        JmxTransSpec opts = TestUtils.fromYamlString("kafkaQueries:\n" +
                                                     "  - targetMBean: targetMBean1\n" +
                                                     "    attributes: [attribute0, attribute1]\n" +
                                                     "    outputs: [output0, output1]\n" +
                                                     "  - targetMBean: targetMBean2\n" +
                                                     "    attributes: [attribute2, attribute3]\n" +
                                                     "    outputs: [output2, output3]",
                JmxTransSpec.class);

        assertThat(opts, is(notNullValue()));
        assertThat(opts.getKafkaQueries(), hasSize(2));

        assertThat(opts.getKafkaQueries().get(0).getTargetMBean(),  is("targetMBean1"));
        assertThat(opts.getKafkaQueries().get(0).getAttributes().size(),  is(2));
        assertThat(opts.getKafkaQueries().get(0).getAttributes().get(0),  is("attribute0"));
        assertThat(opts.getKafkaQueries().get(0).getAttributes().get(1),  is("attribute1"));
        assertThat(opts.getKafkaQueries().get(0).getOutputs().get(0),  is("output0"));
        assertThat(opts.getKafkaQueries().get(0).getOutputs().get(1),  is("output1"));

        assertThat(opts.getKafkaQueries().get(1).getTargetMBean(),  is("targetMBean2"));
        assertThat(opts.getKafkaQueries().get(1).getAttributes().size(),  is(2));
        assertThat(opts.getKafkaQueries().get(1).getAttributes().get(0),  is("attribute2"));
        assertThat(opts.getKafkaQueries().get(1).getAttributes().get(1),  is("attribute3"));
        assertThat(opts.getKafkaQueries().get(1).getOutputs().get(0),  is("output2"));
        assertThat(opts.getKafkaQueries().get(1).getOutputs().get(1),  is("output3"));
    }

    @Test
    public void testOutputDefinition() {
        JmxTransSpec opts = TestUtils.fromYamlString("outputDefinitions:\n" +
                                                     "  - outputType: targetOutputType\n" +
                                                     "    host: targetHost\n" +
                                                     "    port: 9999\n" +
                                                     "    flushDelayInSeconds: 1\n" +
                                                     "    typeNames: [typeName0, typeName1]\n" +
                                                     "    name: targetName\n" +
                                                     "  - outputType: targetOutputType\n" +
                                                     "    name: name1",
                JmxTransSpec.class);

        assertThat(opts, is(notNullValue()));
        assertThat(opts.getOutputDefinitions(), hasSize(2));

        assertThat(opts.getOutputDefinitions().get(0).getHost(), is("targetHost"));
        assertThat(opts.getOutputDefinitions().get(0).getOutputType(), is("targetOutputType"));
        assertThat(opts.getOutputDefinitions().get(0).getFlushDelayInSeconds(), is(1));
        assertThat(opts.getOutputDefinitions().get(0).getTypeNames().get(0), is("typeName0"));
        assertThat(opts.getOutputDefinitions().get(0).getTypeNames().get(1), is("typeName1"));
        assertThat(opts.getOutputDefinitions().get(0).getName(), is("targetName"));


        assertThat(opts.getOutputDefinitions().get(1).getOutputType(), is("targetOutputType"));
        assertThat(opts.getOutputDefinitions().get(1).getName(), is("name1"));
    }

    @Test
    public void testUseCustomImage() {
        JmxTransSpec opts = TestUtils.fromYamlString("image: testImage", JmxTransSpec.class);

        assertThat(opts, is(notNullValue()));
        assertThat(opts.getImage(), is("testImage"));
    }
}
