/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.StatefulPodTemplate;
import io.strimzi.test.ReadWriteUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class PodTemplateTest {
    static Stream<Class<? extends PodTemplate>> templates() {
        return Stream.of(PodTemplate.class, StatefulPodTemplate.class);
    }

    @ParameterizedTest
    @MethodSource("templates")
    void testNodeSelectorRoundTrip(Class<? extends PodTemplate> templateClass) {
        String yaml = "nodeSelector:\n  example.com/workload: kafka\n  kubernetes.io/os: linux\n";
        PodTemplate template = ReadWriteUtils.readObjectFromYamlString(yaml, templateClass);
        String json = ReadWriteUtils.writeObjectToJsonString(template);
        PodTemplate reread = ReadWriteUtils.readObjectFromYamlString(json, templateClass);
        assertThat(reread.getNodeSelector(), is(Map.of("example.com/workload", "kafka", "kubernetes.io/os", "linux")));
        assertThat(ReadWriteUtils.writeObjectToYamlString(reread), is(ReadWriteUtils.writeObjectToYamlString(template)));
    }

    @ParameterizedTest
    @MethodSource("templates")
    void testEmptyNodeSelectorIsOmitted(Class<? extends PodTemplate> templateClass) {
        for (String yaml : new String[]{"{}", "nodeSelector: null", "nodeSelector: {}"}) {
            PodTemplate template = ReadWriteUtils.readObjectFromYamlString(yaml, templateClass);
            assertThat(ReadWriteUtils.writeObjectToJsonString(template), not(containsString("nodeSelector")));
            assertThat(ReadWriteUtils.writeObjectToYamlString(template), not(containsString("nodeSelector")));
        }
    }
}
