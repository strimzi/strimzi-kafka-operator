/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrdGeneratorTest {
    @Test
    public void testGeneratorCrdGoldenPath() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(new YAMLMapper().configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false));
        StringWriter w = new StringWriter();
        crdGenerator.generate(TestCrds.ExampleCrd.class, w);
        String s = w.toString();
        assertThat(s, is(CrdTestUtils.readResource("simpleTest.yaml")));
    }

    @Test
    public void testGeneratorCrdWithHelmMetadataLabels() throws IOException {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("app", "{{ template \"strimzi.name\" . }}");
        labels.put("chart", "{{ template \"strimzi.chart\" . }}");
        labels.put("component", "%plural%.%group%-crd");
        labels.put("release", "{{ .Release.Name }}");
        labels.put("heritage", "{{ .Release.Service }}");
        CrdGenerator crdGenerator = new CrdGenerator(new YAMLMapper().configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false), labels);
        StringWriter w = new StringWriter();
        crdGenerator.generate(TestCrds.ExampleCrd.class, w);
        String s = w.toString();
        assertThat(s, is(CrdTestUtils.readResource("simpleTestHelmMetadata.yaml")));
    }

    @Test
    public void testGeneratorCrdMissingJsonPropertyAnnotationThrowsInvalidCrdException() {
        CrdGenerator crdGenerator = new CrdGenerator(new YAMLMapper().configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false));
        StringWriter w = new StringWriter();
        InvalidCrdException invalidCrd = assertThrows(InvalidCrdException.class, () ->
                crdGenerator.generate(TestCrds.ExampleWithMissingJsonPropertyOrderAnnotationCrd.class, w));

        assertThat(invalidCrd.getMessage(),
                containsString("ExampleWithMissingJsonPropertyOrderAnnotationCrd missing @JsonPropertyOrder annotation"));

    }
}
