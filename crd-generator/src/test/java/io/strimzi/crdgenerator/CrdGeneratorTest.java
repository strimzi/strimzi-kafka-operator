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
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CrdGeneratorTest {
    @Test
    public void simpleTest() throws IOException, URISyntaxException {
        CrdGenerator crdGenerator = new CrdGenerator(new YAMLMapper().configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false));
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTest.yaml"), s);
    }

    @Test
    public void simpleTestWithSubresources() throws IOException, URISyntaxException {
        CrdGenerator crdGenerator = new CrdGenerator(new YAMLMapper().configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false));
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleWithSubresourcesCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTestWithSubresources.yaml"), s);
    }

    @Test
    public void generateHelmMetadataLabels() throws IOException {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("app", "{{ template \"strimzi.name\" . }}");
        labels.put("chart", "{{ template \"strimzi.chart\" . }}");
        labels.put("component", "%plural%.%group%-crd");
        labels.put("release", "{{ .Release.Name }}");
        labels.put("heritage", "{{ .Release.Service }}");
        CrdGenerator crdGenerator = new CrdGenerator(new YAMLMapper().configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false), labels);
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTestHelmMetadata.yaml"), s);
    }
}
