/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.KubeVersion;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class CrdGeneratorTest {
    @Test
    public void simpleTest() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1);
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTest.yaml"), s);
    }

    @Test
    public void simpleTestWithoutDescriptions() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1, CrdGenerator.YAML_MAPPER, emptyMap(),
                new CrdGenerator.DefaultReporter(), emptyList(), null, null, new CrdGenerator.NoneConversionStrategy(), ApiVersion.parseRange("v1+"));
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTestWithoutDescriptions.yaml"), s);
    }

    @Test
    public void simpleTestWithSubresources() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1);
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
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1,
                CrdGenerator.YAML_MAPPER, labels,
                new CrdGenerator.DefaultReporter(), emptyList(), null, null, new CrdGenerator.NoneConversionStrategy(), null);
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTestHelmMetadata.yaml"), s);
    }

    @Test
    public void versionedTest() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_16_PLUS, ApiVersion.V1BETA1);
        StringWriter w = new StringWriter();
        crdGenerator.generate(VersionedExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("versionedTest.yaml"), s);
    }

    @Test
    public void kubeV1_11ErrorWithMultiVersions() throws IOException {
        Set<String> errors = new HashSet<>();
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.parseRange("1.11+"), ApiVersion.V1BETA1,
                CrdGenerator.YAML_MAPPER, emptyMap(), new CrdGenerator.Reporter() {
                    @Override
                    public void warn(String s) {
                    }

                    @Override
                    public void err(String s) {
                        errors.add(s);
                    }
                },
                emptyList(), null, null, new CrdGenerator.NoneConversionStrategy(), null);
        StringWriter w = new StringWriter();
        crdGenerator.generate(VersionedExampleCrd.class, w);
        assertTrue(errors.contains("Multiple scales specified but 1.11 doesn't support schema per version"), errors.toString());
        assertTrue(errors.contains("Target kubernetes versions 1.11+ don't support schema-per-version, but multiple versions present on io.strimzi.crdgenerator.VersionedExampleCrd.ignored"), errors.toString());
        assertTrue(errors.contains("Target kubernetes versions 1.11+ don't support schema-per-version, but multiple versions present on io.strimzi.crdgenerator.VersionedExampleCrd.someInt"), errors.toString());
        // TODO there's a bunch more checks we need here.
        // In particular one about the use of @Alternative
    }

    @Test
    public void simpleTestWithoutType() throws IOException {
        Set<String> errors = new HashSet<>();
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1,
                CrdGenerator.YAML_MAPPER, emptyMap(), new CrdGenerator.Reporter() {
                    @Override
                    public void warn(String s) {
                    }

                    @Override
                    public void err(String s) {
                        errors.add(s);
                    }
                },
                emptyList(), null, null, new CrdGenerator.NoneConversionStrategy(), null);
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        assertTrue(errors.contains("io.strimzi.crdgenerator.ExampleCrd.PolymorphicLeft#getDiscrim is not annotated with @JsonInclude(JsonInclude.Include.NON_NULL)"), errors.toString());
        assertFalse(errors.contains("io.strimzi.crdgenerator.ExampleCrd.PolymorphicRight#getDiscrim is not annotated with @JsonInclude(JsonInclude.Include.NON_NULL)"), errors.toString());
    }
}
